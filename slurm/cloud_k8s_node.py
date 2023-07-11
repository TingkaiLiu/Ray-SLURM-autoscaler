"""
    Modified from kubernetes node provider
"""

from typing import Any, Dict, List, Optional
from types import ModuleType
import copy
import logging
import time
import yaml
import subprocess

from ray.autoscaler._private.slurm import (
    K8S_NODE_PREFIX
)
from ray.autoscaler.command_runner import CommandRunnerInterface
from ray.autoscaler._private.command_runner import KubernetesCommandRunner # TODO: Fix
from ray.autoscaler._private.slurm.cluster_state import SlurmClusterState

import kubernetes
from kubernetes.client.rest import ApiException
from kubernetes.config.config_exception import ConfigException
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME
from ray.autoscaler._private.cli_logger import cli_logger

logger = logging.getLogger(__name__)

MAX_TAG_RETRIES = 3
WAIT_POD_INTERVAL = 5 # seconds
DELAY_BEFORE_TAG_RETRY = 0.5
log_prefix = "KubernetesNodeProvider: "

_configured = False
_core_api = None
def _load_config():
    global _configured
    if _configured:
        return
    try:
        kubernetes.config.load_incluster_config()
    except ConfigException:
        kubernetes.config.load_kube_config()
    _configured = True

def core_api():
    global _core_api
    if _core_api is None:
        _load_config()
        _core_api = kubernetes.client.CoreV1Api()

    return _core_api

def to_label_selector(tags):
    label_selector = ""
    for k, v in tags.items():
        if label_selector != "":
            label_selector += ","
        label_selector += "{}={}".format(k, v)
    return label_selector

class K8sNode:
    """K8S sub-NodeProvider

        This class contains the K8s-specific part of NodeProvider 
        for a multi-node type node provider. The K8s related calls
        are forwarded to this class 
    """

    def __init__(self, 
        cluster_state: SlurmClusterState,
        namespace: str,
        cluster_name: str,
        template_folder: str,
    ) -> None:
        self.state = cluster_state # only for getting head info
        self.namespace = namespace
        self.cluster_name = cluster_name
        self.template_folder = template_folder
        self.valid = True

        try:
            core_api()
        except Exception:
            self.valid = False

    def create_worker_node( # TODO: set memory constraint
        self, node_config: Dict[str, Any], tags: Dict[str, str], count: int
    ) -> Optional[Dict[str, Any]]:
        """Creates a worker node under Slurm

        For the current implementation, each worker is distinguished by id and port
        numbers in the config. So count == 1

        Args:
            node_config: the "node_config" section of specific node type (under
                "available_node_types" section) in the autoscaler config yaml.
                The node type is decided by the node launcher. 
            tags: the tags to be set to the created nodes
            count: the number of nodes to be created

        Optionally returns a mapping from created node ids to node metadata. 
        The return value is not used by the autoscaler, but may be useful for debugging.
        """

        conf = copy.deepcopy(node_config)
        node_id = K8S_NODE_PREFIX + conf["id"]

        # Read the pod template
        with open(self.template_folder+"ray-worker-template.yaml", "r") as f:
            pod_spec = yaml.safe_load(f)

        # Update the worker yaml according to config
        tags[TAG_RAY_CLUSTER_NAME] = self.cluster_name
        tags["ray-node-uuid"] = node_id
        pod_spec["metadata"]["namespace"] = self.namespace
        pod_spec["metadata"]["name"] = node_id
        if "labels" in pod_spec["metadata"]:
            pod_spec["metadata"]["labels"].update(tags)
        else:
            pod_spec["metadata"]["labels"] = tags

        logger.info(
            log_prefix + "calling create_namespaced_pod (count={}).".format(count)
        )
            
        pod = core_api().create_namespaced_pod(self.namespace, pod_spec)

        # Create the NodePort Service with node id as selector
        with open(self.template_folder+"ray-worker-service-template.yaml", "r") as f:
            service_spec = yaml.safe_load(f)
        service_spec["metadata"]["name"] = node_id
        service_spec["spec"]["selector"]["ray-node-uuid"] = node_id
        
        port_config = []
        port_config.append({
            "name": "node-manager-port", 
            "port": conf["node-manager-port"],
            "targetPort": conf["node-manager-port"],
            "nodePort" : conf["node-manager-port"],
        }) 
        port_config.append({
            "name": "object-manager-port", 
            "port": conf["object-manager-port"],
            "targetPort": conf["object-manager-port"],
            "nodePort" : conf["object-manager-port"],
        })

        for cur_port in range(conf["min-worker-port"], conf["max-worker-port"]+1):
            port_config.append({
                "name": "worker-port-"+str(cur_port), 
                "port": cur_port,
                "targetPort": cur_port,
                "nodePort" : cur_port,
            })    

        service_spec["spec"]["ports"] = port_config

        svc = core_api().create_namespaced_service(self.namespace, service_spec)

        # Prepare for init command
        meta_info = self.state.get_meta_info()
        
        # Wait until the pod is up
        while not self.is_running(node_id):
            time.sleep(WAIT_POD_INTERVAL)
        node_ip = self.internal_ip(node_id)
        assert node_ip != None

        # Run init command
        ray_start_command = "ray start"
        ray_start_command += " --address=\"" + meta_info["head_ip"] + ":" + meta_info["gcs_port"] + "\""
        ray_start_command += " --object-manager-port=" + str(conf["object-manager-port"])
        ray_start_command += " --node-manager-port=" + str(conf["node-manager-port"])
        ray_start_command += " --min-worker-port=" + str(conf["min-worker-port"])
        ray_start_command += " --max-worker-port=" + str(conf["max-worker-port"])
        ray_start_command += " --node-ip-address=\"" + node_ip + "\""
        ray_start_command += " --redis-password=\"" + meta_info["redis_password"] + "\""

        logger.info("Run init command\n")
        self.get_command_runner("k8s create:", node_id, {}, self.cluster_name, subprocess, True).run(ray_start_command)
        

    def get_command_runner(
        self,
        log_prefix: str,
        node_id: str,
        auth_config: Dict[str, Any],
        cluster_name: str,
        process_runner: ModuleType,
        use_internal_ip: bool,
        docker_config: Optional[Dict[str, Any]] = None,
    ) -> CommandRunnerInterface:
        """Returns the CommandRunner class used to run commands on specific node.

        Args:
            log_prefix(str): stores "NodeUpdater: {}: ".format(<node_id>). Used
                to print progress in the CommandRunner.
            node_id(str): the node ID.
            auth_config(dict): the authentication configs from the autoscaler
                yaml file.
            cluster_name(str): the name of the cluster.
            process_runner(module): the module to use to run the commands
                in the CommandRunner. E.g., subprocess.
            use_internal_ip(bool): whether the node_id belongs to an internal ip
                or external ip.
            docker_config(dict): If set, the docker information of the docker
                container that commands should be run on.
        """

        logger.info(
            log_prefix + "calling get_command_runner (id={}, namespace={}).".format(node_id, self.namespace)
        )

        return KubernetesCommandRunner(
            log_prefix=log_prefix, 
            namespace=self.namespace, 
            node_id=node_id, 
            auth_config=auth_config, 
            process_runner=process_runner
        )


    def terminate_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Terminates a node under Slurm.

        Optionally return a mapping from deleted node ids to node
        metadata.
        """

        logger.info(log_prefix + "calling delete_namespaced_pod")
        try:
            pod = core_api().delete_namespaced_pod(node_id, self.namespace)
        except ApiException as e:
            if e.status == 404:
                logger.warning(
                    log_prefix + f"Tried to delete pod {node_id},"
                    " but the pod was not found (404)."
                )
            else:
                raise
        logger.info(log_prefix + "calling delete_namespaced_service")
        try: 
            svc = core_api().delete_namespaced_service(node_id, self.namespace)
        except ApiException:
            pass

    
    def non_terminated_nodes(self, tag_filters: Dict[str, str]) -> List[str]:
        """Return a list of node ids filtered by the specified tags dict.

        This list must not include terminated nodes. For performance reasons,
        providers are allowed to cache the result of a call to
        non_terminated_nodes() to serve single-node queries
        (e.g. is_running(node_id)). This means that non_terminate_nodes() must
        be called again to refresh results.

        The node states on file will be updated by checking the slurm job status.
        Other node information on file remains unchanged
        """

        # Match pods that are in the 'Pending' or 'Running' phase.
        # Unfortunately there is no OR operator in field selectors, so we
        # have to match on NOT any of the other phases.
        field_selector = ",".join(
            [
                "status.phase!=Failed",
                "status.phase!=Unknown",
                "status.phase!=Succeeded",
                "status.phase!=Terminating",
            ]
        )

        tag_filters[TAG_RAY_CLUSTER_NAME] = self.cluster_name
        label_selector = to_label_selector(tag_filters)
        pod_list = core_api().list_namespaced_pod(
            self.namespace, field_selector=field_selector, label_selector=label_selector
        )

        # Don't return pods marked for deletion,
        # i.e. pods with non-null metadata.DeletionTimestamp.
        return [
            pod.metadata.name
            for pod in pod_list.items
            if pod.metadata.deletion_timestamp is None
        ]

    def is_running(self, node_id: str) -> bool:
        """Return whether the specified node under k8s is running."""
        pod = core_api().read_namespaced_pod(node_id, self.namespace)
        return pod.status.phase == "Running"
    
    def is_terminated(self, node_id: str) -> bool:
        """Return whether the specified node is terminated."""
        pod = core_api().read_namespaced_pod(node_id, self.namespace)
        return pod.status.phase not in ["Running", "Pending"]

    def set_node_tags(self, node_ids, tags):
        """Sets the tag values (string dict) for the specified node."""
        for _ in range(MAX_TAG_RETRIES - 1):
            try:
                self._set_node_tags(node_ids, tags)
                return
            except ApiException as e:
                if e.status == 409:
                    logger.info(
                        log_prefix + "Caught a 409 error while setting"
                        " node tags. Retrying..."
                    )
                    time.sleep(DELAY_BEFORE_TAG_RETRY)
                    continue
                else:
                    raise
        # One more try
        self._set_node_tags(node_ids, tags)

    def _set_node_tags(self, node_id, tags):
        pod = core_api().read_namespaced_pod(node_id, self.namespace)
        pod.metadata.labels.update(tags)
        core_api().patch_namespaced_pod(node_id, self.namespace, pod)

    def node_tags(self, node_id: str) -> Dict[str, str]:
        """Returns the tags of the given node (string dict)."""

        pod = core_api().read_namespaced_pod(node_id, self.namespace)
        return pod.metadata.labels
    
    def internal_ip(self, node_id: str) -> Optional[str]:
        """Returns the internal ip (Ray ip) of the given node."""
        pod = core_api().read_namespaced_pod(node_id, self.namespace)
        node_addresses = core_api().read_node(pod.spec.node_name).status.addresses

        for address in node_addresses:
            if address.type == "ExternalIP":
                return address.address
        
        return None

