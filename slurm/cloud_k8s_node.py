"""
    Modified from kubernetes node provider
"""

from typing import Any, Dict, List, Optional
from types import ModuleType
import copy
import logging
import time
from uuid import uuid4

from ray.autoscaler._private.slurm import (
    K8S_NODE_PREFIX
)
from ray.autoscaler.command_runner import CommandRunnerInterface
from ray.autoscaler._private.command_runner import KubernetesCommandRunner

import kubernetes
from kubernetes.client.rest import ApiException
from kubernetes.config.config_exception import ConfigException
from ray.autoscaler.tags import NODE_KIND_HEAD, TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_KIND
from ray.autoscaler._private.cli_logger import cli_logger

logger = logging.getLogger(__name__)

MAX_TAG_RETRIES = 3
DELAY_BEFORE_TAG_RETRY = 0.5
log_prefix = "KubernetesNodeProvider: "

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
    """Slurm node sub-NodeProvider

        This class contains the Slurm-specific part of NodeProvider 
        for a multi-node type node provider. The Slurm related calls
        are forwarded to this class 

        Note: The SlurmNode shares the same state file with the overall 
        NodeProvider

    """

    def __init__(self, 
        namespace: str
    ) -> None:
        self.namespace = namespace

    def create_worker_node( # TODO: set memory constraint
        self, node_config: Dict[str, Any], tags: Dict[str, str], count: int
    ) -> Optional[Dict[str, Any]]:
        """Creates a worker node under Slurm

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
        
        parsed_init_command = ""
        if "init_commands" in conf:
            for init in conf["init_commands"]:
                parsed_init_command += init + "\n"

        pod_spec = conf.get("pod", conf)
        service_spec = conf.get("service")
        ingress_spec = conf.get("ingress")
        node_uuid = str(uuid4())
        tags[TAG_RAY_CLUSTER_NAME] = self.cluster_name
        tags["ray-node-uuid"] = node_uuid
        pod_spec["metadata"]["namespace"] = self.namespace
        if "labels" in pod_spec["metadata"]:
            pod_spec["metadata"]["labels"].update(tags)
        else:
            pod_spec["metadata"]["labels"] = tags

        logger.info(
            log_prefix + "calling create_namespaced_pod (count={}).".format(count)
        )

        # TODO: Add init command? Dynamic worker ports range?
        new_nodes = []
        for _ in range(count):
            pod = core_api().create_namespaced_pod(self.namespace, pod_spec)
            new_nodes.append(pod)

        # new_svcs = []
        # if service_spec is not None:
        #     logger.info(
        #         log_prefix + "calling create_namespaced_service "
        #         "(count={}).".format(count)
        #     )

        #     for new_node in new_nodes:

        #         metadata = service_spec.get("metadata", {})
        #         metadata["name"] = new_node.metadata.name
        #         service_spec["metadata"] = metadata
        #         service_spec["spec"]["selector"] = {"ray-node-uuid": node_uuid}
        #         svc = core_api().create_namespaced_service(self.namespace, service_spec)
        #         new_svcs.append(svc)


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
        return KubernetesCommandRunner(
            log_prefix, self.namespace, node_id, auth_config, process_runner
        )


    def terminate_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Terminates a node under Slurm.

        Optionally return a mapping from deleted node ids to node
        metadata.
        """

        logger.info(log_prefix + "calling delete_namespaced_pod")
        try:
            core_api().delete_namespaced_pod(node_id, self.namespace)
        except ApiException as e:
            if e.status == 404:
                logger.warning(
                    log_prefix + f"Tried to delete pod {node_id},"
                    " but the pod was not found (404)."
                )
            else:
                raise
        # try: 
        #     core_api().delete_namespaced_service(node_id, self.namespace)
        # except ApiException:
        #     pass

    
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
        return pod.status.pod_ip # TODO:
