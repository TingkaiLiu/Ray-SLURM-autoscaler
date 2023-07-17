from typing import Any, Dict, List, Optional
from types import ModuleType
import copy
import time

from ray.autoscaler._private.slurm import (
    NODE_STATE_RUNNING,
    NODE_STATE_PENDING,
    NODE_STATE_TERMINATED,
    PASSWORD_LENGTH,
    WAIT_HEAD_INTEVAL,
    LSF_NODE_PREFIX
)
from ray.autoscaler.command_runner import CommandRunnerInterface
from ray.autoscaler._private.slurm.empty_command_runner import EmptyCommandRunner
from ray.autoscaler._private.slurm.cluster_state import SlurmClusterState 

from ray.autoscaler._private.slurm.lsf_commands import (
    lsf_cancel_job,
    lsf_launch_head,
    lsf_launch_worker,
    lsf_get_job_ip,
    lsf_get_job_status,
    LSF_JOB_RUNNING,
    LSF_JOB_PENDING,
    LSF_JOB_NOT_EXIST
)

# Map lsf job status to node states
LSF_JOB_TRANS_MAP = {
    LSF_JOB_RUNNING : NODE_STATE_RUNNING,
    LSF_JOB_PENDING : NODE_STATE_PENDING,
    LSF_JOB_NOT_EXIST : NODE_STATE_TERMINATED
}


from ray.autoscaler._private.cli_logger import cli_logger

class LSFNode:
    """LSF node sub-NodeProvider

        This class contains the LSF-specific part of NodeProvider 
        for a multi-node type node provider. The LSF related calls
        are forwarded to this class 

        Note: The LSFNode shares the same state file with the overall 
        NodeProvider

    """

    def __init__(self, 
        cluster_state: SlurmClusterState, 
        template_folder: str,
        temp_folder: str
    ) -> None:
        self.state = cluster_state
        self.template_folder = template_folder
        self.temp_folder = temp_folder

    def create_head_node( # TODO: set memory constraint
        self, node_config: Dict[str, Any], tags: Dict[str, str], redis_password: str,
        gcs_port: str, ray_client_port: str, dashboard_port: str
    ) -> Optional[Dict[str, Any]]:
        """Creates a head node under LSF

        Args:
            node_config: the "node_config" section of specific node type (under
                "available_node_types" section) in the autoscaler config yaml.
                The node type is decided by the node launcher. 
            tags: the tags to be set to the created nodes
            count: the number of nodes to be created

        Optionally returns a mapping from created node ids to node metadata. 
        The return value is not used by the autoscaler, but may be useful for debugging.
        """

        # LTK's note: node_config only contains the "node_cofig" filed of a specific node type
        current_conf = copy.deepcopy(node_config)
        
        parsed_init_command = ""
        if "init_commands" in current_conf:
            for init in current_conf["init_commands"]:
                parsed_init_command += init + "\n"
        
        parsed_add_lsf_command = ""
        if "additional_lsf_commands" in current_conf: # TODO: Add in yaml
            for cmd in current_conf["additional_lsf_commands"]:
                parsed_add_lsf_command += cmd + "\n"

        # Head node under lsf. Assume all ports are available
            
        node_info = {}
        node_info["state"] = NODE_STATE_PENDING
        node_info["tags"] = tags

        meta_info = {}
        meta_info["head_ip"] = None
        meta_info["head_id"] = None
        meta_info["gcs_port"] = gcs_port
        meta_info["client_port"] = ray_client_port
        meta_info["dashboard_port"] = dashboard_port
        meta_info["redis_password"] = redis_password

        with self.state.lock:
            with self.state.file_lock:
                node_id = lsf_launch_head(
                    self.template_folder,
                    self.temp_folder, 
                    gcs_port,
                    ray_client_port,
                    dashboard_port,
                    redis_password,
                    parsed_init_command,
                    parsed_add_lsf_command
                )

                meta_info["head_id"] = node_id

                # Store pending info: will be updated by non_terminate_node
                self.state.put_node(node_id, node_info)
                self.state.put_meta_info(meta_info)
        
        # Wait until the head node is up
        cli_logger.warning("Waiting for the head to start...This can be block due to resource limit")
        cli_logger.warning("If you force quit here, please run 'ray down <cluster_config>.ymal afterward to clean up")
        
        while lsf_get_job_status(node_id) != LSF_JOB_RUNNING:
            time.sleep(WAIT_HEAD_INTEVAL)

        head_ip = lsf_get_job_ip(node_id)
        meta_info["head_ip"] = head_ip
        self.state.put_meta_info(meta_info)

        # Print out connection instruction
        cli_logger.success("--------------------")
        cli_logger.success("Ray runtime started.")
        cli_logger.success("--------------------")
        cli_logger.success("")
        cli_logger.success("To connect to this Ray runtime, use the following Python code:")
        cli_logger.success("  import ray")
        cli_logger.success("  ray.init(address=\"ray://%s\")\n" \
            % (head_ip+":"+meta_info["client_port"]))
        cli_logger.success("The redis password: %s\n" % redis_password)

    def create_worker_node( # TODO: set memory constraint
        self, node_config: Dict[str, Any], tags: Dict[str, str], count: int
    ) -> Optional[Dict[str, Any]]:
        """Creates a worker node under LSF

        Args:
            node_config: the "node_config" section of specific node type (under
                "available_node_types" section) in the autoscaler config yaml.
                The node type is decided by the node launcher. 
            tags: the tags to be set to the created nodes
            count: the number of nodes to be created

        Optionally returns a mapping from created node ids to node metadata. 
        The return value is not used by the autoscaler, but may be useful for debugging.
        """

        current_conf = copy.deepcopy(node_config)
        
        parsed_init_command = ""
        if "init_commands" in current_conf:
            for init in current_conf["init_commands"]:
                parsed_init_command += init + "\n"
        
        parsed_add_lsf_command = ""
        if "additional_lsf_commands" in current_conf:
            for cmd in current_conf["additional_lsf_commands"]:
                parsed_add_lsf_command += cmd + "\n"

        '''
            Since worker node is started by the autoscaler thread
            The head node is garenteed to be started at this time
            So querying the meta info here is safe
        '''
        
        meta_info = self.state.get_meta_info()
        head_ip = meta_info["head_ip"]
        assert head_ip != None

        # if head_ip == None: 
        #     head_id = meta_info["head_id"]
        #     assert head_id != HEAD_NODE_ID_OUTSIDE_SLURM
        #     head_ip = slurm_get_job_ip(head_id)
        #     meta_info["head_ip"] = head_ip
        #     self.state.put_meta_info(meta_info)
        
        for _ in range(count):
            
            node_info = {}
            node_info["state"] = NODE_STATE_PENDING
            node_info["tags"] = tags
            
            with self.state.lock:
                with self.state.file_lock:
                    
                    node_id = lsf_launch_worker(
                        self.template_folder,
                        self.temp_folder,
                        head_ip+":"+meta_info["gcs_port"],
                        meta_info["redis_password"],
                        parsed_init_command,
                        parsed_add_lsf_command
                    )

                    # Store pending info: will be updated by non_terminate_node
                    self.state.put_node(node_id, node_info)


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
        common_args = {
            "log_prefix": log_prefix,
            "node_id": node_id,
            "provider": self,
            "auth_config": auth_config,
            "cluster_name": cluster_name,
            "process_runner": process_runner,
            "use_internal_ip": use_internal_ip,
            "under_lsf" : True,
        }
        return EmptyCommandRunner(**common_args)  


    def terminate_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Terminates a node under LSF.

        Optionally return a mapping from deleted node ids to node
        metadata.
        """

        workers = self.state.get_nodes()
        if node_id not in workers:
            cli_logger.warning("Trying to terminate non-exsiting node\n")
            return
        
        with self.state.lock:
            with self.state.file_lock:

                lsf_cancel_job(node_id)
                self.state.delete_node(node_id)
    
    def non_terminated_nodes(self, tag_filters: Dict[str, str]) -> List[str]:
        """Return a list of node ids filtered by the specified tags dict.

        This list must not include terminated nodes. For performance reasons,
        providers are allowed to cache the result of a call to
        non_terminated_nodes() to serve single-node queries
        (e.g. is_running(node_id)). This means that non_terminate_nodes() must
        be called again to refresh results.

        The node states on file will be updated by checking the LSF job status.
        Other node information on file remains unchanged
        """

        workers = self.state.get_nodes()
        matching_ids = []
        for worker_id, info in workers.items():
            
            if worker_id.startswith(LSF_NODE_PREFIX):
                # Update node status
                lsf_job_status = lsf_get_job_status(worker_id)
                if LSF_JOB_TRANS_MAP[lsf_job_status] != info["state"]:
                    info["state"] = LSF_JOB_TRANS_MAP[lsf_job_status]
                    if info["state"] == NODE_STATE_TERMINATED:
                        self.state.delete_node(worker_id)
                    else:
                        self.state.put_node(worker_id, info)
            
                if info["state"] == NODE_STATE_TERMINATED: 
                    continue

                ok = True
                for k, v in tag_filters.items():
                    if info["tags"].get(k) != v:
                        ok = False
                        break
                if ok:
                    matching_ids.append(worker_id)

        return matching_ids

    def is_running(self, node_id: str) -> bool:
        """Return whether the specified node under slur is running."""
        return lsf_get_job_status(node_id) == LSF_JOB_RUNNING
    
    def is_terminated(self, node_id: str) -> bool:
        """Return whether the specified node is terminated."""
        return lsf_get_job_status(node_id) == LSF_JOB_NOT_EXIST

    def set_node_tags(self, node_id: str, tags: Dict[str, str]) -> None:
        """Sets the tag values (string dict) for the specified node."""
        with self.state.lock:
            with self.state.file_lock:
                workers = self.state.get_nodes()
                if node_id in workers:
                    info = workers[node_id]
                    info["tags"].update(tags)
                    self.state.put_node(node_id, info)
                    return
            
        cli_logger.warning("Set tags is called non-exsiting node\n")

    def node_tags(self, node_id: str) -> Dict[str, str]:
        """Returns the tags of the given node (string dict)."""

        workers = self.state.get_nodes()
        if node_id in workers:
            return workers[node_id]["tags"]
        else:
            cli_logger.warning("Get tags for non-existing node\n")
            return {}
    
    def internal_ip(self, node_id: str) -> Optional[str]:
        """Returns the internal ip (Ray ip) of the given node."""
        return lsf_get_job_ip(node_id)
