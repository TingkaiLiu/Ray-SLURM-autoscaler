
"""Ray-Slurm Autoscaler Interface

Created by Tingkai Liu (tingkai2@illinois.edu) on June 10, 2022
Using the node_provider.py template

"""

import subprocess
import json
import logging
from types import ModuleType
from typing import Any, Dict, List, Optional

from ray.autoscaler.command_runner import CommandRunnerInterface
from ray.autoscaler._private.slurm.empty_command_runner import EmptyCommandRunner

from ray.autoscaler._private.slurm.slurm_commands import (
    slurm_cancel_job,
    slurm_launch_head,
    slurm_launch_worker,
    slurm_get_job_ip,
    slurm_get_job_status,
    SLURM_JOB_RUNNING,
    SLURM_JOB_PENDING,
    SLURM_JOB_NOT_EXIST
)

from threading import RLock # reentrant lock
from filelock import FileLock # reentrant (recursive) lock

from ray.autoscaler._private.cli_logger import cli_logger

import copy
import os
import socket
import random 
import string
import time

logger = logging.getLogger(__name__)

# Const
NODE_STATE_RUNNING = "running"
NODE_STATE_PENDING = "pending"
NODE_STATE_TERMINATED = "terminated"

# Map slurm job status to node states
SLURM_JOB_TRANS_MAP = {
    SLURM_JOB_RUNNING : NODE_STATE_RUNNING,
    SLURM_JOB_PENDING : NODE_STATE_PENDING,
    SLURM_JOB_NOT_EXIST : NODE_STATE_TERMINATED
}


HEAD_NODE_ID_OUTSIDE_SLURM = "-1" # TODO: Pid? 

# Default values if not provided in the node config
HEAD_UNDER_SLURM = False
WORKER_UNDER_SLURM = True
DEFAULT_TEMP_FOLDER_NAME = "temp_script"

# The range for getting free ports
PORT_LOWER_BOUND = 20000
PORT_HIGHER_BOUND = 30000

PASSWORD_LENGTH = 10
WAIT_HEAD_INTEVAL = 5 # second

filelock_logger = logging.getLogger("filelock")
filelock_logger.setLevel(logging.WARNING)

''' Heler functions '''

def _test_free_port(local_ip: str, port: int) -> bool:
    """ Check if a port is free on a NIC

    Args:
        local_ip: The IP of specific NIC on the local machine
        port: The port to be tested
    """
    
    ret = True
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        s.bind((local_ip, port))
    except OSError:
        ret = False
    
    s.close()
    return ret

def _get_free_ports(local_ip: str, count: int) -> List[int]:
    """Return free ports on a specific NIC

    Args:
        local_ip: The IP of specific NIC on the local machine
        count: The number of ports to get
    """
    
    ret = []
    sockets = []

    for _ in count:
        s = socket.socket()

        # The released port would be reused immediately
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Find to an available port
        s.bind((local_ip, 0)) 

        sockets.append(s)
        ret.append(s.getsockname()[1])
        
    for s in sockets:
        s.close()

    return ret

def _get_free_ports_range(local_ip: str, lower_bound: int, higher_bound: int, count: int) -> List[int]:
    """Get a free ports within range [lower_boud, higher_bound] on a specific NIC

    Args:
        local_ip: The IP of specific NIC on the local machine
        lower_bound: the lower bound of the port range
        higher_bound: the higher bound of the port range
        count: the number ports to get
    """

    ret = []
    sockets = []

    for _ in range(count):
        s = socket.socket()

        # The released port would be reused immediately
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # random.seed() # default seed is system time
        
        try_again = True
        cur_port = random.randint(lower_bound, higher_bound) 
        while try_again:
            try:
                s.bind((local_ip, cur_port))
            except OSError:
                cur_port = random.randint(lower_bound, higher_bound) 
            else:
                try_again = False

        sockets.append(s)
        ret.append(s.getsockname()[1])

    for s in sockets:
        s.close()

    return ret


class SlurmClusterState:
    """Maintain additional information on file for slurm cluster
    (modified from local.ClusterState)

    File format:
    {
        "meta" : { # mainly about the head node
            "head_ip" : <head_ip>
            "head_id" : <head_id>
            "gcs_port" : <gcs_port>
            "client_port" : <ray_client_port>
            "dashboard_port" : <dashboard_port>
            "redis_password" : <redis_password>
        }
        "nodes" : {
            <id> : {
                "state" : <state>,
                "tags" : <tag>
            }
        }
    }

    Information needed for each node:
    1. Slurm job id (major key)
    2. State: running, pending, terminated (should just be deleted)
    3. Tag (set and read by updater)

    The node states on file may not be up to date---need to query slurm for updating
    The updates are performed by non-terminate-node() call
    """

    def __init__(self, lock_path, save_path, provider_config):
        
        self.lock = RLock()
        os.makedirs(os.path.dirname(lock_path), exist_ok=True)
        self.file_lock = FileLock(lock_path)
        self.save_path = save_path

        with self.lock:
            with self.file_lock:
                if os.path.exists(self.save_path): # Reload the cluser states
                    cluster_state = json.loads(open(self.save_path).read())
                    
                    # Sanity check for file that doesn't fit the format
                    if ("meta" not in cluster_state) or ("nodes" not in cluster_state):
                        cluster_state = {"meta":{}, "nodes":{}}
                    
                    with open(self.save_path, "w") as f:
                        logger.debug(
                            "ClusterState: Writing cluster state: {}".format(cluster_state["nodes"])
                        )
                        f.write(json.dumps(cluster_state))

                else:
                    cluster_state = {"meta":{}, "nodes":{}}
                    with open(self.save_path, "w") as f:
                        logger.debug(
                            "ClusterState: Writing cluster state: {}".format(cluster_state["nodes"])
                        )
                        f.write(json.dumps(cluster_state))

        logger.info(
            "ClusterState: Loaded cluster state: {}".format(list(cluster_state["nodes"]))
        )

    def _get(self):
        """Return all the whole cluster info on file.
        """
        
        with self.lock:
            with self.file_lock:
                cluster_state = json.loads(open(self.save_path).read())
                return cluster_state
    
    def get_meta_info(self):
        """Get the meta info section
        """
        cluster_state = self._get()
        return cluster_state["meta"]
        
    def put_meta_info(self, info):
        """Update the head ip and id (could be None) in cluster meta data 
        """
        
        assert "head_ip" in info
        assert "head_id" in info
        assert "gcs_port" in info
        assert "client_port" in info
        assert "dashboard_port" in info
        assert "redis_password" in info

        with self.lock:
            with self.file_lock:
                cluster_state = self._get()
                cluster_state["meta"] = info
                with open(self.save_path, "w") as f:
                    logger.info(
                        "ClusterState: "
                        "Writing cluster state: {}".format(list(cluster_state["meta"]))
                    )
                    f.write(json.dumps(cluster_state))
    
    def delete_meta_info(self):
        with self.lock:
            with self.file_lock:
                cluster_state = self._get()
                cluster_state["meta"] = {}
                with open(self.save_path, "w") as f:
                    logger.info(
                        "ClusterState: "
                        "Writing cluster state: {}".format(list(cluster_state["meta"]))
                    )
                    f.write(json.dumps(cluster_state))

    def get_head_ip(self):
        meta_info = self.get_meta_info()

        if "head_ip" in meta_info:
            return meta_info["head_ip"]
        else:
            return None
    
    def get_head_id(self):
        meta_info = self.get_meta_info()

        if "head_id" in meta_info:
            return meta_info["head_id"]
        else:
            return None
    
    def get_redis_password(self):
        meta_info = self.get_meta_info()

        if "redis_password" in meta_info:
            return meta_info["redis_password"]
        else:
            return None

    def get_nodes(self):
        """Return all the nodes info on file 
        """

        cluster_state = self._get()
        return cluster_state["nodes"]

    def put_node(self, worker_id: str, info):
        """Update the node info for certain worker_id.
        """
        
        assert "tags" in info
        assert "state" in info
        with self.lock:
            with self.file_lock:
                cluster_state = self._get()
                cluster_state["nodes"][worker_id] = info
                with open(self.save_path, "w") as f:
                    logger.info(
                        "ClusterState: "
                        "Writing cluster state: {}".format(list(cluster_state["nodes"]))
                    )
                    f.write(json.dumps(cluster_state))
    
    def delete_node(self, worker_id: str):
        """Delete a worker from file.
        """
        with self.lock:
            with self.file_lock:
                cluster_state = self._get()
                if worker_id in cluster_state["nodes"]:
                    cluster_state["nodes"].pop(worker_id)
                with open(self.save_path, "w") as f:
                    logger.info(
                        "ClusterState: "
                        "Writing cluster state: {}".format(list(cluster_state["nodes"]))
                    )
                    f.write(json.dumps(cluster_state))


class NodeProvider:
    """Interface for getting and returning nodes from a Cloud.

    **Important**: This is an INTERNAL API that is only exposed for the purpose
    of implementing custom node providers. It is not allowed to call into
    NodeProvider methods from any Ray package outside the autoscaler, only to
    define new implementations of NodeProvider for use with the "external" node
    provider option.

    Args:
        provider_config: The "provider" section of the autoscaler config yaml
        cluster_name: The "cluster_name" section of the autoscaler config yaml

    *** About Slurm node_provider: ***

    The nodes are distinguished in three different ways:
        node id: the slurm batch submission id for the node
        node name: the node name under slurm
        node ip: the local node ip

    Design hack:
    The "run setup command after a node is created" doesn't fit the slurm model. 
    As a result, all the setup are done at create_node() (inside the slurm script), 
    and a empty command runner is used. 
    For copying (rsync), since all the nodes share a file system, the file mounting is 
    not needed. The only place needed 

    The following things are stored in the temperory folder:
        1. The cluster states storage file
        2. The file lock for cluster states storage file
        3. Modified (from template) Slurm/Bash scripts for launching specific nodes


    """

    def __init__(self, provider_config: Dict[str, Any], cluster_name: str) -> None:
        """Init the node provider class.

        Three main things are done:
            1. Store the necessary information from provider_config
            2. Create the temperory folder (if not exist) and assign file paths for 
                cluster states storage and file lock
            3. Re-construct the cluster state information from file, if exist
        """
        
        self.provider_config = provider_config
        self.cluster_name = cluster_name

        # Helpers for get_node_id() function
        self._internal_ip_cache: Dict[str, str] = {} # node ip : node id
        self._external_ip_cache: Dict[str, str] = {} # should not be used

        # Create a folder to store modified scripts
        temp_folder_name = DEFAULT_TEMP_FOLDER_NAME
        if "temp_folder_name" in provider_config:
            temp_folder_name = provider_config["temp_folder_name"]
        
        os.makedirs(temp_folder_name, exist_ok=True)

        self.temp_folder = temp_folder_name
        self.template_folder = provider_config["template_path"]

        if not self.template_folder.endswith('/'):
            self.template_folder += '/'

        # self.head_ip = provider_config["head_ip"]

        # Will be replaced if the port is occupied
        self.gcs_port = provider_config["gcs_port"]
        self.ray_client_port = provider_config["ray_client_port"]
        self.dashboard_port = provider_config["dashboad_port"]

        lock_path = os.path.join(self.temp_folder,
                        "cluster-{}.lock".format(cluster_name))
        state_path = os.path.join(self.temp_folder,
                        "cluster-{}.state".format(cluster_name))
        self.state = SlurmClusterState(
            lock_path,
            state_path,
            provider_config,
        )
    
    @staticmethod
    def bootstrap_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Bootstraps the cluster config by adding env defaults if needed.
        
        Args:
            cluster_config: The whole autoscaler config yaml
        """
        return cluster_config

    @staticmethod
    def fillout_available_node_types_resources(
        cluster_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fills out missing "resources" field for available_node_types.
        
        Args:
            cluster_config: The whole autoscaler config yaml
        """
        # TODO: Future: overide to prevent user from messing up. Can also fill in default value here
        return cluster_config  
    
    def prepare_for_head_node(self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Returns a new cluster config with custom configs for head node.
        
        Args:
            cluster_config: The whole autoscaler config yaml
        """
        return cluster_config

    @property
    def max_terminate_nodes(self) -> Optional[int]:
        """The maximum number of nodes which can be terminated in one single
        API request. By default, this is "None", which means that the node
        provider's underlying API allows infinite requests to be terminated
        with one request.

        For example, AWS only allows 1000 nodes to be terminated
        at once; to terminate more, we must issue multiple separate API
        requests. If the limit is infinity, then simply set this to None.

        This may be overridden. The value may be useful when overriding the
        "terminate_nodes" method.
        """
        return None

    def is_readonly(self) -> bool:
        """Returns whether this provider is readonly.

        Readonly node providers do not allow nodes to be created or terminated.
        """
        return False


    def create_node( # TODO: set memory constraint
        self, node_config: Dict[str, Any], tags: Dict[str, str], count: int
    ) -> Optional[Dict[str, Any]]:
        """Creates a number of nodes within the namespace.

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

        if "head_node" not in current_conf:
            raise ValueError("Must specify whether the node is head in the node config")
        
        is_head_node = current_conf["head_node"] == 1
        if is_head_node and count != 1:
            raise ValueError("Cannot create more than one head")
        under_slurm = False

        if "under_slurm" in current_conf:
            under_slurm = current_conf["under_slurm"] == 1
        else:
            if is_head_node:
                under_slurm = HEAD_UNDER_SLURM
            else:
                under_slurm = WORKER_UNDER_SLURM
        
        parsed_init_command = ""
        if "init_commands" in current_conf:
            for init in current_conf["init_commands"]:
                parsed_init_command += init + "\n"
        
        parsed_add_slurm_command = ""
        if "additional_slurm_commands" in current_conf:
            for cmd in current_conf["additional_slurm_commands"]:
                parsed_add_slurm_command += cmd + "\n"


        if is_head_node:

            redis_password = ''.join(random.choices(string.ascii_uppercase + string.digits, k=PASSWORD_LENGTH))

            if under_slurm: # head node under slurm. Assume all ports are available
                
                node_info = {}
                node_info["state"] = NODE_STATE_PENDING
                node_info["tags"] = tags

                meta_info = {}
                meta_info["head_ip"] = None
                meta_info["head_id"] = None
                meta_info["gcs_port"] = self.gcs_port
                meta_info["client_port"] = self.ray_client_port
                meta_info["dashboard_port"] = self.dashboard_port
                meta_info["redis_password"] = redis_password

                with self.state.lock:
                    with self.state.file_lock:
                        node_id = slurm_launch_head(
                            self.template_folder,
                            self.temp_folder, 
                            self.gcs_port,
                            self.ray_client_port,
                            self.dashboard_port,
                            redis_password,
                            parsed_init_command,
                            parsed_add_slurm_command
                        )

                        meta_info["head_id"] = node_id

                        # Store pending info: will be updated by non_terminate_node
                        self.state.put_node(node_id, node_info)
                        self.state.put_meta_info(meta_info)
                
                # Wait until the head node is up
                cli_logger.warning("Waiting for the head to start...This can be block due to resource limit")
                cli_logger.warning("If you force quit here, please run 'ray down <cluster_config>.ymal afterward to clean up")
                
                while slurm_get_job_status(node_id) != SLURM_JOB_RUNNING:
                    time.sleep(WAIT_HEAD_INTEVAL)

                head_ip = slurm_get_job_ip(node_id)
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

            else: # if under_slurm
                
                if "head_ip" in current_conf:
                    head_ip = current_conf["head_ip"]
                else:
                    head_ip = socket.gethostbyname(socket.gethostname())
           
                # Test whether the given port is free. If not, get new free ports
                ray_ports = [self.gcs_port, self.ray_client_port, self.dashboard_port]
                replace_index = [] # the index of the busy ports to be replaced

                for i in range(len(ray_ports)):
                    if not _test_free_port(head_ip, int(ray_ports[i])):
                        cli_logger.warning("Port %s is not free. Replaced." % ray_ports[i])
                        replace_index.append(i)

                free_ports = _get_free_ports_range(head_ip, PORT_LOWER_BOUND, PORT_HIGHER_BOUND, len(replace_index))
                for i in range(len(replace_index)):
                    ray_ports[replace_index[i]] = str(free_ports[i])
                
                # Fill the launching file
                f = open(self.template_folder+"head.sh", "r")
                template = f.read()
                f.close()
                
                template = template.replace("[_PY_HEAD_NODE_IP_]", head_ip)
                template = template.replace("[_PY_PORT_]", ray_ports[0])
                template = template.replace("[_PY_INIT_COMMAND_]", parsed_init_command)
                template = template.replace("[_PY_RAY_CLIENT_PORT_]", ray_ports[1])
                template = template.replace("[_PY_DASHBOARD_PORT_]", ray_ports[2])
                template = template.replace("[_PY_REDIS_PASSWORD_]", redis_password)

                f = open(self.temp_folder+"/head.sh", "w")
                f.write(template)
                f.close()

                node_info = {}
                node_info["state"] = NODE_STATE_RUNNING
                node_info["tags"] = tags

                meta_info = {}
                meta_info["head_ip"] = head_ip
                meta_info["head_id"] = HEAD_NODE_ID_OUTSIDE_SLURM
                meta_info["gcs_port"] = ray_ports[0]
                meta_info["client_port"] = ray_ports[1]
                meta_info["dashboard_port"] = ray_ports[2]
                meta_info["redis_password"] = redis_password

                with self.state.lock:
                    with self.state.file_lock:
                        subprocess.run(["bash", "-l", self.temp_folder+"/head.sh"]) # TODO: check error
                        
                        self.state.put_node(HEAD_NODE_ID_OUTSIDE_SLURM, node_info)
                        self.state.put_meta_info(meta_info) 

                self._internal_ip_cache[head_ip] = HEAD_NODE_ID_OUTSIDE_SLURM 

                # Prepare the script for terminating node
                f = open(self.template_folder+"end_head.sh", "r")
                template = f.read()
                f.close()
                
                template = template.replace("[_PY_INIT_COMMAND_]", parsed_init_command)

                f = open(self.temp_folder+"/end_head.sh", "w")
                f.write(template)
                f.close()

        else: # worker node should under slurm
            '''
                Since worker node is started by the autoscaler thread
                The head node is garenteed to be started at this time
                So querying the meta info here is safe
            '''
            assert under_slurm

            
            
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
                        node_id = slurm_launch_worker(
                            self.template_folder,
                            self.temp_folder,
                            head_ip+":"+meta_info["gcs_port"],
                            meta_info["redis_password"],
                            parsed_init_command,
                            parsed_add_slurm_command
                        )

                        # Store pending info: will be updated by non_terminate_node
                        self.state.put_node(node_id, node_info)


    def create_node_with_resources_and_labels(
        self,
        node_config: Dict[str, Any],
        tags: Dict[str, str],
        count: int,
        resources: Dict[str, float],
        labels: Dict[str, str], # TODO: Not handled yet
    ) -> Optional[Dict[str, Any]]:
        """Create nodes with a given resource config. 

        Ignore this function for now---simply forward the call to create_node()
        """

        return self.create_node(node_config, tags, count)
    
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
            "under_slurm" : node_id != HEAD_NODE_ID_OUTSIDE_SLURM,
        }

        # if docker_config and docker_config["container_name"] != "":
        #     return DockerCommandRunner(docker_config, **common_args)
        # else:
        #     return SSHCommandRunner(**common_args)

        return EmptyCommandRunner(**common_args)  

    def terminate_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Terminates the specified node.

        Optionally return a mapping from deleted node ids to node
        metadata.
        """

        workers = self.state.get_nodes()
        head_id = self.state.get_head_id()

        if node_id not in workers:
            cli_logger.warning("Trying to terminate non-exsiting node\n")
            return
        
        with self.state.lock:
            with self.state.file_lock:
                if node_id == HEAD_NODE_ID_OUTSIDE_SLURM:
                    subprocess.run(["bash", "-l", self.temp_folder+"/end_head.sh"]) # TODO: check error
                else:
                    slurm_cancel_job(node_id)
                
                self.state.delete_node(node_id)

                if node_id == head_id:
                    self.state.delete_meta_info()

        # if self.launched_nodes[node_id][INFO_IP_INDEX] in self._internal_ip_cache:
        #     self._internal_ip_cache.pop(self.launched_nodes[node_id][INFO_IP_INDEX])


        # TODO: check head? 


    def terminate_nodes(self, node_ids: List[str]) -> Optional[Dict[str, Any]]:
        """Terminates a set of nodes.

        May be overridden with a batch method, which optionally may return a
        mapping from deleted node ids to node metadata.
        """
        for node_id in node_ids:
            logger.info("NodeProvider: {}: Terminating node".format(node_id))
            self.terminate_node(node_id)
        return None


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
        
        workers = self.state.get_nodes()
        matching_ids = []
        for worker_id, info in workers.items():
            
            if worker_id != HEAD_NODE_ID_OUTSIDE_SLURM:
                # Update node status
                slurm_job_status = slurm_get_job_status(worker_id)
                if SLURM_JOB_TRANS_MAP[slurm_job_status] != info["state"]:
                    info["state"] = SLURM_JOB_TRANS_MAP[slurm_job_status]
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
        """Return whether the specified node is running."""

        if node_id == HEAD_NODE_ID_OUTSIDE_SLURM:
            return True # TODO:
        else:
            return slurm_get_job_status(node_id) == SLURM_JOB_RUNNING


    def is_terminated(self, node_id: str) -> bool:
        """Return whether the specified node is terminated."""
        if node_id == HEAD_NODE_ID_OUTSIDE_SLURM:
            return False # TODO:
        else:
            return slurm_get_job_status(node_id) == SLURM_JOB_NOT_EXIST
    
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

    def external_ip(self, node_id: str) -> Optional[str]:
        """Returns the external ip of the given node."""
        raise NotImplementedError("Must use internal IPs with slurm")

    def internal_ip(self, node_id: str) -> Optional[str]:
        """Returns the internal ip (Ray ip) of the given node."""
        if node_id == HEAD_NODE_ID_OUTSIDE_SLURM:
            return self.state.get_head_ip()
        else:
            return slurm_get_job_ip(node_id)

    def get_node_id(self, ip_address: str, use_internal_ip: bool = True) -> str:
        """Returns the node_id given an IP address.

        Assumes ip-address is unique per node.

        This function also updates the whole ip_cache if current cache information
        cannot satisfy the query. The update is done by calling non_terminated_nodes()
        to get all the nodes, and then get the IP address of each of them. As a result
        of non_terminated_nodes() call, the node states on file are also updated. 

        Args:
            ip_address: Address of node.
            use_internal_ip: Whether the ip address is
                public or private.

        Raises:
            ValueError if not found.
        """

        if not use_internal_ip:
            raise ValueError("Must use internal IPs with slurm")

        def find_node_id():
            if use_internal_ip:
                return self._internal_ip_cache.get(ip_address)
            else:
                return self._external_ip_cache.get(ip_address)

        if not find_node_id(): 
            all_nodes = self.non_terminated_nodes({})
            ip_func = self.internal_ip if use_internal_ip else self.external_ip
            ip_cache = (
                self._internal_ip_cache if use_internal_ip else self._external_ip_cache
            )
            for node_id in all_nodes:
                ip_cache[ip_func(node_id)] = node_id

        if not find_node_id():
            if use_internal_ip:
                known_msg = f"Worker internal IPs: {list(self._internal_ip_cache)}"
            else:
                known_msg = f"Worker external IP: {list(self._external_ip_cache)}"
            raise ValueError(f"ip {ip_address} not found. " + known_msg)

        return find_node_id()
        
    def safe_to_scale(self) -> bool:
        """Optional condition to determine if it's safe to proceed with an autoscaling
        update. Can be used to wait for convergence of state managed by an external
        cluster manager.

        Called by the autoscaler immediately after non_terminated_nodes().
        If False is returned, the autoscaler will abort the update.
        """
        return True

    def post_process(self) -> None:
        """This optional method is executed at the end of
        StandardAutoscaler._update().
        """
        pass
