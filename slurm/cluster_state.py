import logging
import os
import json

from threading import RLock # reentrant lock
from filelock import FileLock # reentrant (recursive) lock
from ray.autoscaler._private.cli_logger import cli_logger

logger = logging.getLogger(__name__)

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
