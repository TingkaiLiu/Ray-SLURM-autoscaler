'''Ray-Slurm Interface

Created by Tingkai Liu (tingkai2@illinois.edu) on June 10, 2022

'''

# To be filled by deployment script
LSF_IP_LOOKUP = [_DEPLOY_SLURM_IP_LOOKUP_] 
# TODO: LSF? 


# Const
NODE_STATE_RUNNING = "running"
NODE_STATE_PENDING = "pending"
NODE_STATE_TERMINATED = "terminated"

PASSWORD_LENGTH = 10
WAIT_HEAD_INTEVAL = 5 # second

BARE_NODE_TYPE_TAG = "BARE" # only for head outside slurm
SLURM_NODE_TYPE_TAG = "SLURM" # Not used
K8S_NODE_TYPE_TAG = "K8S"
LSF_NODE_TYPE_TAG = "LSF"

SLURM_NODE_PREFIX = "SLURM-" # Not used
K8S_NODE_PREFIX = "k8s-ray-" # have to user lower case in k8s
LSF_NODE_PREFIX = "LSF-"
