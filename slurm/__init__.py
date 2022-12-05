'''Ray-Slurm Interface

Created by Tingkai Liu (tingkai2@illinois.edu) on June 10, 2022

'''

# To be filled by deployment script
SLURM_IP_LOOKUP = [_DEPLOY_SLURM_IP_LOOKUP_] 


# Const
NODE_STATE_RUNNING = "running"
NODE_STATE_PENDING = "pending"
NODE_STATE_TERMINATED = "terminated"

PASSWORD_LENGTH = 10
WAIT_HEAD_INTEVAL = 5 # second

BARE_NODE_TYPE_TAG = "BARE" # only for head outside slurm
SLURM_NODE_TYPE_TAG = "SLURM"
K8S_NODE_TYPE_TAG = "K8S"

SLURM_NODE_PREFIX = "SLURM-"
K8S_NODE_PREFIX = "k8s-ray-" # have to user lower case in k8s
