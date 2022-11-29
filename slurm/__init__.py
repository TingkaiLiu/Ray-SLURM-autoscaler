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

SLURM_NODE_PREFIX = "SLURM-"
K8S_NODE_PREFIX = "K8S-"
