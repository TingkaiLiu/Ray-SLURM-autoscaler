#!/bin/bash -l

#SBATCH --cpus-per-task=[_DEPLOY_WORKER_CPUS_]
#SBATCH --gpus-per-task=[_DEPLOY_WORKER_GPUS_]

#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --tasks-per-node=1
#SBATCH --time=[_DEPLOY_SLURM_JOB_TIME_]

[_PY_ADD_SLURM_CMD_]

set -x

ip_head="[_PY_IP_HEAD_]" # To be replaced by python laucher

[_PY_INIT_COMMAND_] # To be replaced by python laucher

echo "Starting WORKER"
srun --nodes=1 --ntasks=1 \
    ray start --address "$ip_head" \
    --num-cpus "${SLURM_CPUS_PER_TASK}" --num-gpus "${SLURM_GPUS_PER_TASK}" --block &

# __doc_worker_ray_end__

sleep infinity # wait forever to presist the ray runtime

