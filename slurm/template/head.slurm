#!/bin/bash -l

#SBATCH --cpus-per-task=[_DEPLOY_HEAD_CPUS_]
#SBATCH --gpus-per-task=[_DEPLOY_HEAD_GPUS_]

#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --tasks-per-node=1
#SBATCH --time=[_DEPLOY_SLURM_JOB_TIME_]

[_PY_ADD_SLURM_CMD_]

set -x

# __doc_head_address_start__

# Getting the node names
head_node="[_PY_HEAD_NODE_]" # To be replaced by python script
port="[_PY_PORT_]"
ray_client_port="[_PY_RAY_CLIENT_PORT_]"
dashboad_port="[_PY_DASHBOARD_PORT_]"
# __doc_head_address_end__

# __doc_head_ray_start__
head_node_ip=$(srun --nodes=1 --ntasks=1 -w "$head_node" hostname --ip-address)
ip_head=$head_node_ip:$port
echo "IP Head: $ip_head"


[_PY_INIT_COMMAND_] # To be replaced by python laucher

srun --nodes=1 --ntasks=1 -w "$head_node" \
    ray stop
srun --nodes=1 --ntasks=1 -w "$head_node" \
    ray start --head --node-ip-address="$head_node_ip" --port=$port --dashboard-port=$dashboad_port\
    --num-cpus "${SLURM_CPUS_PER_TASK}" --num-gpus "${SLURM_GPUS_PER_TASK}" \
    --ray-client-server-port "$ray_client_port" --autoscaling-config=~/ray_bootstrap_config.yaml --block &
# __doc_head_ray_end__


sleep infinity # wait forever to presist the ray runtime

