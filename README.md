

> Author: Tingkai Liu (tingkai2@illinois.edu)
> Date: Aug 26, 2022


# Ray-SLURM-autoscaler + K8s Cloud bursting

This repo includes the SLURM NodeProvider implementation for Ray with K8s Cloud bursting capability. With this side-package, the Ray cluster launcher can be used across a SLURM based cluster and a K8s Cloud Cluster, where each "node" in the Ray cluster corresponds to a SLURM batch job submission, or a container Pod with Ray runtime in K8s Cloud. Furthermore, the Ray autoscaler can also be utilized based on the autoscaler config yaml, where Ray will add more nodes (submit more SLURM batch jobs or start more pods) when the demand is not satisfied, and remove nodes (cancel SLURM jobs or remove pods) after the resource is idle for certain time. 

This package provides supports for the following scenario:

- The head node of the Ray cluster located on the login node of the SLURM cluster, and the nodes of the K8s Cloud node can be directly reachable from the HPC login node. In most cases, this implies the Cloud nodes have public IP addresses. 

# Deployment guide

Prerequisite: 
- Ray is needed to be installed. 
- K8s Python client is needed to be installed
- The credentials for acceesing the K8s Cloud resource (.kube/ directory) is in place

Download the package or run 

```
    git clone https://github.com/TingkaiLiu/Ray-SLURM-autoscaler.git
    cd Ray-SLURM-autoscaler
    git checkout slurm_k8s
```

A deployment script is provided. Before running the script, several fields are needed to be filled in deploy.py:

- The path of Ray library, for example:

`
/Users/<user_name>/opt/anaconda3/envs/<env_name>/lib/python3.9/site-packages/ray/
`

- The compute node name and IP address of the cluster under SLURM
- The size (number of CPUs) of one compute node
- The maximum active time of a slurm job

After filling the fields, run 

```
    cd Ray-SLURM-autoscaler
    python3 deploy.py
```

After deployed successfully, an autoscaler config yaml (ray-slurm.yaml) will be generated. User needs to further fill ray-slurm.yaml for specific configuration, just like using Ray on supported Cloud providers. Notice that SLURM-based autoscaler config has some special fields such as "head ip" and "additional SLURM commands". Please see the comments in the generated yaml file for detail. 

Specifically for the Cloud resources, user need to fill:

- The description of each Cloud node, with an available ports in the range of 30000-32767. It is recommended that the number of ports for a Cloud node is >= 3x number of CPUs of the node. 

After ray-slurm.yaml is filled, a Ray cluster with autoscaling capability can be started by the cluster launcher 

```
    ray up ray-slurm.yaml --no-config-cache
```

If you are deploying it as a system admin, after deployment, you can provide only ray-slurm.yaml for users to launch their custom Ray cluster and fill their own Cloud resource. 

## Manual deployment

Manual deployment is also possible. To do this, all the things done by the deployment script are needed to be performed. The deployment script does the following things:

- Fill the template SLURM script with compute node size (both head and worker) and time. 
- Fill the SLURM compute node names and IPs in \_\_init\_\_.py
- Copy the code and template script to Ray library path. 
- Fill the path name in autoscaler config yaml

TL;DR: Fill all the "Macros" with prefix "\_DEPLOY\_" and copy all files into ray package.

# Notes
The original Ray autoscaler assumes each "node" in the Ray cluster is a VM-like machine that can be SSH-ed into to run setup command, and those machines doesn't share a file system. As a result, the autoscaler treats "node allocation" and "node setup" as two steps,using two different thread to do the job, and requires ways to transmit files across different nodes (such as rsync). 

However, this model doesn't fit the SLURM cluster model. Each "node" in a SLURM cluster is a slurm job, in which "node allocation" and "node setup" are done at the same time. Furthermore, the nodes in the SLURM clusters usually share a file system and don't require inter-node file transmission. 

As a result, some hacking is performed in this site package. All the node setup are done when the node is created (when submitting the SLURM batch). The command runner (for setting up the nodes after node creation) is implemented to be empty, and the inter-node file transmission is implemented as local copying. For details about the autoscaler system structure and hacking, see this development guide: https://github.com/TingkaiLiu/ray/blob/NodeProvider_doc/doc/source/cluster/node-provider.rst

For the Cloud part, since the compute node in the HPC clusters are not usually directly reachable from public network, some high-level Ray packages may not be supported. This framework has the best support for embarrasingly parallel workloads, or workloads using the head node for centralized communication. 
