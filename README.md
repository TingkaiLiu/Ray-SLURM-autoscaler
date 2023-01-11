

> Author: Tingkai Liu (tingkai2@illinois.edu)
> Date: Aug 26, 2022


# Ray-SLURM-autoscaler + K8s Cloud bursting

(The K8s Cloud part of the document is still in progress)

This repo includes the SLURM NodeProvider implementation for Ray with K8s Cloud bursting capability. With this side-package, the Ray cluster launcher can be used on a SLURM based cluster, where each "node" in the Ray cluster corresponds to a SLURM batch job submission. Furthermore, the Ray autoscaler can also be utilized based on the autoscaler config yaml, where Ray will add more nodes (submit more SLURM batch jobs) when the demand is not satisfied, and remove nodes (cancel SLURM jobs) after the resource is idle for certain time. 

This package provides supports for two scenarios:

- The head node of the Ray cluster is outside of SLURM (i.e. on the login node of the cluster). This is useful when users want to connect to the Ray cluster remotely (using Ray client, for example), while the compute nodes of the cluster are not directly reachable from outside of the cluster. In such cases, it is recommended to set the CPU and GPU resource of the Ray head node to be 0, since the login node is not supposed to be used for computation. 

- The head node of the Ray cluster is under SLURM. This is useful when users are only using the Ray cluster when logging into the compute cluster, and want to avoid conflict with other users on the same cluster.  

# Deployment guide

Prerequisite: Ray is needed to be installed. 

Download the package or run 

```
    git clone https://github.com/TingkaiLiu/Ray-SLURM-autoscaler.git
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

After ray-slurm.yaml is filled, a Ray cluster with autoscaling capability can be started by the cluster launcher 

```
    ray up ray-slurm.yaml --no-config-cache
```

If you are deploying it as a system admin, after deployment, you can provide only ray-slurm.yaml for users to launch their custom Ray cluster. 

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
