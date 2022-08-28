
'''Slurm Command interfaces

Created by Tingkai Liu (tingkai2@illinois.edu) on June 17, 2022

'''

import subprocess
from ray.autoscaler._private.cli_logger import cli_logger
from ray.autoscaler._private.slurm import SLURM_IP_LOOKUP

# Public Const
SLURM_JOB_RUNNING = "R"
SLURM_JOB_PENDING = "PD"
SLURM_JOB_NOT_EXIST = "E"

# Private Const
SLURM_INFO_NODE_IDX = -1
SLURM_INFO_JOB_STATUS_IDX = 4

def slurm_cancel_job(job_id: str):
    if job_id.isdecimal():
        subprocess.run(["scancel", job_id])
    else:
        cli_logger.warning("Slurm interface: invalid job id")

def slurm_launch_worker(
    template_folder: str, 
    temp_folder_name: str, 
    head_ip_with_port: str, 
    init_commands: str,
    additional_slurm_commands: str
) -> str:
    '''Launch a worker node under slurm 
        Return the slurm job id as the node id

        # TODO: Add support for different node type
    '''

    f = open(template_folder+"worker.slurm", "r")
    template = f.read()
    f.close()
    
    template = template.replace("[_PY_ADD_SLURM_CMD_]", additional_slurm_commands)
    template = template.replace("[_PY_IP_HEAD_]", head_ip_with_port)
    template = template.replace("[_PY_INIT_COMMAND_]", init_commands)

    f = open(temp_folder_name+"/worker.slurm", "w")
    f.write(template)
    f.close()

    slurm_command = ["sbatch", temp_folder_name+"/worker.slurm"]
    output = subprocess.check_output(slurm_command).decode()

    # Test slurm batch output
    worker_job_id = ""
    comp = output.split()
    if comp[-1].isdecimal():
        worker_job_id = comp[-1]
    else:
        cli_logger.error("Slurm error when starting worker")
        raise ValueError("Slurm error when starting worker")

    return worker_job_id
    

def slurm_launch_head(
    template_folder: str, 
    temp_folder_name:str,
    head_node: str,
    gcs_port: str,
    ray_client_port: str,
    dashboard_port: str, 
    init_commands: str,
    additional_slurm_commands: str
) -> str:
    # Replace the script with proper var
    f = open(template_folder+"head.slurm", "r")
    template = f.read()
    f.close()
    
    template = template.replace("[_PY_ADD_SLURM_CMD_]", additional_slurm_commands)
    template = template.replace("[_PY_HEAD_NODE_]", head_node)
    template = template.replace("[_PY_PORT_]", gcs_port)
    template = template.replace("[_PY_INIT_COMMAND_]", init_commands)
    template = template.replace("[_PY_RAY_CLIENT_PORT_]", ray_client_port)
    template = template.replace("[_PY_DASHBOARD_PORT_]", dashboard_port)

    f = open(temp_folder_name+"/head.slurm", "w")
    f.write(template)
    f.close()

    slurm_command = ["sbatch", temp_folder_name+"/head.slurm"]
    output = subprocess.check_output(slurm_command).decode()

    # Test slurm batch output
    head_job_id = ""
    comp = output.split()
    if comp[-1].isdecimal():
        head_job_id = comp[-1]
    else:
        cli_logger.error("Slurm error when starting head")
        raise ValueError("Slurm error when starting head")
    
    return head_job_id

def slurm_get_job_ip(job_id: str) -> str:
    '''Return ip of the node which job_id is assigned to 

        Assuming each job takes only one node
    '''
    
    slurm_command = ["squeue", "-j "+job_id]

    try:
        output = subprocess.check_output(slurm_command, stderr=subprocess.STDOUT).decode().splitlines()
    except subprocess.CalledProcessError as e:
        # cli_logger.warning(e)
        return None

    if len(output) != 2:
        return None

    node_name = output[1].split()[SLURM_INFO_NODE_IDX]
    if node_name in SLURM_IP_LOOKUP:
        return SLURM_IP_LOOKUP[node_name]
    else:
        return None

def slurm_get_job_status(job_id: str) -> str:
    '''Return the job status given the job id
    '''

    slurm_command = ["squeue", "-j "+job_id]

    try:
        output = subprocess.check_output(slurm_command, stderr=subprocess.STDOUT).decode().splitlines()
    except subprocess.CalledProcessError as e: # happens when job not exist
        # cli_logger.warning(e)
        return SLURM_JOB_NOT_EXIST
    
    if len(output) != 2:
        return SLURM_JOB_NOT_EXIST
    
    status = output[1].split()[SLURM_INFO_JOB_STATUS_IDX]
    if status == "PD":
        return SLURM_JOB_PENDING
    elif status == "R":
        return SLURM_JOB_RUNNING
    else:
        return SLURM_JOB_NOT_EXIST # Give warning?

