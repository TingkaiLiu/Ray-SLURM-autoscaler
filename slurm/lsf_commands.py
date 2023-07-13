
'''LSF Command interfaces

Created by Tingkai Liu (tingkai2@illinois.edu) on June 5, 2023

'''

import subprocess
from ray.autoscaler._private.cli_logger import cli_logger
from ray.autoscaler._private.slurm import (
    LSF_IP_LOOKUP, 
    LSF_NODE_PREFIX
)

# Public Const
LSF_JOB_RUNNING = "RUN"
LSF_JOB_PENDING = "PEND"
LSF_JOB_NOT_EXIST = "EXIT"

def lsf_cancel_job(job_id: str) -> None:
    if not job_id.startswith(LSF_NODE_PREFIX):
        cli_logger.warning("LSF interface: invalid job id")
        return
    job_id = job_id[len(LSF_NODE_PREFIX):]

    lsf_command = ["bkill", job_id]

    try:
        output = subprocess.check_output(lsf_command, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e: # happens when job not exist
        cli_logger.warning("LSF interface: invalid job id")
    
def lsf_launch_worker(
    template_folder: str, 
    temp_folder_name: str, # Not used
    head_ip_with_port: str, 
    redis_password: str,
    init_commands: str,
    additional_lsf_commands: str
) -> str:
    '''Launch a worker node under LSF
        Return the LSF job id as the node id

    '''

    f = open(template_folder+"worker.lsf", "r")
    template = f.read()
    f.close()
    
    template = template.replace("[_PY_ADD_LSF_CMD_]", additional_lsf_commands)
    template = template.replace("[_PY_IP_HEAD_]", head_ip_with_port)
    template = template.replace("[_PY_INIT_COMMAND_]", init_commands)
    template = template.replace("[_PY_REDIS_PASSWORD_]", redis_password)

    f = open(temp_folder_name+"/worker.lsf", "w")
    f.write(template)
    f.close()

    lsf_command = "bsub < " + temp_folder_name + "/worker.lsf"

    try:
        output = subprocess.check_output(lsf_command, stderr=subprocess.STDOUT, shell=True).decode()
    except subprocess.CalledProcessError as e: # happens when job not exist
        cli_logger.error("LSF error when starting worker: " + lsf_command, str(e))
        raise ValueError("LSF error when starting worker: ")

    comps = output.split() # Example output: 'Job <13486> is submitted to default queue <normal>.\n'
    job_id = comps[1][1:-1] 

    return LSF_NODE_PREFIX + job_id
    

def lsf_launch_head(
    template_folder: str, 
    temp_folder_name:str, # Not used
    gcs_port: str,
    ray_client_port: str,
    dashboard_port: str, 
    redis_password: str,
    init_commands: str,
    additional_lsf_commands: str
) -> str:
    # Replace the script with proper var
    f = open(template_folder+"head.lsf", "r")
    template = f.read()
    f.close()
    
    template = template.replace("[_PY_ADD_LSF_CMD_]", additional_lsf_commands)
    template = template.replace("[_PY_PORT_]", gcs_port)
    template = template.replace("[_PY_INIT_COMMAND_]", init_commands)
    template = template.replace("[_PY_RAY_CLIENT_PORT_]", ray_client_port)
    template = template.replace("[_PY_DASHBOARD_PORT_]", dashboard_port)
    template = template.replace("[_PY_REDIS_PASSWORD_]", redis_password)

    f = open(temp_folder_name+"/head.lsf", "w")
    f.write(template)
    f.close()

    lsf_command = "bsub < " + temp_folder_name+"/head.lsf"

    try:
        output = subprocess.check_output(lsf_command, stderr=subprocess.STDOUT, shell=True).decode()
    except subprocess.CalledProcessError as e: # happens when job not exist
        cli_logger.error("LSF error when starting head: " + lsf_command, str(e))
        raise ValueError("LSF error when starting head")

    comps = output.split() # Example output: 'Job <13486> is submitted to default queue <normal>.\n'
    job_id = comps[1][1:-1] 
    
    return LSF_NODE_PREFIX + job_id

def lsf_get_job_ip(job_id: str) -> str:
    '''Return ip of the node which job_id is assigned to 

        Assuming each job takes only one node
    '''

    if not job_id.startswith(LSF_NODE_PREFIX):
        cli_logger.warning("LSF interface: invalid job id")
        return None
    job_id = job_id[len(LSF_NODE_PREFIX):]
    
    lsf_command = ["bjobs", "-W", job_id]

    try:
        output = subprocess.check_output(lsf_command, stderr=subprocess.STDOUT).decode()
    except subprocess.CalledProcessError as e: # happens when job not exist
        return None

    hostname = output.splitlines()[1].split()[5]

    if hostname in LSF_IP_LOOKUP:
        return LSF_IP_LOOKUP[hostname]
    else:
        return None

def lsf_get_job_status(job_id: str) -> str:
    '''Return the job status given the job id
    '''

    if not job_id.startswith(LSF_NODE_PREFIX):
        cli_logger.warning("LSF interface: invalid job id")
        return LSF_JOB_NOT_EXIST
    job_id = job_id[len(LSF_NODE_PREFIX):]

    lsf_command = ["bjobs", "-W", job_id]

    try:
        output = subprocess.check_output(lsf_command, stderr=subprocess.STDOUT).decode()
    except subprocess.CalledProcessError as e: # happens when job not exist
        return LSF_JOB_NOT_EXIST

    status = output.splitlines()[1].split()[2]

    if status == "PEND":
        return LSF_JOB_PENDING
    elif status == "RUN":
        return LSF_JOB_RUNNING
    else:
        return LSF_JOB_NOT_EXIST # Give warning?

