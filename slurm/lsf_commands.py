
'''LSF Command interfaces

Created by Tingkai Liu (tingkai2@illinois.edu) on June 5, 2023

'''

from lsf import lsb
from ray.autoscaler._private.cli_logger import cli_logger
from ray.autoscaler._private.slurm import (
    LSF_IP_LOOKUP, # TODO:
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

    try:
        lsb.kill(job_id)
    except lsb.LsbJobNotFoundException:
        cli_logger.warning("LSF interface: invalid job id")
    except lsb.LsbException as e:
        cli_logger.warning("LSF error while cancelling job", str(e))
    
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

    # f = open(temp_folder_name+"/worker.lsf", "w")
    # f.write(template)
    # f.close()

    try:
        job_id = lsb.submit(template)
        print("Job submitted with ID:", job_id)
    except lsb.LsbSubmitException as e:
        cli_logger.error("LSF error when starting worker\n", str(e))
        raise ValueError("LSF error when starting worker")

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

    try:
        job_id = lsb.submit(template)
        print("Job submitted with ID:", job_id)
    except lsb.LsbSubmitException as e:
        cli_logger.error("LSF error when starting head\n", str(e))
        raise ValueError("LSF error when starting head")
    
    return LSF_NODE_PREFIX + job_id

def lsf_get_job_ip(job_id: str) -> str:
    '''Return ip of the node which job_id is assigned to 

        Assuming each job takes only one node
    '''

    if not job_id.startswith(LSF_NODE_PREFIX):
        cli_logger.warning("LSF interface: invalid job id")
        return None
    job_id = job_id[len(LSF_NODE_PREFIX):]
    
    try:
        job_info = lsb.openjobinfo(job_id)
        execution_host = job_info['JOB_EXECUTION_HOSTS'][0]
    except lsb.LsbJobNotFoundException:
        return None

    if execution_host in LSF_IP_LOOKUP:
        return LSF_IP_LOOKUP[execution_host]
    else:
        return None

def lsf_get_job_status(job_id: str) -> str:
    '''Return the job status given the job id
    '''

    if not job_id.startswith(LSF_NODE_PREFIX):
        cli_logger.warning("LSF interface: invalid job id")
        return LSF_JOB_NOT_EXIST
    job_id = job_id[len(LSF_NODE_PREFIX):]

    try:
        job_info = lsb.openjobinfo(job_id)
        status = job_info['JOB_STATUS']
    except lsb.LsbJobNotFoundException:
        return LSF_JOB_NOT_EXIST

    if status == "PEND":
        return LSF_JOB_PENDING
    elif status == "RUN":
        return LSF_JOB_RUNNING
    else:
        return LSF_JOB_NOT_EXIST # Give warning?

