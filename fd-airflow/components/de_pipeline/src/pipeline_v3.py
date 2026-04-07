import os
import logging
from datetime import datetime
from airflow.decorators import dag, task, task_group
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from ghost_calc_pipelines.components.de_pipeline.src.helper.pipeline_helper import PipelineHelper
from ghost_calc_pipelines.components.de_pipeline.src.helper import inference_helper
from ghost_calc_pipelines.components.de_pipeline.src.helper import inference_metrics_helper
from ghost_calc_pipelines.components.de_pipeline.src import constants as constant
import json
import traceback

default_args = {
    'start_date': days_ago(1),
}

"""
credential_path = "/opt/airflow/dags/ghost_calc_pipelines/components/de_pipeline/src/credentials.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
os.environ['GOOGLE_CLOUD_PROJECT'] = "dollar-tree-project-369709"
"""


def print_inf():
    print("infere")


def check_received_files():
    obj_helper = PipelineHelper()
    missing_files = obj_helper.get_missing_files()
    bash_command = obj_helper.get_bash_command(missing_files)
    return {"bash_command": bash_command, "missing_files": missing_files}


def initialize_pipeline():
    current_date_time = datetime.now()
    current_date = current_date_time.strftime("%Y%m%d")
    current_time = current_date_time.strftime("%H%M%S")
    run_id = current_date + "_" + current_time
    Variable.set(key="run_date", value=current_date)
    Variable.set(key="run_id", value=run_id)
    return run_id


@task_group(group_id='fd_ingestion')
def fd_ingestion():
    @task
    def trigger_ingestion(**context):
        current_date_time = datetime.now()
        current_date = Variable.get(key="run_date")
        current_time = current_date_time.strftime("%H%M%S")
        # ToDo : if current_date  is null use current date from datetime
        if current_date is "None":
            current_date = current_date_time.strftime("%Y%m%d")
        run_id = current_date + "-" + current_time
        Variable.set(key="run_id", value=run_id)
        obj_helper = PipelineHelper()
        bash_command = check_received_files()
        obj_helper.run_ingestion(bash_command["bash_command"], context)
        print(context["ti"].xcom_pull(key="session_id", task_ids="init_ingest"))
        return bash_command["missing_files"]

    @task
    def process_ingested_data(missing_files, **context):
        obj_helper = PipelineHelper()
        context["ti"].xcom_push(key="missing_files", value=missing_files)
        return obj_helper.get_valid_files_after_ingestion()

    return process_ingested_data(trigger_ingestion())


@task
def validator(ingested_files):

    @task
    def submit_job(ingested_files, **context):
        print(" ingested files list ")
        logging.info(ingested_files)
        missing_files = context["ti"].xcom_pull(key="missing_files")
        logging.info(missing_files)
        obj_helper = PipelineHelper()
        batch_id = obj_helper.submit_validator_job(ingested_files, missing_files, context)
        return batch_id

    @task
    def delete_job(batch_id, **context):
        obj_helper = PipelineHelper()
        obj_helper.delete_batch(constant.PREPROCESS, batch_id, context)
        return ingested_files

    return delete_job(submit_job(ingested_files))


@task_group
def preprocess(valid_files):

    @task
    def submit_job(valid_file, **context):
        obj_helper = PipelineHelper()
        batch_id = obj_helper.submit_preprocess_job(valid_files, context)
        return batch_id

    @task
    def delete_job(batch_id, **context):
        obj_helper = PipelineHelper()
        obj_helper.delete_batch(constant.PREPROCESS, batch_id, context)
        return "path_to_preprocessed_dir"

    return delete_job(submit_job(valid_files))


@task
def get_list_location_groups(path_to_preprocessed_dir,**context):
    # toDo :change static path
    obj_helper = PipelineHelper()
    location_groups = obj_helper.get_location_groups()
    context["ti"].xcom_push(key="location_groups", value=location_groups)
    return location_groups


@task_group
def denorm(location_groups):
    @task
    def submit_job(location_group, **context):
        logging.info(" location group received in submit job- ")
        obj_helper = PipelineHelper()
        batch_id = obj_helper.submit_denorm_job(location_group, context)
        return batch_id

    @task
    def delete_job(batch_id,**context):
        obj_helper = PipelineHelper()
        obj_helper.delete_batch(constant.DENORM,batch_id,context)
        return context["ti"].xcom_pull(key="location_groups")

    return delete_job(submit_job(location_groups))


@task
def merge(location_groups):
    print("merge")
    """
    location_groups = [['0.0', '1.0', '10.0', '100.0', '101.0', '102.0', '103.0', '104.0', '105.0', '106.0'],
                       ['107.0', '108.0', '109.0', '11.0', '110.0', '111.0', '112.0', '113.0', '114.0', '115.0'],
                       ['116.0', '117.0', '118.0', '119.0', '12.0', '120.0', '121.0', '122.0', '123.0', '124.0'],
                       ['125.0', '126.0', '127.0', '128.0', '129.0', '13.0', '130.0', '131.0', '132.0', '133.0'],
                       ['134.0', '135.0', '136.0', '137.0', '138.0', '139.0', '14.0', '140.0', '141.0', '142.0'],
                       ['143.0', '144.0', '145.0', '146.0', '147.0', '148.0', '149.0', '15.0', '150.0', '151.0'],
                       ['152.0', '153.0', '154.0', '155.0', '156.0', '157.0', '158.0', '159.0', '16.0', '160.0'],
                       ['161.0', '162.0', '163.0', '164.0', '165.0', '166.0', '167.0', '168.0', '169.0', '17.0'],
                       ['170.0', '171.0', '172.0', '173.0', '174.0', '175.0', '176.0', '177.0', '178.0', '179.0'],
                       ['18.0', '180.0', '181.0', '182.0', '183.0', '184.0', '185.0', '186.0', '187.0', '188.0'],
                       ['189.0', '19.0', '2.0', '20.0', '200.0', '201.0', '202.0', '203.0', '204.0', '205.0'],
                       ['206.0', '207.0', '208.0', '209.0', '21.0', '210.0', '211.0', '212.0', '213.0', '214.0'],
                       ['215.0', '216.0', '217.0', '218.0', '219.0', '22.0', '220.0', '221.0', '222.0', '223.0'],
                       ['224.0', '225.0', '226.0', '227.0', '228.0', '229.0', '23.0', '230.0', '231.0', '232.0'],
                       ['233.0', '234.0', '235.0', '236.0', '237.0', '238.0', '239.0', '24.0', '240.0', '241.0'],
                       ['242.0', '243.0', '244.0', '245.0', '246.0', '247.0', '248.0', '249.0', '25.0', '250.0'],
                       ['251.0', '252.0', '26.0', '260.0', '27.0', '28.0', '29.0', '3.0', '30.0', '31.0'],
                       ['32.0', '33.0', '34.0', '35.0', '36.0', '37.0', '38.0', '39.0', '4.0', '40.0'],
                       ['41.0', '42.0', '43.0', '44.0', '45.0', '46.0', '47.0', '48.0', '49.0', '5.0'],
                       ['50.0', '51.0', '52.0', '53.0', '54.0', '55.0', '56.0', '57.0', '58.0', '59.0'],
                       ['60.0', '61.0', '62.0', '63.0', '64.0', '65.0', '66.0', '67.0', '68.0', '69.0'],
                       ['7.0', '70.0', '71.0', '72.0', '73.0', '74.0', '75.0', '76.0', '77.0', '78.0'],
                       ['79.0', '8.0', '80.0', '81.0', '82.0', '83.0', '84.0', '85.0', '86.0', '87.0'],
                       ['88.0', '89.0', '9.0', '90.0', '91.0', '92.0', '93.0', '94.0', '95.0', '96.0'],
                       ['97.0', '98.0', '99.0']]"""
    return location_groups


@task_group
def business_feature_store(location_groups):

    @task
    def submit_job(location_group, **context):
        print(" business_feature_store")
        obj_helper = PipelineHelper()
        batch_id = obj_helper.submit_bfs_job(location_group, context)
        return batch_id

    @task
    def delete_job(batch_id,**context):
        obj_helper = PipelineHelper()
        obj_helper.delete_batch(constant.DENORM,batch_id,context)
        return context["ti"].xcom_pull(key="location_groups")

    return delete_job(submit_job(location_groups))


@dag(schedule_interval=None, default_args=default_args, catchup=False)
def prod_pipeline_v3():
    valid_files = validator.expand(ingested_files=fd_ingestion())
    location_groups = get_list_location_groups(preprocess.expand(valid_files=valid_files))
    batch = denorm.expand(location_groups=location_groups)
    fs_location_grp = merge(business_feature_store.expand(location_groups=merge(batch)))
    inference_helper.inference.expand(location_groups=fs_location_grp)
    inference_metrics_helper.inference_metrics.expand(location_groups=[["132.0"]])
    #inference_helper.inference.expand(location_groups=business_feature_store.expand(location_groups=merge(batch)))


dag = prod_pipeline_v3()
