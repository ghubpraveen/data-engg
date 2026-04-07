import os
import logging
from datetime import datetime
from airflow.decorators import dag, task, task_group
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from ghost_calc_pipelines.components.de_pipeline.src.helper.pipeline_helper import PipelineHelper
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
def validator(ingested_files, **context):
    print(" ingested files list ")
    logging.info(ingested_files)
    missing_files = context["ti"].xcom_pull(key="missing_files")
    logging.info(missing_files)
    obj_helper = PipelineHelper()
    obj_helper.submit_validator_job(ingested_files, missing_files, context)
    return ingested_files


@task
def preprocess(valid_files, **context):
    obj_helper = PipelineHelper()
    obj_helper.submit_preprocess_job(valid_files, context)
    return "path_to_preprocessed_dir"


@task
def get_list_location_groups(path_to_preprocessed_dir):
    # toDo :change static path
    obj_helper = PipelineHelper()
    location_groups = obj_helper.get_location_groups()
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
    def delete_job(batch_id):
        return location_groups

    return delete_job(submit_job(location_groups))


@task
def merge(location_groups):
    print("merge")
    location_groups=[['0.0', '1.0', '10.0', '100.0', '101.0', '102.0', '103.0', '104.0', '105.0', '106.0'], ['107.0', '108.0', '109.0', '11.0', '110.0', '111.0', '112.0', '113.0', '114.0', '115.0'], ['116.0', '117.0', '118.0', '119.0', '12.0', '120.0', '121.0', '122.0', '123.0', '124.0'], ['125.0', '126.0', '127.0', '128.0', '129.0', '13.0', '130.0', '131.0', '132.0', '133.0'], ['134.0', '135.0', '136.0', '137.0', '138.0', '139.0', '14.0', '140.0', '141.0', '142.0'], ['143.0', '144.0', '145.0', '146.0', '147.0', '148.0', '149.0', '15.0', '150.0', '151.0'], ['152.0', '153.0', '154.0', '155.0', '156.0', '157.0', '158.0', '159.0', '16.0', '160.0'], ['161.0', '162.0', '163.0', '164.0', '165.0', '166.0', '167.0', '168.0', '169.0', '17.0'], ['170.0', '171.0', '172.0', '173.0', '174.0', '175.0', '176.0', '177.0', '178.0', '179.0'], ['18.0', '180.0', '181.0', '182.0', '183.0', '184.0', '185.0', '186.0', '187.0', '188.0'], ['189.0', '19.0', '2.0', '20.0', '200.0', '201.0', '202.0', '203.0', '204.0', '205.0'], ['206.0', '207.0', '208.0', '209.0', '21.0', '210.0', '211.0', '212.0', '213.0', '214.0'], ['215.0', '216.0', '217.0', '218.0', '219.0', '22.0', '220.0', '221.0', '222.0', '223.0'], ['224.0', '225.0', '226.0', '227.0', '228.0', '229.0', '23.0', '230.0', '231.0', '232.0'], ['233.0', '234.0', '235.0', '236.0', '237.0', '238.0', '239.0', '24.0', '240.0', '241.0'], ['242.0', '243.0', '244.0', '245.0', '246.0', '247.0', '248.0', '249.0', '25.0', '250.0'], ['251.0', '252.0', '26.0', '260.0', '27.0', '28.0', '29.0', '3.0', '30.0', '31.0'], ['32.0', '33.0', '34.0', '35.0', '36.0', '37.0', '38.0', '39.0', '4.0', '40.0'], ['41.0', '42.0', '43.0', '44.0', '45.0', '46.0', '47.0', '48.0', '49.0', '5.0'], ['50.0', '51.0', '52.0', '53.0', '54.0', '55.0', '56.0', '57.0', '58.0', '59.0'], ['60.0', '61.0', '62.0', '63.0', '64.0', '65.0', '66.0', '67.0', '68.0', '69.0'], ['7.0', '70.0', '71.0', '72.0', '73.0', '74.0', '75.0', '76.0', '77.0', '78.0'], ['79.0', '8.0', '80.0', '81.0', '82.0', '83.0', '84.0', '85.0', '86.0', '87.0'], ['88.0', '89.0', '9.0', '90.0', '91.0', '92.0', '93.0', '94.0', '95.0', '96.0'], ['97.0', '98.0', '99.0']]
    return location_groups


@task
def business_feature_store(location_groups,**context):
    print(" business_feature_store")
    obj_helper = PipelineHelper()
    obj_helper.submit_bfs_job(location_groups,context)
    return location_groups


@task
def call_inference_pipeline(location_groups, **context):
    print(" inference")
    obj_helper = PipelineHelper()
    return obj_helper.run_inference(context)


@task.virtualenv(
    task_id="virtualenv_python", requirements=["kfp==2.0.1","protobuf==3.20.3","kfp-pipeline-spec==0.2.2","google-cloud-aiplatform==1.24.1"], system_site_packages=False
)
def callable_virtualenv():
    """
    Example function that will be performed in a virtual environent.

    Importing at the module level ensures that it will not attempt to import the
    library before it is installed.
    """
    from time import sleep
    import subprocess
    import json

    from google.cloud import aiplatform
    # from google_cloud_pipeline_components.v1.custom_job import create_custom_training_job_from_component
    import kfp
    import kfp.v2.dsl as dsl
    from kfp.v2.dsl import component, Output, HTML
    #import test_kflow
    #from dags.ghost_calc_pipelines.components.ml_pipeline.src import inference_kubeflow

    print("Starting subprocess -- ")

    #file_location ="/home/airflow/gcs/dags/ghost_calc_pipelines/components/ml_pipeline/src/inference_kubeflow.py"
    file_location = "/opt/airflow/dags/ghost_calc_pipelines/components/ml_pipeline/src/inference_kubeflow.py"
    """
    result = subprocess.run(
        ["python3", file_location],
        capture_output=True, text=True,check=True)
    

    print(result.stdout)

    print(result.stderr)

    print(result.args)"""
    import json
    import os
    from google.cloud import aiplatform
    # from google_cloud_pipeline_components.v1.custom_job import create_custom_training_job_from_component
    import kfp
    import kfp.v2.dsl as dsl
    from kfp.v2.dsl import component, Output, HTML

    PROJECT_ID = 'dollar-tree-project-369709'
    BUCKET_URI = 'extracted-bucket-dollar-tree'
    REGION = "us-west1"
    credential_path = "/opt/airflow/dags/ghost_calc_pipelines/components/de_pipeline/src/credentials.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
    os.environ['GOOGLE_CLOUD_PROJECT'] = "dollar-tree-project-369709"

    aiplatform.init(project=PROJECT_ID, staging_bucket=BUCKET_URI, location=REGION)

    @component(
        output_component_file="demo_component.yaml",
        base_image="python:3.7",
        packages_to_install=["google-cloud-aiplatform==1.24.1"],
    )
    def fd_custom_inference() -> str:
        def get_infra_specs():
            CONTAINER_TRAIN_IMAGE = "us-west1-docker.pkg.dev/dollar-tree-project-369709/fd-dask-docker/dask-mlinference-image:latest"
            worker_pool_specs = [{
                'machine_spec': {
                    'machine_type': 'e2-standard-4',
                },
                'replica_count': 1,
                'container_spec': {
                    'image_uri': CONTAINER_TRAIN_IMAGE,
                    "args": [
                        '--source_path_model_s1',
                        'gs://user-bucket-dollar-tree/szymon/data-science-dev/train/xgb/20230705_165153/model.json',
                        '--source_path_model_s2',
                        'gs://user-bucket-dollar-tree/szymon/data-science-dev/train/xgb/20230705_165516/model.json',
                        '--source_path_model_s3',
                        'gs://user-bucket-dollar-tree/szymon/data-science-dev/train/xgb/20230705_165911/model.json',
                        '--source_path_model_s4',
                        'gs://user-bucket-dollar-tree/szymon/data-science-dev/train/xgb/20230705_170235/model.json',
                        '--decision_threshold_step_1', '0.5',
                        '--decision_threshold_step_2', '0.5',
                        '--business_feature_store_base_path',
                        'gs://user-bucket-dollar-tree/Dipeshkumar.p/inference_test',
                        '--output_path', 'gs://extracted-bucket-dollar-tree/darshan/transformed/prediction',
                        '--location_group_list', '132.0',
                        '--load_date', '2022-08-08'
                    ]
                },
            }, {
                'machine_spec': {
                    'machine_type': 'e2-standard-4',
                },
                'replica_count': 1,
                'container_spec': {
                    'image_uri': CONTAINER_TRAIN_IMAGE,
                    "args": [
                        '--num_workers', '2',
                        '--no_worker_threads', '2',
                        '--memory_limit', '100G'
                    ]
                }
            }]
            return worker_pool_specs

        def train(job_spec):
            from google.cloud import aiplatform

            my_job = aiplatform.CustomJob(
                display_name="inference",
                worker_pool_specs=job_spec,
                staging_bucket="gs://extracted-bucket-dollar-tree/darshan/raw_data/stage_bucket/",
                project='dollar-tree-project-369709',
                location='us-west1'
            )

            my_job.run(service_account="prod-vm-win-darshan@dollar-tree-project-369709.iam.gserviceaccount.com")

        specs = get_infra_specs()
        train(specs)

        return "success"

    PIPELINE_ROOT = "gs://{}/darshan/raw_data/machine_settings".format(BUCKET_URI)

    CPU_LIMIT = "16"  # vCPUs
    MEMORY_LIMIT = "100G"

    @dsl.pipeline(
        name="component-fd-custom-inference",
        description="A simple pipeline that requests component-level machine resource",
        pipeline_root=PIPELINE_ROOT,
    )
    def pipeline():
        training_job_task = (
            fd_custom_inference()
            .set_display_name("fd-inference")
            .set_cpu_limit(CPU_LIMIT)
            .set_memory_limit(MEMORY_LIMIT)
        )

    def main():
        print(" Running Kubeflow pipeline ** ")
        kfp.v2.compiler.Compiler().compile(
            pipeline_func=pipeline,
            package_path="component_level_settings.json",
        )

        aipipeline = aiplatform.PipelineJob(
            display_name="component-level-settings",
            template_path="component_level_settings.json",
            pipeline_root=PIPELINE_ROOT,
            enable_caching=False,
        )

        aipipeline.run()

    main()

@dag(schedule_interval=None, default_args=default_args, catchup=False)
def prod_pipeline_v2():
    callable_virtualenv()



dag = prod_pipeline_v2()
