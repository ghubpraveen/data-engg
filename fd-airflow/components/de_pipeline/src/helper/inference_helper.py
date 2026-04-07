from components.de_pipeline.src import constants as constant
from components.de_pipeline.src.helper.helper import Helper
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
import json
from urllib.parse import urlparse
import logging
from google.cloud import storage
import re
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator
)
from airflow.models import Variable


@task.virtualenv(
    task_id="inference",
    requirements=["kfp==2.0.1", "protobuf==3.20.3", "kfp-pipeline-spec==0.2.2", "google-cloud-aiplatform==1.24.1"],
    system_site_packages=False
)
def inference(location_groups, **context):
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
    # import test_kflow
    # from dags.components.ml_pipeline.src import inference_kubeflow

    print("Starting subprocess -- ")

    # file_location ="/home/airflow/gcs/dags/components/ml_pipeline/src/inference_kubeflow.py"
    file_location = "/opt/airflow/dags/components/ml_pipeline/src/inference_kubeflow.py"
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
    credential_path = "/opt/airflow/dags/components/de_pipeline/src/credentials.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
    os.environ['GOOGLE_CLOUD_PROJECT'] = "dollar-tree-project-369709"

    aiplatform.init(project=PROJECT_ID, staging_bucket=BUCKET_URI, location=REGION)

    @component(
        output_component_file="demo_component.yaml",
        base_image="python:3.7",
        packages_to_install=["google-cloud-aiplatform==1.24.1"],
    )
    def fd_custom_inference(location_groups: list = []) -> str:
        def get_infra_specs(location_groups: list):
            CONTAINER_TRAIN_IMAGE = "us-west1-docker.pkg.dev/dollar-tree-project-369709/fd-dask-docker/dask-mlinference-image:latest"
            #CONTAINER_TRAIN_IMAGE = "us-west1-docker.pkg.dev/dollar-tree-project-369709/fd-dask-docker/dask-mlinference-image@sha256:4309089c3566a631f2adcbe3c4fc4b5e62ce55dbde38a5d6c1a419b184b92c9f"
            worker_pool_specs = [{
                'machine_spec': {
                    'machine_type': 'n1-standard-32',
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
                        'gs://extracted-bucket-dollar-tree/darshan/historical/historical_feature_store/business_fs.parquet',
                        '--output_path', 'gs://extracted-bucket-dollar-tree/darshan/transformed/prediction',
                        '--location_group_list', " ".join(location_groups),
                        '--load_date', '2022-08-05',
                        '--dask_address', "local",
                        '--num_workers_local_cluster', '2',
                        '--num_threads_per_worker', '1',
                        '--memory_limit_local_worker', '100G',
                        '--dask_connection_timeout', '120',
                        '--local_dask_flag','Y'
                    ]
                },
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

        specs = get_infra_specs(location_groups=location_groups)
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
    def pipeline(location_groups: list = []):
        training_job_task = (
            fd_custom_inference(location_groups=location_groups)
            .set_display_name("fd-inference")
            .set_cpu_limit(CPU_LIMIT)
            .set_memory_limit(MEMORY_LIMIT)
        )

    def main(location_groups: list):
        print(" Running Kubeflow pipeline ** " + " ".join(location_groups))
        kfp.v2.compiler.Compiler().compile(
            pipeline_func=pipeline,
            pipeline_parameters={"location_groups": location_groups},
            package_path="component_level_settings.json",
        )

        aipipeline = aiplatform.PipelineJob(
            display_name="component-level-settings",
            template_path="component_level_settings.json",
            pipeline_root=PIPELINE_ROOT,
            enable_caching=False,
        )

        aipipeline.run()
        return location_groups

    main(location_groups=location_groups)
    return location_groups
