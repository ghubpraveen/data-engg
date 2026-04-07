import os
import sys
import argparse
import json
import kfp
import kfp.v2.dsl as dsl
from kfp.v2.dsl import component, Output, HTML
from pathlib import Path
import datetime

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = 'dollar-tree-project-369709'
REGION = 'us-west1'
PIPELINE_ROOT = 'gs://user-bucket-dollar-tree/darshan/vertex_pipeline'
BUCKET_NAME = 'extracted-bucket-dollar-tree'
BASE_PATH = 'Ramalingam/phase2/3M/auditonly/vertex_pipeline'


@component(
    base_image="python:3.7",
    packages_to_install=["google-cloud-aiplatform==1.24.1"],
    output_component_file=str(Path(__file__).with_suffix(".yaml")),
)
def custom_inference_job(
        project_id: str,
        project_location: str,
        container_uri: str,
        staging_bucket: str,
        job_name: str,
        job_suffix: str,
        replica_count: int,
        master_machine_type: str,
        worker_machine_type: str,
        run_type: str,
        memory_limit: str,
        base_gcs_path: str,
        num_workers: int,
        no_worker_threads: int,
        data_split_path: str,
        fs_eval_path: str,
        location_group: str):
    """Run a custom training job using a training script.
    """
    import json
    import logging
    import os.path
    import time
    import google.cloud.aiplatform as aip

    worker_pool_specs = [
        {
            "machine_spec": {
                "machine_type": master_machine_type,
            },
            "replica_count": 1,
            "container_spec": {
                "image_uri": container_uri,
                "command": ['bash', 'entrypoint.sh'],
                "args": [
                    '--run_name', job_name + '_' + job_suffix,
                    '--run_type', run_type,
                    '--memory_limit', memory_limit,
                    '--base_gcs_path', base_gcs_path,
                    '--num_workers', str(num_workers),
                    '--no_worker_threads', str(no_worker_threads),
                    '--data_split_path', data_split_path,
                    '--fs_eval_path', fs_eval_path,
                    '--location_group', location_group,
                    '--source_path_model_s1','gs://user-bucket-dollar-tree/szymon/data-science-dev/train/xgb'
                                             '/20230705_165153',
                    ' --source_path_model_s2','gs://user-bucket-dollar-tree/szymon/data-science-dev/train/xgb'
                                              '/20230705_165516',
                    '--source_path_model_s3','gs://user-bucket-dollar-tree/szymon/data-science-dev/train/xgb'
                                             '/20230705_165911',
                    '--source_path_model_s4','gs://user-bucket-dollar-tree/szymon/data-science-dev/data/split'
                                             '/20230705_163347',
                    '--decision_threshold_step_1','0.5',
                    '--decision_threshold_step_2','0.5'
                    '--business_feature_store_base_path','gs://user-bucket-dollar-tree/Dipeshkumar.p/inference_test/LOCATION_GROUP=132.0/LOAD_DATE=2022-08-08',
                    '--output_path','gs://user-bucket-dollar-tree/darshan/inference_output',
                    '--location_group_list','132',
                    '--load_date','2022-08-08',
                    '--num_workers','10',
                    '--threads_per_worker','2',
                    '--memory_limit','512GB'

                ],
            },
        },
        {
            "machine_spec": {
                "machine_type": worker_machine_type,
            },
            "replica_count": 3,
            "container_spec": {
                "image_uri": container_uri,
                "command": ['bash', 'entrypoint.sh'],
                "args": [
                    '--memory_limit', "512GB",
                    '--num_workers', str(47),
                    '--no_worker_threads', str(2)
                ],
            },
        }
    ]

    my_job = aip.CustomJob(
        display_name=job_name + '_' + job_suffix,
        worker_pool_specs=worker_pool_specs,
        staging_bucket=staging_bucket,
        project=project_id,
        location=project_location
    )

    my_job.run(service_account="prod-vm-win-darshan@dollar-tree-project-369709.iam.gserviceaccount.com")


@dsl.pipeline(
    name='model-training',
    description='Model Training',
    pipeline_root=PIPELINE_ROOT)
def pipeline(
        run_name: str = 'DEMO_INFERNCE',
        run_type: str = 'all',
        train_memory_limit: str = '512GB',
        train_num_workers: int = 47,
        train_no_worker_threads: int = 2,
        data_split_path: str = 'gs://extracted-bucket-dollar-tree/Ramalingam/phase2/3M/auditonly/vertex_training_datasplit/feature_store_3m_train-datasplit/data_split.parquet',
        location_group: str = 'ALL',
        train_replica_count: int = 3,
        train_master_machine_type: str = "n1-highmem-32",
        train_worker_machine_type: str = "n1-highmem-96"):


    run_name_suffix = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    run_name_suffix = str(run_name_suffix)
    run_name_suffix = run_name_suffix[0:8] + '_' + run_name_suffix[8:14] if len(
        run_name_suffix) and run_name_suffix.isdigit() else run_name_suffix

    custom_inference_job(
        project_id="dollar-tree-project-369709",
        project_location=REGION,
        container_uri='us-west1-docker.pkg.dev/dollar-tree-project-369709/fd-dask-docker/dask-mlinference-image:latest',
        staging_bucket='user-bucket-dollar-tree',
        job_name=run_name,
        job_suffix='Training' + '_' + run_name_suffix,
        replica_count=train_replica_count,
        master_machine_type=train_master_machine_type,
        worker_machine_type=train_worker_machine_type,
        run_type=run_type,
        memory_limit=train_memory_limit,
        base_gcs_path='gs://' + BUCKET_NAME + '/' + BASE_PATH,
        num_workers=train_num_workers,
        no_worker_threads=train_no_worker_threads,
        data_split_path=data_split_path,
        fs_eval_path=data_split_path,
        location_group=location_group)


if __name__ == '__main__':
    # try:
    versioned_pipeline_file = 'inference_pipeline.json'
    logger.info('--- Compile pipeline as V2 ---')
    kfp.v2.compiler.Compiler().compile(
        pipeline_func=pipeline,
        package_path=versioned_pipeline_file)
    # except Exception as e:
    #    logger.error(f'Error occured during pipeline compilation: {e}')