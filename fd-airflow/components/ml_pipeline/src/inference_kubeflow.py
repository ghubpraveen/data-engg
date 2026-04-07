import json

from google.cloud import aiplatform
# from google_cloud_pipeline_components.v1.custom_job import create_custom_training_job_from_component
import kfp
import kfp.v2.dsl as dsl
from kfp.v2.dsl import component, Output, HTML

PROJECT_ID = 'dollar-tree-project-369709'
BUCKET_URI = 'extracted-bucket-dollar-tree'
REGION = "us-west1"

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
                    '--business_feature_store_base_path', 'gs://user-bucket-dollar-tree/Dipeshkumar.p/inference_test',
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
    from google_cloud_pipeline_components.types import artifact_types
    from google_cloud_pipeline_components.v1.model import ModelUploadOp
    from kfp.components import importer_node

    training_job_task = (
        fd_custom_inference()
        .set_display_name("fd-inference")
        .set_cpu_limit(CPU_LIMIT)
        .set_memory_limit(MEMORY_LIMIT)
    )


if __name__ == '__main__':
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
