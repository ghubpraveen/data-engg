import configparser as ConfigParser

from kfp import dsl
from kfp import client
from kfp import compiler
from kfp.dsl import ContainerSpec
import kfp.components as comp

from google.cloud import dataproc_v1

from component_utils import dataproc_component


@dsl.container_component
def sample_create_batch():
    return dsl.ContainerSpec(image='alpine', command=['echo'], args=['Hello'])


@dsl.container_component
def data_split_incremental(x: float, y: str) -> ContainerSpec:
    return dsl.ContainerSpec(image='alpine', command=['echo'], args=['Hello'])


@dsl.component
def validate(x: str, y: str, z: str) -> str:
    return "success"


@dsl.component
def merge_date_split(x: str, y: str) -> str:
    return dataproc_component.dataproc_create_pyspark_batch(
        project="dollar-tree-project-369709",
        main_python_file_uri="gs://vertex-scripts/de-scripts/data-split/data_split.py",
        location='us-west1',
        python_file_uris=["gs://vertex-scripts/de-scripts/data-split/pyspark/transform.py"],
        runtime_config_properties={"spark.cores.max": "15", "spark.executor.cores": "8", "spark.executor.memory": "6g"},
        args=["--input_dfs_path=gs://user-bucket-dollar-tree/Dipeshkumar.p/fs_single_day",
              "--output_df_path=gs://user-bucket-dollar-tree/Dipeshkumar.p/data_split_test/6_July/test2"]
    )


@dsl.pipeline
def my_pipeline(a: float, b: float) -> str:
    create_batch = dataproc_component.dataproc_create_pyspark_batch(
        project="dollar-tree-project-369709",
        main_python_file_uri="gs://vertex-scripts/de-scripts/data-split/data_split.py",
        location='us-west1',
        python_file_uris=["gs://vertex-scripts/de-scripts/data-split/pyspark/transform.py","gs://vertex-scripts/de"
                                                                                           "-scripts/data-split/log_utils.py",
                          "gs://vertex-scripts/de-scripts/data-split/file_utils.py","gs://vertex-scripts/de-scripts/data-split/gcs_utils.py","gs://vertex-scripts/de-scripts/data-split/logger_utils.py"],
        runtime_config_properties={"spark.cores.max": "15", "spark.executor.cores": "8", "spark.executor.memory": "6g"},
        args=["--input_path=gs://user-bucket-dollar-tree/Dipeshkumar.p/fs_single_day",
              "--data_split_out_path=gs://user-bucket-dollar-tree/Dipeshkumar.p/data_split_test/6_July/test2",
              "--location_group=106.0", "--target_column_list='MULTICLASS_TARGET'\\s'BINARY_TARGET'", "--run_mode=I",
              "--max_records_per_batch=10000","--log_file_path=gs://pipeline_audit/task-details/06152023_041923/data"
                                              "-split-details_tbl.csv","--run_id=sample","--task_id=task_sample"]
    )

    merge_train = dataproc_component.dataproc_create_pyspark_batch(
        project="dollar-tree-project-369709",
        main_python_file_uri="gs://vertex-scripts/de-scripts/data-split/data_split.py",
        location='us-west1',
        python_file_uris=["gs://vertex-scripts/de-scripts/data-split/pyspark/transform.py"],
        runtime_config_properties={"spark.cores.max": "15", "spark.executor.cores": "8", "spark.executor.memory": "6g"},
        args=["--input_dfs_path=gs://user-bucket-dollar-tree/Dipeshkumar.p/fs_single_day",
              "--output_df_path=gs://user-bucket-dollar-tree/Dipeshkumar.p/data_split_test/6_July/test2"],
        split_output_path=create_batch.output
    )
    merge_test = dataproc_component.dataproc_create_pyspark_batch(
        project="dollar-tree-project-369709",
        main_python_file_uri="gs://vertex-scripts/de-scripts/data-split/data_split.py",
        location='us-west1',
        python_file_uris=["gs://vertex-scripts/de-scripts/data-split/pyspark/transform.py"],
        runtime_config_properties={"spark.cores.max": "15", "spark.executor.cores": "8", "spark.executor.memory": "6g"},
        args=["--input_dfs_path=gs://user-bucket-dollar-tree/Dipeshkumar.p/fs_single_day",
              "--output_df_path=gs://user-bucket-dollar-tree/Dipeshkumar.p/data_split_test/6_July/test2"],
        split_output_path=create_batch.output
    )
    merge_valid = dataproc_component.dataproc_create_pyspark_batch(
        project="dollar-tree-project-369709",
        main_python_file_uri="gs://vertex-scripts/de-scripts/data-split/data_split.py",
        location='us-west1',
        python_file_uris=["gs://vertex-scripts/de-scripts/data-split/pyspark/transform.py"],
        runtime_config_properties={"spark.cores.max": "15", "spark.executor.cores": "8", "spark.executor.memory": "6g"},
        args=["--input_dfs_path=gs://user-bucket-dollar-tree/Dipeshkumar.p/fs_single_day",
              "--output_df_path=gs://user-bucket-dollar-tree/Dipeshkumar.p/data_split_test/6_July/test2"],
        split_output_path=create_batch.output
    )
    historical_task = validate(x=merge_train.output, y=merge_test.output, z=merge_valid.output)
    return historical_task.output


compiler.Compiler().compile(pipeline_func=my_pipeline, package_path='pipeline.yaml')

#Total executor cores: 15 is not divisible by cores per executor: 8, the left cores: 7 will not be allocated
