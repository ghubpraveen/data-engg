import configparser as ConfigParser

from google.cloud import dataproc_v1


def sample_create_batch():
    # Create a client
    # Create the cluster client.
    project_id="dollar-tree-project-369709"
    cluster_name="test_cluster"
    region = "uswest1"

    cluster_client = dataproc_v1.ClusterControllerClient(
        client_options={"api_endpoint": "uswest1-dataproc.googleapis.com:443"}
    )

    # Create the cluster config.
    cluster = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
            "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
        },
    }

    # Create the cluster.
    operation = cluster_client.create_cluster(
        request={"project_id": project_id, "region": region, "cluster": cluster}
    )
    result = operation.result()

    print(f"Cluster created successfully: {result.cluster_name}")

    # Handle the response
    print(response)
    return "batch created"

sample_create_batch()