import os
from datetime import datetime
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowException
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.models.taskinstance import TaskInstance


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["hdfs"])
def test_provider_hdfs():
    """
    This DAG uploads the local file "test_provider_hdfs.py" to "/tmp/test_provider_hdfs.py" path on HDFS
    service.
    """

    @task()
    def upload_delete_file(task_instance: TaskInstance, dag_run):
        # Initialize the WebHDFSHook using the pre-configured Airflow connection
        # 'webhdfs_default' should be set up in the Airflow Connections UI with the proper host/port/auth.
        hdfs_hook = WebHDFSHook(webhdfs_conn_id="webhdfs_default")

        # Use the git bundle path to upload a file from the repository
        local_file_path = os.path.join(
            task_instance.bundle_instance.path, "dags/test_provider_hdfs.py"
        )
        hdfs_destination_path = "/tmp/test_provider_hdfs.py"

        # 1. Upload file
        print(f"Uploading {local_file_path} to HDFS at {hdfs_destination_path}...")
        client = hdfs_hook.get_conn()
        client.upload(
            local_path=local_file_path, hdfs_path=hdfs_destination_path, overwrite=True
        )
        print("HDFS file upload complete!")

        # 2. Check if file exists after the upload
        file_info = client.status(hdfs_path=hdfs_destination_path)
        if not file_info:
            raise AirflowException(
                f"HDFS upload failed, {hdfs_destination_path} path does not exist."
            )
        print("HDFS file info: ", file_info)

        # 3. Remove file
        result = client.delete(hdfs_path=hdfs_destination_path)
        if not result:
            raise AirflowException(
                f"Failed to delete HDFS file {hdfs_destination_path}."
            )
        print("HDFS file was deleted successfully!")

    upload_delete_file()


test_provider_hdfs()
