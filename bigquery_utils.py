import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from google.oauth2 import service_account
from google.cloud import bigquery


def _upload_temp_on_gs(df: DataFrame, blob_location: str, bq_table_name: str):
    print('Starting upload of table %s to gs' % bq_table_name)
    df.write.mode("overwrite").orc(blob_location)
    print('Upload complete')


def _get_credentials(json_keyfile_path):
    return service_account.Credentials.from_service_account_file(json_keyfile_path)


def _load_data_on_bq(blob_location: str, project_name: str, dataset_name: str, table_name: str, json_keyfile_path: str):
    credentials = _get_credentials(json_keyfile_path)
    bq = bigquery.Client(project=project_name, credentials=credentials)
    table_ref = bq.dataset(dataset_name).table(table_name)

    job_config = bigquery.LoadJobConfig()

    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.source_format = bigquery.SourceFormat.ORC
    job_config.autodetect = True

    load_job = bq.load_table_from_uri(
        source_uris=blob_location + "/*.orc",
        destination=table_ref,
        project=project_name,
        job_config=job_config
    )

    print('Starting ingestion of table %s.%s to Big Query' % (dataset_name, table_name))
    load_job.result()  # Waits for table load to complete.
    print('Ingestion finished.')
    destination_table = bq.get_table(table_ref)
    print('Loaded {} rows.'.format(destination_table.num_rows))


def _get_blob_location(bucket_name: str, path: str) -> str:
    return "gs://%s/tmp/big-query/%s" % (bucket_name, path)


def _delete_temp_on_gs(spark, json_keyfile_path, bucket_name, blob_location):
    sc = spark.sparkContext
    URI = sc._gateway.jvm.java.net.URI
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
    conf = Configuration()

    conf.set("fs.gs.auth.service.account.json.keyfile", json_keyfile_path)
    fs = FileSystem.get(URI("gs://%s" % bucket_name), conf)
    fs.delete(Path(blob_location))


def write_bq(spark: SparkSession, df: DataFrame, bucket_name, project_name, json_keyfile_path, dataset_name: str, table_name: str):
    assert os.path.isfile(json_keyfile_path), "File %s not found" % json_keyfile_path
    spark.conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark.conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    spark.conf.set("google.cloud.auth.service.account.enable", "true")
    spark.conf.set("fs.gs.auth.service.account.json.keyfile", json_keyfile_path)
    full_table_name = "%s.%s" % (dataset_name, table_name)
    blob_location = _get_blob_location(bucket_name, full_table_name)
    _upload_temp_on_gs(df, blob_location, full_table_name)
    _load_data_on_bq(blob_location, project_name, dataset_name, table_name, json_keyfile_path)
    _delete_temp_on_gs(spark, json_keyfile_path, bucket_name, blob_location)
