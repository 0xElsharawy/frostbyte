from pathlib import Path
import polars as pl
import pyarrow.parquet as pq
from pyiceberg.schema import Schema
from pyiceberg.io.pyarrow import pyarrow_to_schema
from pyiceberg.catalog import load_catalog
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta


def csv_to_parquet():
    base_dir = Path(__file__).resolve().parent.parent
    input_dir = base_dir / "dataset"
    output_dir = base_dir / "parquet"
    output_dir.mkdir(exist_ok=True)

    for csv_path in input_dir.glob("*.csv"):
        if csv_path.name.endswith("_dataset.csv"):
            new_filename = csv_path.name.replace("_dataset.csv", ".parquet")
        else:
            new_filename = csv_path.name.replace(".csv", ".parquet")

        parquet_path = output_dir / new_filename

        print(f"Converting: {csv_path.name} → {new_filename}")

        pl.scan_csv(csv_path).sink_parquet(parquet_path, compression="snappy")

    return str(output_dir)


def load_to_minio(**context):
    parquet_dir = Path(context["ti"].xcom_pull(task_ids="csv_to_parquet"))

    hook = S3Hook(aws_conn_id="minio_conn")

    bucket = "lakehouse"
    prefix = "raw/"

    if hook.check_for_bucket(bucket_name=bucket):
        print("Successfully connected to MinIO!")

    for file in parquet_dir.glob("*.parquet"):
        object_name = f"{prefix}{file.name}"

        print(f"Uploading {file} → s3://{bucket}/{object_name}")

        hook.load_file(
            filename=str(file),
            key=object_name,
            bucket_name=bucket,
            replace=True,
        )


def create_iceberg_tables(**context):
    parquet_dir = Path(context["ti"].xcom_pull(task_ids="csv_to_parquet"))

    catalog_config = {
        "type": "sql",
        "uri": "postgresql+psycopg2://postgres:postgres@postgres:5432/iceberg_catalog",
        "warehouse": "s3://lakehouse",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.path-style-access": "true",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
    }

    catalog = load_catalog(
        "jdbc_catalog",
        **catalog_config,
    )
    bucket = "lakehouse"
    namespace = "raw"

    for file in parquet_dir.glob("*.parquet"):
        table_name = file.stem
        identifier = f"{namespace}.{table_name}"

        table_location = f"s3://{bucket}/{namespace}/{table_name}"

        arrow_schema = pq.read_schema(file)
        iceberg_schema = pyarrow_to_schema(arrow_schema)

        table = catalog.create_table(
            identifier, schema=iceberg_schema, location=table_location
        )

        remote_path = f"s3://{bucket}/raw/{table_name}/{file.name}"

        print(f"Registering {remote_path} into table {identifier}")
        table.add_files(file_paths=[remote_path])


default_args = {
    "owner": "ahmed",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="elt_dag",
    default_args=default_args,
    description="A simple ELT DAG to convert CSV to Parquet, load to MinIO and transform data with dbt",
    start_date=datetime(2022, 2, 2),
    schedule=None,
    catchup=False,
) as dag:
    csv_to_parquet_task = PythonOperator(
        task_id="csv_to_parquet",
        python_callable=csv_to_parquet,
    )

    load_to_minio_task = PythonOperator(
        task_id="load_to_minio",
        python_callable=load_to_minio,
    )

    create_iceberg_tables_task = PythonOperator(
        task_id="create_iceberg_tables",
        python_callable=create_iceberg_tables,
    )

    csv_to_parquet_task >> load_to_minio_task >> create_iceberg_tables_task
