from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import subprocess
import boto3
import os

# Definir variables globales
S3_BUCKET = "spaceflight"
GLUE_JOB_NAME = "test_glue"
DATABASE_NAME = "spaceflight"
IAM_ROLE_ARN = "arn:aws:iam::637423473516:role/aws-service-role/redshift.amazonaws.com/AWSServiceRoleForRedshift"
REDSHIFT_TABLE_SOURCE = "public.dim_news_source"
REDSHIFT_TABLE_TOPIC = "public.dim_topic"
REDSHIFT_TABLE_FACT = "public.fact_article"
fecha_actual = datetime.now().strftime('%Y%m%d')
def run_download_articles():
    s3 = boto3.client("s3")
    s3.download_file(S3_BUCKET, "script/import_article_date.py", "/tmp/import_article_date.py")
    subprocess.run(["python", "/tmp/import_article_date.py"], check=True)
    os.remove("/tmp/import_article_date.py")

def run_download_blogs():
    s3 = boto3.client("s3")
    s3.download_file(S3_BUCKET, "script/import_blogs_date.py", "/tmp/import_blogs_date.py")
    subprocess.run(["python", "/tmp/import_blogs_date.py"], check=True)
    os.remove("/tmp/import_blogs_date.py")


def run_download_reports():
    s3 = boto3.client("s3")
    s3.download_file(S3_BUCKET, "script/import_reports_date.py", "/tmp/import_reports_date.py")
    subprocess.run(["python", "/tmp/import_reports_date.py"], check=True)
    os.remove("/tmp/import_reports_date.py")
    subprocess.run(["python", "S3_BUCKET + /script/import_reports_date.py"], check=True)


# Definir argumentos del DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "data_pipeline",
    default_args=default_args,
    description="Orquestación de descarga, procesamiento y carga de datos",
    schedule_interval="@daily",
)

download_article = PythonOperator(
    task_id="download_article",
    python_callable=run_download_articles,
    dag=dag,
)


download_blog = PythonOperator(
    task_id="download_blog",
    python_callable=run_download_blogs,
    dag=dag,
)

download_report = PythonOperator(
    task_id="download_report",
    python_callable=run_download_reports,
    dag=dag,
)

run_glue_job = GlueJobOperator(
    task_id="run_glue_job",
    job_name=GLUE_JOB_NAME,
    aws_conn_id="aws_default",
    script_location=f"s3://{S3_BUCKET}/script/test_glue.py",
    script_args={
        "--s3_input_path": f"s3://{S3_BUCKET}/raw/",
        "--s3_output_path": f"s3://{S3_BUCKET}/curada/",
    },
    dag=dag,
)

redshift_source = RedshiftDataOperator(
    task_id="load_data_to_redshift",
    database= DATABASE_NAME ,
    workgroup_name="default-workgroup", 
    sql=f"""
        COPY {REDSHIFT_TABLE_SOURCE}
        FROM 's3://{S3_BUCKET}/curada/site/load_date={fecha_actual}/'
        IAM_ROLE '{IAM_ROLE_ARN}'
        FORMAT AS PARQUET;
    """,
    aws_conn_id="aws_default",
    dag=dag,
)

redshift_topic = RedshiftDataOperator(
    task_id="load_data_to_redshift",
    database= DATABASE_NAME ,
    workgroup_name="default-workgroup", 
    sql=f"""
        COPY {REDSHIFT_TABLE_TOPIC}
        FROM 's3://{S3_BUCKET}/curada/topic/load_date={fecha_actual}/'
        IAM_ROLE '{IAM_ROLE_ARN}'
        FORMAT AS PARQUET;
    """,
    aws_conn_id="aws_default",
    dag=dag,
)
redshift_fact = RedshiftDataOperator(
    task_id="load_data_to_redshift",
    database= DATABASE_NAME ,
    workgroup_name="default-workgroup", 
    sql=f"""
        COPY {REDSHIFT_TABLE_FACT}
        FROM 's3://{S3_BUCKET}/curada/fact/load_date={fecha_actual}/'
        IAM_ROLE '{IAM_ROLE_ARN}'
        FORMAT AS PARQUET;
    """,
    aws_conn_id="aws_default",
    dag=dag,
)

[download_article, download_blog, download_report] >> run_glue_job >> [redshift_source, redshift_topic, redshift_fact]

