from airflow import models
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from google.cloud import bigquery
from google.oauth2 import service_account

default_args = {
    'owner' : 'sandi',
}

PROJECT_ID = Variable.get('PROJECT_ID')
EVENT_TABLE = Variable.get('EVENT_TABLE')
SOURCE_PROJECT_ID = Variable.get('SOURCE_PROJECT_ID')
SOURCE_CREDENTIAL = Variable.get('SOURCE_CREDENTIAL')

credentials = service_account.Credentials.from_service_account.fila(SOURCE_CREDENTIAL)
client = bigquery.Client
(credentials = credentials, project=SOURCE_PROJECT_ID)

with models.DAG(
    'bs-week2-event_table',
    default_args = default_args,
    schedule_interval = '0 0 */3 * *',
    start_date = days_ago(1),
) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')


    bigquery_create_events_task = BigQueryExecuteQueryOperator(
        task_id ='exe_query_event_table',
        sql='''
        SELECT
            MAX(if(param.key = "product_id", param.value.int_value, NULL)) AS product_id,
            MAX(if(param.key = "product_name", param.value.string_value, NULL)) AS product_name,
            MAX(if(param.key = "product_price", param.value.float_value, NULL)) AS product_price,
            MAX(if(param.key = "product_sku", param.value.int_value, NULL)) AS product_sku,
            MAX(if(param.key = "product_version", param.value.string_value, NULL)) AS product_version,
            MAX(if(param.key = "product_type", param.value.string_value, NULL)) AS product_type,
            MAX(if(param.key = "product_division", param.value.string_value, NULL)) AS product_division,
            MAX(if(param.key = "product_group", param.value.string_value, NULL)) AS product_group,
            MAX(if(param.key = "product_department", param.value.string_value, NULL)) AS product_department,
            MAX(if(param.key = "product_class", param.value.string_value, NULL)) AS product_class,
            MAX(if(param.key = "product_subclass", param.value.string_value, NULL)) AS product_subclass,
            MAX(if(param.key = "product_is_book", param.value.bool_value, NULL)) AS product_is_book,
            user_id,
            state,
            city,
            created_at,
            event_id,
            MAX(if(param.key = "event_name", param.value.bool_value, NULL)) AS event_name,
            MAX(if(param.key = "event_datetime", param.value.bool_value, NULL)) AS date_time
        FROM `academi-315200.unified_events.event`,
        UNNEST (event_params) as param
        GROUP BY
            user_id,
            state,
            city,
            created_at,
            event_id
            ''',
            write_disposition='WRITE APPEND',
            create_disposition='CREATE_IF_NEEDED',
            destination_dataset_table = EVENT_TABLE,
            location='US',
            use_legacy_sql = False,
            gcp_conn_id='google_cloud_default'
            )

start >> bigquery_create_events_task >> end
