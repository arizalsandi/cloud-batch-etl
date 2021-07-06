from datetime import datetime, timedelta
from airflow import models
from airflow.models import Variable
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

BUCKET_PATH = models.Variable.get("BUCKET_PATH")
BUCKET_INPUT = models.Variable.get("BUCKET_INPUT")
PROJECT_ID = models.Variable.get("PROJECT_ID")
OUTPUT_FULL_TABLE_BQ = models.Variable.get("ALL_KEYWORDS_BQ_OUTPUT_TABLE")
OUTPUT_TOP_TABLE_BQ = models.Variable.get("TOP_KEYWORDS_BQ_OUTPUT_TABLE")


default_args = {
    "start_date": datetime(2021,3,10),
    "end_date"  : datetime(2021,3,15), 
    "dataflow_default_options": {
        "project": PROJECT_ID, 
        "temp_location": BUCKET_PATH + "/tmp/",
        "numWorkers": 1,
    },
}

with models.DAG(
    "bs-week2-search-most-keyword",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
) as dag:
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    storage_to_bigquery = DataflowTemplateOperator(
        task_id="storage_to_bigquery",
        template="gs://dataflow-templates/latest/GCS_Text_to_BigQuery",
        parameters={
            "javascriptTextTransformFunctionName": "transformCSVtoJSON",
            "JSONPath" : BUCKET_PATH + "/jsonSchema.json",
            "javascriptTextTransformGcsPath": BUCKET_PATH +"/transformCSVtoJSON.js",
            "inputFilePattern": BUCKET_INPUT + "/keyword_search_search_{{ ds_nodash }}.csv",
            "outputTable" : OUTPUT_FULL_TABLE_BQ,
            "bigQueryLoadingTemporaryDirectory" : BUCKET_PATH + "/tmp/",
        },
    )

    querying_daily_top_search = BigQueryOperator(
        task_id = "querying_daily_top_search",
        sql =  """SELECT lower(search_keyword) as keyword, count(lower(search_keyword)) as search_count, created_at as date
            FROM `striking-goal-316805.bs_week2.search_keyword`
            where created_at = '{{ds}}'
            GROUP BY keyword, created_at
            ORDER BY search_count DESC
            LIMIT 1;""",
        use_legacy_sql = False,
        destination_dataset_table = OUTPUT_TOP_TABLE_BQ,
        write_disposition = 'WRITE_APPEND'
    )


start >> storage_to_bigquery >> querying_daily_top_search >> end