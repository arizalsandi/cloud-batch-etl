# ETL BY CLOUD COMPOSER

Perform batch ETL using cloud composer by Google Cloud.

## Data Sources
1. CSV who stored in Google Cloud Storage
2. Bigquery Table

## Prerequisite
1. Python 3.6.9 ( or more )
2. Google Cloud Platform
   - Composer
   - Google Cloud Storage
   - BigQuery
   - Dataflow

## Setup

1. Enable related API ( Cloud Composer, Cloud Dataflow, etc ) on your GCP
2. Create service account for your own access to GCP environment
3. Create a new bucket in Google Cloud Storage, so you can put your dependencies resources 
4. Go to Big Data > Composer,  to create new Composer Environtment.
  - Name : {Depends on yours, here i used blankspace-cloud-composer as my composers name}
  - Location: us-central1
  - Node count: 3
  - Zone: us-central1a
  - Machine Type: n1-standard-1
  - Disk size (GB): 20 GB
  - Image Version: composer-1.17.0-preview.1-airflow-2.0.1
  - Python Version: 3
 5. Go to Airflow UI through Composer were you created, choose **Admin** **>** **Variables** to import your `variable.json` which containing value and key like in this repo `data/variables.json`. Done forget to use your 
6. Create table in Big Query same as on what your task need
7. To Run your DAG, just put your file in `dags/`, then your DAG will appear if there's no issue with your code

## Output
Our first task, we need serve Big Query results and find the most keyword from your CSV file.
Bellow here, my path progress until the result shown up in Big Query.

First task flow :
1. Put your dependencies files into your Google Cloud Storage ( csv file, json file and js file )
2. Make your code in your local first, then send your code into your `dags/` folder
3. If there are no issue about your code, your DAG will appear on your Airflow UI
4. After success with your DAG, look at your Dataflow as a ETL progress
5. You can see the result which stored in your Big Query

Here's some result of first task:

![week2mostkeyword](https://user-images.githubusercontent.com/84316622/124599119-84860f80-de8f-11eb-94cb-738e358efcde.png)


Our second task, we need to Transform product events from the unified user events bigquery table, into a product table in your bigquery.
Bellow here, my path progress until the result shown up in Big Query.

Second task flow :
1. Get your access into external Big Query Table by asking the Administrator, in my case i got this one {academi-315200:unified_events.event}
2. Put your code into `dags/`
3. If there are no issue about your code, your DAG will appear on your Airflow UI
4. After success with your DAG, the result will stored in your Big Query

Here's some result of second task:

![week2eventtable](https://user-images.githubusercontent.com/84316622/124599990-7a184580-de90-11eb-9fd9-9fef10aaeff4.png)

![week2eventtable2](https://user-images.githubusercontent.com/84316622/124600923-746f2f80-de91-11eb-9b3b-e5b9cfc87bd8.png)







