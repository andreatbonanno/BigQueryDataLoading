# define properties of tables to be loaded
tables = [
    {
        "name" : "table1",
        "file": "table1_schema.txt",
        "category": "A",
        "partitioned": True,
        "delimiter": '\t'
    },
    {
        "name" : "table2",
        "file": "table2_schema.txt",
        "category": "A",
        "partitioned": False,
        "delimiter": '\t'
    },
    {
        "name" : "table3",
        "file": "table3_schema.txt",
        "category": "B",
        "partitioned": True,
        "delimiter": '|'
    },
    {
        "name" : "bq_export",
        "file": "BQ_Export_schema.txt",
        "category": "C",
        "partitioned": True,
        "delimiter": '|'
    }
]



"""
BEFORE RUNNING:
---------------
1. If not already done, enable the BigQuery API
   and check the quota for your project at
   https://console.developers.google.com/apis/api/bigquery
2. This sample uses Application Default Credentials for authentication.
   If not already done, install the gcloud CLI from
   https://cloud.google.com/sdk/ and run
   'gcloud beta auth application-default login'
3. Install the Python client library for Google APIs by running
   'pip install --upgrade google-api-python-client'
"""

from datetime import date
from my_utils import daterange, submit_bq_load_job

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

# Authentication is provided by the 'gcloud' tool when running locally
credentials = GoogleCredentials.get_application_default()

# Construct the bigquery service object (version v2) for interacting
service = discovery.build('bigquery', 'v2', credentials=credentials)

# ID of the project that will be billed for the job
project_id = '<PROJECT_ID>'

# ID of the dataset to load the data in
dataset_id = '<DATASET_ID>'

# define date ranges (Sep. 2016)
start_date = date(2016, 9, 1)
end_date = date(2016, 10, 1)

# load table data
for t in tables:
    # if table is partitioned by day, load daily data separately
    if t['partitioned']:
        for single_date in daterange(start_date, end_date):
            submit_bq_load_job(t, single_date.strftime("%Y%m%d"), service, project_id, dataset_id)
    else:
        submit_bq_load_job(t, None, service, project_id, dataset_id)