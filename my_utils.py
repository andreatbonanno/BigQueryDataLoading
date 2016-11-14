import json
import time
from datetime import timedelta, date
from pprint import pprint

# return a generator for all dates in the range start_date - end_date
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

# utility converting the inline csv schema (exportable from bq) into json
def inline_csv_to_json_schema(schema):
    schema = schema.replace(",", ',\r')
    schema = schema.replace(":", ',"type":"')
    schema = schema.replace(",\r", '\r{"name":"')
    schema = schema.replace(",", '",')
    schema = schema.replace("\r", '"},')
    return "[{\"name\":\"" + schema + "\"}]"

# set the job body and submit the job request to bq
def submit_bq_load_job(table, date_str, service, project_id, dataset_id):
    job_body = {
      "kind": "bigquery#job",
      "configuration": {
        "load": {
          "sourceUris": [
            "gs://<BUCKET_NAME>/" + table['category'] + "/" + table['name'] + "/" + table['name'] + ('' if date_str is None else ('_' + date_str)) + "__000.gz"
          ],
          "destinationTable": {
            "projectId": project_id,
            "datasetId": dataset_id,
            "tableId": table['category'] + "_" + table['name'] + ('' if date_str is None else ('_' + date_str))
          },
          "sourceFormat": 'CSV',
          "writeDisposition": 'WRITE_TRUNCATE',
          "fieldDelimiter": table['delimiter'],
          "quote": '',
          "schema": {
            "fields": json.loads(inline_csv_to_json_schema(open(table['file'], 'r').read()))
          }   
            }
      }
    }

    request = service.jobs().insert(projectId=project_id, body=job_body)
    
    # get the response dictionary
    response = request.execute()
    
    # print response status for debugging
    pprint(response['status'])
