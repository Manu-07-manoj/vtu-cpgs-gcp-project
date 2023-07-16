from google.cloud import monitoring_v3
import pandas as pd
import datetime
from google.cloud import bigquery

import os



credsfilename = 'C:\\Manu\\code\\Python\\firstproject\\GCP-project\\credentials\\bold-upgrade-385913-a30364831b36.json'

def authenticate_bigquery(credsfilename):
    print('authenticating....')
    bq_client = bigquery.Client.from_service_account_json(credsfilename)
    SERVICE_ACCOUNT_JSON = os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credsfilename
    metric_client = monitoring_v3.MetricServiceClient.from_service_account_json(SERVICE_ACCOUNT_JSON)

    return metric_client, bq_client

def average():
    aggregation = monitoring_v3.Aggregation({
        "alignment_period": {'seconds':3600},
        'per_series_aligner': monitoring_v3.Aggregation.Aligner.ALIGN_MEAN
    })

    return aggregation



def compute_engine_metrics(projectid,client, aggregation,startyearname, endyearname,startmonth,endmonth,
                           lastday,firstday, yearmonth, dateextention, storage_local, metric_bucket,
                           before_yearmonht, afteryearmonth, agg):
    project_name = f'projects/bold-upgrade-385913'

    print('-----------------------')
    print(endyearname, endmonth, lastday)
    print(startyearname, startmonth, firstday)


    timeinterval = monitoring_v3.TimeInterval({
        'end_time': datetime.datetime(int(endyearname), int(endmonth), int(lastday)),
        'start_time': datetime.datetime(int(startyearname), int(startmonth), int(firstday))
    })

    metric_names = ["memory_utilization"]

    metrics = ['metric.type = "compute.googleapis.com/instance/memory/balloon/ram_used"']

    for i in range(len(metrics)):
        results = client.list_time_series(
            request = {
                'name' : f"{project_name}",
                "filter": metrics[i],
                'interval': timeinterval,
                'view': monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                'aggregation': aggregation
            }
        )

        df = pd.DataFrame(results)
        df_concat = pd.DataFrame()

        for result in results:

            for  j in range(len(result.points)):
                df = pd.DataFrame({
                    'metric' : metric_names[i],
                    'metric_type': [result.metric.type],
                    'metric_instance_name': [result.metric.labels['instance_name']],
                    'resource_type': [result.resource.type],
                    'resource_zone': [result.resource.labels['zone']],
                    'resource_project_id':[result.resource.labels['project_id']],
                    'resource_instace_id':str([result.resource.labels['instance_id']]),
                    'start_time': [result.points[j].interval.start_time],
                    'end_time': [result.points[j].interval.end_time],
                    'usage': [result.points[j].value.double_value],
                    'yearmonht': yearmonth

                     

                })

                df_concat = pd.concat([df_concat, df])

        if (len(df_concat)> 0):
            df_concat.to_csv('C:/Manu/code/Python/firstproject/GCP-project/files/mem_metrics.csv')


yearmonth = '202305'
fromdate = '2023-05-01'
todate = '2023-05-09'
storage_client='' 
bucketname='' 
before_yearmonht ='' 
afteryearmonth = ''

year = yearmonth[:4]
month = (yearmonth)[4:6]
firstday = ((fromdate.split('-'))[-1])
startmonth = (fromdate.split('-'))[1]
startyearname = (fromdate.split('-'))[0]
lastday = (todate.split('-'))[-1]
endmonth = (todate.split('-'))[1]
endyearname = (todate.split('-'))[0]
dateextention = todate

print(
year,
month,
firstday,
startmonth,
startyearname,
lastday ,
endmonth ,
endyearname ,
dateextention
)

projectids = ['bold-upgrade-385913', 'modified-argon-385707']

client,bqclient = authenticate_bigquery(credsfilename)

for projectid in projectids :
    compute_engine_metrics(projectid, client, average(), startyearname, endyearname, startmonth, endmonth, lastday,
                       firstday, yearmonth, dateextention, storage_client, bucketname, before_yearmonht, afteryearmonth, agg="avg")