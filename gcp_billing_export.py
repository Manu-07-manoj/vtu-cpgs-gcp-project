from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file('C:\\Manu\\code\\Python\\firstproject\\GCP-project\\credentials\\bold-upgrade-385913-a30364831b36.json')
client = bigquery.Client(credentials= credentials)

credentials = service_account.Credentials.refresh
# def big_query_to_csv():
query = '''SELECT  * FROM `bold-upgrade-385913.StandardBillDataset.gcp_billing_export_v1_0145E3_1A5780_7D2220` '''
df = client.query(query).to_dataframe() 
df.to_csv('C:/Manu/code/Python/firstproject/GCP-project/files/gcp_billing_export.csv', index=False,header=True)
print("file generated successfully")
print(df)

# big_query_to_csv()