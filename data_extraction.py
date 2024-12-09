import pandas as pd
import tabula
import requests
import boto3
import json

class DataExtractor:
    def __init__(self):
        self.number_of_stores = None
        self.headers = None
    
    def read_rds_table(self, connector, table_name):
        engine = connector.init_db_engine()
        query = f"SELECT * FROM {table_name}"
        data = pd.read_sql(query, engine)
        return data
    
    def retrieve_pdf_data(self, pdf_link):
        dfs = tabula.read_pdf(pdf_link, pages='all')
        return pd.concat(dfs, ignore_index=True)

    def list_number_of_stores(self, endpoint, headers):
        response = requests.get(endpoint, headers=headers)
        if response.status_code == 200:
            data = response.json()
            self.headers = headers
            self.number_of_stores = data.get("number_stores")
            return self.number_of_stores
        else:
            raise Exception(f"Failed to retrieve data: {response.status_code}")
        
    def retrieve_stores_data(self, store_endpoint):
        if not self.number_of_stores or not self.headers:
            print("Please use list_number_of_stores first")
            return
                
        stores_data = []
        for store_number in range(self.number_of_stores):
            response = requests.get(store_endpoint.format(store_number=store_number), headers=self.headers)
            if response.status_code == 200:
                store_data = response.json()
                stores_data.append(store_data)
            else:
                print(f"Failed to retrieve data for store {store_number}: {response.status_code}")
        
        return pd.DataFrame(stores_data)
    
    def extract_from_s3(self, s3_address):
        s3 = boto3.client('s3')
        bucket_name = s3_address.split('/')[2]
        file_key = '/'.join(s3_address.split('/')[3:])
        file_extension = file_key.split('.')[-1].lower()

        if file_extension == 'csv':
            s3.download_file(bucket_name, file_key, "temp_file.csv")
            return pd.read_csv("temp_file.csv")
        elif file_extension == 'json':
            response = s3.get_object(Bucket=bucket_name, Key=file_key)
            data = json.loads(response['Body'].read().decode('utf-8'))
            return pd.DataFrame(data)
        else:
            raise ValueError(f"Unsupported file type: {file_extension}")

