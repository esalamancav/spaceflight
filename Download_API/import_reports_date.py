import requests
import pandas as pd
from io import StringIO
import boto3 
from datetime import datetime, timedelta
import json
import time 
import os
import sys

class ApiRequest:
    def __init__(self): 
        self.session = None
    def ApiRest(url, ses):
        response = ses.get(url)
        return response  
        
class Endpoints:
    def Article(session, date_load, limit):
        url = url_server + 'v4/articles/?limit=' + str(limit) + '&published_at_gt=' + str(date_load)
        response = ApiRequest.ApiRest(url, session)
        print(url)
        return response
        
    def Blog(session, date_load, limit):
        url = url_server + '/v4/blogs/?limit=' + str(limit) + '&published_at_gt=' + str(date_load)
        response = ApiRequest.ApiRest(url, session)
        return response

    def Reports(session, date_load, limit):
        url = url_server + '/v4/reports/?limit=' + str(limit) + '&published_at_gt=' + str(date_load)
        response = ApiRequest.ApiRest(url, session)
        return response
    def Info(session, date_load):
        url = url_server + '/v4/info/'
        response = ApiRequest.ApiRest(url, session)
        return response 
class Process:      
    def get_id():
        max_article_id = 28900
        return max_article_id
    
    def get_data(date):
        #headers = {'Content-Type': 'application/json'}
        session = requests.Session()
        data_load = []
        get_article = Endpoints.Reports(session, date, limit)
        if get_article.status_code == 200:
            project_json = json.loads(get_article.text) 
            for item in project_json['results']:
                if "summary" in item:
                    item["summary"] = item["summary"].replace("\n", " ").replace("\r", " ")
            json_str = json.dumps(project_json['results'])
            df_json = pd.read_json(json_str, lines=False)
        else:
            print('Error: '+ get_article.status_code + ' al momento de consultar los articulos')
        df_json[["featured", "launches", "events"]] = pd.NA
        df = df_json[["id", "title", "url", "image_url", "news_site", "summary", "published_at", "updated_at", "featured", "launches", "events"]]
        df["published_at"] = pd.to_datetime(df["published_at"])
        df["updated_at"] = pd.to_datetime(df["updated_at"])
        df["featured"] = df["featured"].astype("boolean")
        return df

    
    def load_s3(df, bucket_name, object_name):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)      

        s3_client = boto3.client('s3')
        try:
            s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=csv_buffer.getvalue())
            print(f"DataFrame cargado exitosamente en S3: {bucket_name}/{object_name}")
        except Exception as e:
            print(f"Error al cargar el DataFrame: {e}")

if __name__ == '__main__':

    fecha_actual = datetime.now().strftime('%Y%m%d')
    fecha_anterior = datetime.now() - timedelta(days=1)
    fecha_anterior = '2024-01-01 00:00:00'
    limit = 510
    url_server = 'https://api.spaceflightnewsapi.net/'
    ruta = '/shared/RTD/codigo/json/'
    bucket_name = "spaceflight"
    object_name = "raw/Report_" + fecha_actual + ".csv" 

    #max_id = Process.get_id()
    df_api = Process.get_data(fecha_anterior)
    Process.load_s3(df_api, bucket_name, object_name )
    #df_api.to_csv(f"C:/Users/esala/Drive_Esteban/Esteban/Inetum/reportes.csv", index=False)
    #print(df_api.dtypes)



    

    #print(df_data)