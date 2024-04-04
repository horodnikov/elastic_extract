import json
import pandas as pd
import logging
import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from sqlalchemy import create_engine, text


logging.basicConfig(level=logging.INFO)
load_dotenv()

# Your Elasticsearch host and index name
ELASTIC_HOST= os.environ['ELASTIC_HOST']
ELASTIC_API_KEY=os.environ['ELASTIC_API_KEY']
ELASTIC_INDEX=os.environ['ELASTIC_INDEX']

DRIVER = os.environ['MSSQL_DRIVER']
SERVER = os.environ['MSSQL_SERVER']
DATABASE = os.environ['MSSQL_DATABASE']
USER = os.environ['MSSQL_USER']
PASSWORD = os.environ['MSSQL_PASS']


def transform(x):
    if  isinstance(x, list):
        return ', '.join(x).strip()
    return x

# Initialize the Elasticsearch client with the API key
es = Elasticsearch(
    [ELASTIC_HOST],
    api_key=ELASTIC_API_KEY,
)

if __name__ == "__main__":
  
    with open('data.json') as user_file:
        file_contents = user_file.read()
        parsed_json = json.loads(file_contents)
        
    # Fetch documents
    response = es.search(index=ELASTIC_INDEX, body=parsed_json)
    hits = response['hits']['hits']  # Extracting the documents found
    
    df=pd.json_normalize(hits) 

    modify_cols = [
        'fields.@timestamp',
        'fields.username',
        'fields.geoip.continent_name',
        'fields.geoip.city_name', 
        'fields.external_ip', 
        ]
    
    db_column = [
      'Timestamp',
      'Username',
      'Country',
      'City',
      'IP'
    ]
    

    df = df[modify_cols].applymap(transform)
  
    columns = dict(zip(modify_cols, db_column))
    parsed = df.rename(columns=columns)
    print(parsed)
    conn = 'DRIVER={0};SERVER={1};DATABASE={2};UID={3};PWD={4}'.format(
        DRIVER, SERVER, DATABASE, USER, PASSWORD)

    table = 'STNE_DIM_location_temp_solut'
    
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % conn)
    with engine.connect() as connection:
        try:
              parsed.to_sql(
                  name=table,
                  # schema=SCHEMA,
                  con=connection,
                  index=False,
                  if_exists="append")
        except Exception as exception:
            logging.error(exception)
            