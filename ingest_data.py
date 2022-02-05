from unicodedata import name
import pandas as pd



#create a postgres connection
from sqlalchemy import create_engine
from time import time

import argparse
import os


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db=params.db
    table_name=params.table_name
    url=params.url

    # download csv
    csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df_iter = pd.read_csv(csv_name,iterator=True,chunksize=100000)
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(0).to_sql(name=table_name,con=engine,if_exists='replace')
    count = 0
    while True:
        t_start = time()
        if count > 0:
            df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name,con=engine,if_exists='append')
        t_end = time()
        print("inserted another chunk... took %3.f second" %(t_end-t_start))
        count+=1

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingesting data from a csv file to Postgres')
    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='host for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where the results will be')
    parser.add_argument('--url', help='url of the csv file')


    args = parser.parse_args()
    main(args)


