#!/usr/bin/env python
# coding: utf-8
import os
import pandas as pd
import argparse
from time import time
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    file_name = "output.parquet"

    # download file - wget not working
    os.system(f"wget {url} -O {file_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # read file
    df = pd.read_parquet(file_name)

    n = 100000
    list_df = [df[i:i + n] for i in range(0, len(df), n)]

    for _df in list_df: # append "n / len(df)" chunks of data
        t_start = time()

        _df.to_sql(name=table_name, con=engine, if_exists="append")

        t_end = time()

        print("Inserted another chunk, took %.3f seconds" % (t_end - t_start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                    description='Ingest parquet date to Postgres')


# user, password, host, port, databse name, table name
# url of the file
    parser.add_argument('--user', help="user name for postgres")
    parser.add_argument('--password', help="password name for postgres")
    parser.add_argument('--host', help="host for postgres")
    parser.add_argument('--port', help="port for postgres")
    parser.add_argument('--db', help="database name for postgres")
    parser.add_argument('--table_name', help="name of the table where we will write the results")
    parser.add_argument('--url', help="url of the parquet file")

    args = parser.parse_args()
    main(args)
