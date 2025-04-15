import os
import traceback

import boto3
import pyodbc
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch
from concurrent.futures import TimeoutError
from pebble import ProcessPool, ProcessExpired


# Create a process that gathers the required data to populate the storageFilePath + sourceFilePath
# Use that csv to copy the files to the new S3 bucket + populate the storageFilePath + sourceFilePath


# TODO Use query to gather package_id, query

# Need a query to gather all records from countyScansTitle.dbo.LND_6732_20250414

def create_alchemy_engine():
    """
    Create a SQLAlchemy engine.
    """
    try:
        connection_string = (
            f"mssql+pyodbc://{cstitle_username}:{cstitle_password}@{cstitle_server}/countyScansTitle?driver=ODBC+Driver+17+for+SQL+Server")  # Ensure the driver is installed
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        print(f"Error creating engine: {e}")
        raise

def create_leaseid_df():

    # Create the SQLAlchemy engine
    engine = create_alchemy_engine()

    query = ("SELECT * FROM countyScansTitle.dbo.LND_6732_20250414 "
             "WHERE source_path IS NULL or source_path = '' "
             "ORDER BY leaseid DESC")

    df = pd.read_sql(query, engine)
    # print(f"df.head():\n\n {df.head()}")

    return df

def process_row(row):
    s3 = boto3.client('s3', region_name='us-east-1')

    response = s3.list_objects_v2(Bucket='di-diml-platinum-prod', Prefix=row['package_id'])
    contents = response['Contents']
    row['source_path'] = contents[0]['Key']

    row['destination_path'] = ('enverus-courthouse-prod-chd-plants' + '/' + row['state_countyname']
                        + '/' + row['recordid'][0:4].lower() + '/' + row['recordid'].lower()
                        + os.path.splitext(row['source_path'])[1])

    return row


# TODO Make sure to populate the S3storagePath table talk with Ty
def query_s3(df_lease_ids, max_workers=32, max_timeout=90):
    # TODO Need to gather the dataset ID value by querying the S3 bucket, follow the same pattern to gather the
    """
    Process the dataframe using Pebble for multiprocessing.

    Args:
        df: pandas DataFrame to process
        max_workers: number of worker processes
        timeout: timeout in seconds for each row processing

    Returns:
        Updated DataFrame with processing results
    """
    df_results = pd.DataFrame()
    start_time = datetime.now()
    rolling_start_time = datetime.now()

    counter = 0
    with ProcessPool(max_workers=max_workers) as pool:
        future = pool.map(process_row, df_lease_ids.to_dict(orient='records'), timeout=max_timeout)
        iterator = future.result()

        while True:
            try:
                counter += 1

                result_dict = next(iterator)
                df_results = pd.concat([df_results, pd.DataFrame([result_dict])], ignore_index=True)

                if str(counter)[-3:] == '000':
                    end_time = datetime.now()
                    elapsed = end_time - rolling_start_time
                    rolling_start_time = datetime.now()
                    print("Last Thousand Files Processed In: {}".format(str(elapsed)[:-3]))

            except StopIteration:
                break
            except TimeoutError as error:
                print("Reading File Took Longer Than {} Seconds".format(error.args[1]))
            except ProcessExpired as error:
                print("{}. Exit code: {}".format(error, error.exitcode))
            except Exception:
                message = "An error occurred: {error} ".format(error=traceback.print_exc())
                print(message)

    end_time = datetime.now()
    elapsed = end_time - start_time
    print("Files were processed in (hh:mm:ss.ms) {}".format(str(elapsed)[:-3]))

    df_results.to_sql('LND_6732_20250414', create_alchemy_engine(), if_exists='append', index=False)


if __name__ == "__main__":
    # Load environment variables from the .env file
    load_dotenv()

    environ_var = 'dev'
    es_api = os.getenv('es_api')
    pd.set_option('display.max_columns', None)
    pd.set_option('display.expand_frame_repr', False)

    cstitle_username = os.getenv('cstitle_username')
    cstitle_password = os.getenv('cstitle_password')
    if environ_var.lower() == 'dev':
        cstitle_server = os.getenv('cstitle_dev_server')
    elif environ_var.lower() == 'prod':
        cstitle_server = os.getenv('cstitle_prod_server')

    print(f"cstitle_server: {cstitle_server}")
    print(f"cstitle_username: {cstitle_username}")
    print(f"cstitle_password: {cstitle_password}")

    try:
        df_lease_ids = create_leaseid_df()

        query_s3(df_lease_ids, max_workers=8)
    except Exception as e:
        print(f"Error: {e}")
        print(f"Traceback: {traceback.format_exc()}")
        raise e
