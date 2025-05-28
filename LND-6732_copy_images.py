import os
import traceback

import boto3
from botocore.config import Config
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
from concurrent.futures import TimeoutError
from pebble import ProcessPool, ProcessExpired

from typing import List, Dict, Any, Generator
from io import BytesIO
from PyPDF2 import PdfReader


def create_alchemy_engine():
    """
    Create a SQLAlchemy engine.
    """
    try:
        connection_string = (
            f"mssql+pyodbc://{cstitle_username}:{cstitle_password}@{cstitle_server}/countyScansTitle?driver=ODBC+Driver+17+for+SQL+Server")  # Ensure the driver is installed

        # connection_string = (
        #     "mssql+pyodbc://@{cstitle_server}/countyScansTitle?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes"        )
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        print(f"Error creating engine: {e}")
        raise

def create_leaseid_df():
    # Create the SQLAlchemy engine
    engine = create_alchemy_engine()

    # TODO Change the query so it gathers from the DEST table if they don't exist in the new dest
    print(f"Start: Gathering LeaseIDs {datetime.now()}")
    query = ("""SELECT * 
                FROM countyScansTitle.dbo.LND_6732_DEST_20250506 dest WITH(NOLOCK)
                WHERE NOT EXISTS (SELECT 1
                                  FROM countyScansTitle.dbo.LND_6732_20250506 src WITH(NOLOCK)
                                  WHERE dest.recordID = src.recordID)
                ORDER BY leaseid DESC;""")

    df = pd.read_sql(query, engine)
    # print(f"df.head():\n\n {df.head()}")
    print(f"len(df): {len(df)}")
    print(f"Complete: Gathering LeaseIDs {datetime.now()}")

    return df

# TODO Copy image from source_path to destination_path
# TODO Insert record into tblS3Image
def process_batch(batch):
    config = Config(
        retries={
            'max_attempts': 10,  # Maximum number of retry attempts
            'mode': 'standard'  # Standard mode includes exponential backoff
        }
    )
    s3_client = boto3.client('s3', region_name='us-east-1', config=config)

    return_list = list()
    start_time = datetime.now()

    counter = 0
    for row in batch:
        return_dict = dict()
        counter += 1

        record_id = row['recordid']
        source_path = row['source_path']
        try:
            # Source bucket and object
            source_key = row['source_path']
            source_bucket = 'di-diml-platinum-prod'

            # Destination bucket and object
            destination_bucket = 'enverus-courthouse-prod-chd-plants'
            destination_key = row['state_countyname'] + '/' + record_id[0:4].lower() + '/' + record_id.lower() + os.path.splitext(source_path)[1]

            # Copy the object
            copy_source = {
                'Bucket': source_bucket,
                'Key': source_key
            }

            return_dict['recordID'] = row['recordid']
            return_dict['s3FilePath'] = 's3://' + destination_bucket + '/' + row['state_countyname'] + '/' + record_id[0:4].lower() + '/' + record_id.lower() + os.path.splitext(source_path)[1]
            return_dict['pageCount'] = row['page_count']
            return_dict['fileSizeBytes'] = row['file_size']
            return_dict['_ModifiedDateTime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            return_dict['_ModifiedBy'] = 'LND-6732'
            return_dict['status'] = 'copied'

            s3_client.copy(copy_source, destination_bucket, destination_key)
            # print(f"File copied from {source_bucket}/{source_key} to {destination_bucket}/{destination_key}")

        except Exception as e:
            print(f"recordID: {row['recordid']}; Error: {e}")
            print(f"Traceback: {traceback.format_exc()}")
            return_dict['status'] = 'error'

        return_list.append(return_dict)

        if str(counter)[-3:] == '000':
            end_time = datetime.now()
            elapsed = end_time - start_time
            print(f"end_time: {end_time}")
            print(f'elapsed: {elapsed}')
            print(f"Processed: {counter} rows")

    return return_list


def create_batches_from_dataframe(
        df: pd.DataFrame,
        batch_size: int = 1000
) -> Generator[List[Dict[str, Any]], None, None]:
    """
    Convert a pandas DataFrame to dictionary format and yield batches of specified size.

    Args:
        df: The pandas DataFrame to process
        batch_size: Number of rows per batch (default: 10000)

    Yields:
        Batches of records in dictionary format
    """
    total_rows = len(df)

    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        batch_df = df.iloc[start_idx:end_idx]
        # Convert the batch to a list of dictionaries
        batch_records = batch_df.to_dict('records')
        yield batch_records



def map_images(df_lease_ids, max_workers=7, max_timeout=None):
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
    start_time = datetime.now()
    rolling_start_time = datetime.now()

    counter = 0
    batch_list = list()

    for batch in create_batches_from_dataframe(df_lease_ids):
        batch_list.append(batch)

    with ProcessPool(max_workers=max_workers) as pool:
         future = pool.map(process_batch, batch_list, timeout=max_timeout)
         iterator = future.result()

         while True:
             try:
                 counter += 1

                 result = next(iterator)
                 df_results = pd.DataFrame(result)
                 df_results.to_sql('LND_6732_20250506', create_alchemy_engine(), if_exists='append', index=False)

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


def copy_table(table_name=None):
    cstitle_username = os.getenv('cstitle_username')
    cstitle_password = os.getenv('cstitle_password')
    cstitle_dev_server = os.getenv('cstitle_dev_server')
    cstitle_prod_server = os.getenv('cstitle_prod_server')

    source_connection_string = f"mssql+pyodbc://{cstitle_username}:{cstitle_password}@{cstitle_dev_server}/countyScansTitle?driver=ODBC+Driver+17+for+SQL+Server"
    target_connection_string = f"mssql+pyodbc://{cstitle_username}:{cstitle_password}@{cstitle_prod_server}/countyScansTitle?driver=ODBC+Driver+17+for+SQL+Server"

    # Create engines for source and target databases
    source_engine = create_engine(source_connection_string)
    target_engine = create_engine(target_connection_string)

    try:
        # Read the table from the source database into a DataFrame
        with source_engine.connect() as source_conn:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, source_conn)
            print(f"Read {len(df)} rows from table '{table_name}' in the source database.")

        # Write the DataFrame to the target database
        with target_engine.connect() as target_conn:
            df.to_sql(table_name, con=target_conn, if_exists='replace', index=False)
            print(f"Successfully wrote {len(df)} rows to table '{table_name}' in the target database.")
    except Exception as e:
        print(f"An error occurred: {e}")


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
        # print("Start: Copying Images To S3")
        # df_lease_ids = create_leaseid_df()
        # map_images(df_lease_ids, max_workers=8)
        # print("Complete: Copying Images To S3")

        print("Start: Copy [countyScansTitle].[dbo].[LND_6732_tblS3Image_20250506] From Dev -> Prod")
        copy_table(table_name='LND_6732_tblS3Image_20250506')
        print("Complete: Copy [countyScansTitle].[dbo].[LND_6732_tblS3Image_20250506] From Dev -> Prod")
    except Exception as e:
        print(f"Error: {e}")
        print(f"Traceback: {traceback.format_exc()}")
        raise e
