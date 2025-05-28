import os
import csv
import glob
import traceback

import boto3
from botocore.config import Config
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
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




if __name__ == "__main__":
    # Load environment variables from the .env file
    load_dotenv()

    environ_var = 'dev'
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
        print("Start: Processing Brief Legals CSV")
        engine = create_alchemy_engine()
        csv_files = glob.glob(r'C:\tmp\briefLegals\*.csv')

        dataframes = []

        for csv_file in csv_files:
            print(f'Reading: {csv_file}')
            df_ld = pd.read_csv(csv_file, sep=',', doublequote=False, escapechar='\\')
            dataframes.append(df_ld)
            print(f"Loaded CSV: {csv_file}")

        # Concatenate all DataFrames into a single DataFrame
        df_land_descriptions = pd.concat(dataframes, ignore_index=True)
        df_land_descriptions.fillna('', inplace=True)

        # Print the combined DataFrame
        # print(f'df_land_descriptions.head():\n{df_land_descriptions.head()}')
        #
        # # Create table with Dummy Data, so there will be a landDescriptionId to join back on
        # # Insert records into LND_6838_tblLandDescription
        # # Load the DataFrame into a temporary table
        print(f"Start: Creating LND_6838_temp_table")
        df_land_descriptions.to_sql('LND_6838_temp_table', con=engine, if_exists='replace', index=False)
        print(f"Complete: Creating LND_6838_temp_table")

        print("Start: Updating LND_6838_tbllandDescription_20250519 From LND_6838_temp_table")
        # Perform a bulk update
        with engine.begin() as connection:
            connection.execute(text("""
            UPDATE target_table
            SET 
                target_table.Survey = source_table.Survey,
                target_table.Subdivision = source_table.Subdivision,
                target_table.Lot = source_table.Lot,
                target_table.Block = source_table.Block,
                target_table.Section = source_table.Section,
                target_table.Township = source_table.Township,
                target_table.RangeOrBlock = source_table.Range,
                target_table.Quartercalls = source_table.Quartercall,
                target_table.NewCityBlock = source_table.NewCityBlock
            FROM countyScansTitle.dbo.LND_6838_tbllandDescription_20250519 target_table
            JOIN countyScansTitle.dbo.LND_6838_temp_table source_table
                ON target_table.landDescriptionID = source_table.landDescriptionID;            
            """))
            connection.commit()

        with engine.begin() as connection:
            # Remove the temporary table
            connection.execute(text("""DROP TABLE countyScansTitle.dbo.LND_6838_temp_table"""))
            connection.commit()
        print("Complete: Updating LND_6838_tbllandDescription_20250519 From LND_6838_temp_table")

        print("Complete: Processing Brief Legals CSV")
    except Exception as e:
        print(f"Error: {e}")
        print(f"Traceback: {traceback.format_exc()}")
        raise e

# TODO Copy table to Prod Since Its a Receipt Table