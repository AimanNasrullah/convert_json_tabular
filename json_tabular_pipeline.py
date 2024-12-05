from sqlalchemy import create_engine, text
import os
import threading
import json
import pandas as pd
import dask.dataframe as dd


# absolute path from json
json_dir = os.path.abspath("json_files")

# Global var to control the monitoring thread
pipeline_running = True
files_processed_this_minute = 0
files_processed_lock = threading.Lock()

def db_connection():
    return create_engine("postgresql+psycopg2://postgres:password@localhost:5432/jsonTestData")

def test_db_connection(engine):
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Database connection successful")
        return True
    except Exception as e:
        print(f"Logs: Database connection failed: {e}")
        return False

def process_json_files(file_path):
    try:
        pass
        # print(f"Processing {file_path}")

        # with open(file_path, "r") as file:
        #     data = json.load(file)
        
        # # convert JSON to Dask Dataframe and partition it
        # ddf = dd.from_pandas(pd.Dataf)
        
    except:
        pass

if __name__ == "__main__":
    engine = db_connection()

    if not test_db_connection(engine):
        print("Logs: Exiting due to database connection failure")
    else:
        print("All Good!")
