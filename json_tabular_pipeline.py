from sqlalchemy import create_engine, text
import os
import threading
import json
import pandas as pd
import dask.dataframe as dd
from concurrent.futures import ThreadPoolExecutor


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

# Process JSON Files with Dask
def process_json_files(file_path, engine):
    try:
        print(f"Processing {file_path}")

        # Load JSON data
        with open(file_path, "r") as file:
            data = json.load(file)
       

        # Convert JSON to Dask DataFrame and partition it
        ddf = dd.from_pandas(pd.DataFrame(data), npartitions=4)

        # Bulk insert each partition into PostgreSQL
        with engine.connect() as connection:
            for partition in ddf.to_delayed():
                df = partition.compute()  # Compute Dask partition into Pandas DataFrame
                df.to_sql('json_data', con=connection, if_exists='append', index=False, method='multi')

        # Thread-safe counter update
        with files_processed_lock:
            global files_processed_this_minute
            files_processed_this_minute += 1

        print(f"Successfully processed {file_path}")
    except Exception as e:
        print(f"Logs: Error processing {file_path}: {e}")

# Blast JSON Files
"""
    ThreadPoolExecutor:
        - Creates a pool of threads (workers).
        - Each worker is assigned a task (processing one JSON file).
    executor.map:
        - Maps the process_json_file function to each file in the json_files list.
        - Automatically distributes files among the threads.
    Parameters:
        - max_workers: Number of threads running in parallel. This determines how many files are processed simultaneously.
"""
def blast_json_files(json_dir, max_workers, engine):
    json_files = [os.path.join(json_dir, f) for f in os.listdir(json_dir) if f.endswith(".json")]

    # Distribute tasks among multiple workers
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(lambda file: process_json_files(file, engine), json_files)

if __name__ == "__main__":
    engine = db_connection()

    if not test_db_connection(engine):
        print("Logs: Exiting due to database connection failure")
    else:
        print("All Good!")
