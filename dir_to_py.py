from sqlalchemy import create_engine
from croniter import croniter
import argparse
import os
from datetime import datetime
import concurrent.futures
import util as util
import kafka_log_publisher as kafkaLog
import split_file as fileSpliter
import threading

CONNECTION_STRING = util.config['Database']['connection_string']
RAW_DATA_STORE_CONNECTION_STRING = util.config['Database']['smart_data_fabric_connection_string']
MODULE_NAME='dir_to_py'

MAX_FILE_PICK = int(util.config['Parameters']['max_file_pick'])

engine = create_engine(CONNECTION_STRING)
raw_store_engine = create_engine(RAW_DATA_STORE_CONNECTION_STRING)


def get_next_execution_time(process_start_time, croninfo):
    cron = croniter(croninfo, process_start_time)
    next_execution_time = cron.get_next(datetime)
    return next_execution_time


def job(folder_path, friendly_name, croninfo):
    # with  concurrent.futures.ThreadPoolExecutor(max_workers=int(MAX_FILE_PICK)) as FILE_PICK_THREADS:
    kafkaLog.generate_kafka_log("INFO", MODULE_NAME,"job", "Job Processing Started")
    process_start_time = datetime.now()
    # genrate next execution date time before insert the log
    next_execution_time = get_next_execution_time(process_start_time,croninfo)
    util.ensure_directories_exist(folder_path)
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv') and not f.startswith('.')]
    message=f"Found {len(csv_files)} CSV files to process."
    kafkaLog.generate_kafka_log("INFO", MODULE_NAME,"job", message)
    futureLst=[]
    if len(csv_files) > 0:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_FILE_PICK) as pool_pick:
            for file_name in os.listdir(folder_path):
                full_path = os.path.join(folder_path, file_name)
                if file_name.endswith('.csv') and not os.path.isdir(full_path) and not file_name.startswith("."):
                    full_path = os.path.join(folder_path, file_name)
                    kafkaLog.generate_kafka_log("INFO", MODULE_NAME,"job", f"File {full_path} starting to process")
                    futureLst.append(pool_pick.submit(fileSpliter.splitFile,full_path, folder_path, friendly_name,  next_execution_time))
                    kafkaLog.generate_kafka_log("INFO", MODULE_NAME,"job", f"File {full_path} Started to process")
            concurrent.futures.wait(futureLst)
        
    else:
        kafkaLog.generate_kafka_log("INFO", MODULE_NAME,"job", "No files found to process")

def main():
    parser = argparse.ArgumentParser(description="Process CSV files and insert data into a PostgreSQL database.")
    parser.add_argument('main_folder_path', type=str, help='Path to the main folder containing the subfolders with CSV files.')
    parser.add_argument('friendly_name', type=str, help='Friendly name of the table schema, helps to create schema for csv table.')
    parser.add_argument('croninfo', type=str, help='Cron job info String, helps to determine the next schedule job date & time.')
    args = parser.parse_args()
    concatenated_string = f"Get Request From Cron Job With Following arguments:\nmain_folder_path: {args.main_folder_path},\nfriendly_name: {args.friendly_name},\ncroninfo: {args.croninfo}"
    kafkaLog.generate_kafka_log("INFO", MODULE_NAME,"main", concatenated_string)
    job(args.main_folder_path, args.friendly_name, args.croninfo)

if __name__ == "__main__":
    main()