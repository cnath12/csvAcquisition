import datetime
import pandas as pd
# from psycopg2 import sql
from sqlalchemy import create_engine,text
from sqlalchemy.exc import SQLAlchemyError
import util as util
import kafka_log_publisher as kafkaLog
import csv,traceback,re
import os
from datetime import datetime
import concurrent.futures
CONNECTION_STRING = util.config['Database']['connection_string']
RAW_DATA_STORE_CONNECTION_STRING = util.config['Database']['smart_data_fabric_connection_string']

engine = create_engine(CONNECTION_STRING)
raw_store_engine = create_engine(RAW_DATA_STORE_CONNECTION_STRING)
CHUNK_SIZE = int(util.config['Parameters']['chunk_size'])

MODULE_NAME='load_csv'

MAX_FILE_READ = int(util.config['Parameters']['max_file_read'])

    
def insert_log(raw_store_engine, log_data):
    try:
        query = text("""
        INSERT INTO csv_acquisition_service_log (folder_name, friendly_name, table_name,big_file_name, file_name,file_upload_count,file_error_count,
        process_start_time, process_end_time, next_execution_time, file_initial_count, status,status_description)
        VALUES (:folder_name, :friendly_name, :table_name, :big_file_name,
        :file_name,:file_upload_count,:file_error_count,
        :process_start_time, :process_end_time, :next_execution_time,
        :file_initial_count, :status,:status_description)
        RETURNING id;
        """)
        with raw_store_engine.connect() as connection:
            result = connection.execute(query, log_data)
            log_id = result.fetchone()[0]
            print(f"log_id: {log_id}")
            connection.commit()
            return log_id
    except SQLAlchemyError as e:
        print("An util.ERROR occurred while inserting the log:", str(e))
        return None

def update_log(raw_store_engine, log_data, file_name):
    try:
        query = text("""
        UPDATE csv_acquisition_service_log
        SET 
            process_end_time = now(),
            status = :status,
             file_initial_count = :file_initial_count,
            file_upload_count=:file_upload_count,
            status_description = :status_description,
            file_error_count = :file_error_count
        WHERE file_name = :file_name;
        """)
        with raw_store_engine.connect() as connection:
            connection.execute(query, {**log_data, 'file_name': file_name})
            connection.commit()
    except SQLAlchemyError as e:
        print("An util.ERROR occurred while updating the log:", str(e))
        
def update_log_status(raw_store_engine, log_data, file_name):
    try:
        query = text("""
        UPDATE csv_acquisition_service_log
        SET 
            process_end_time = now(),
            status = :status,
            status_description = :status_description
        WHERE file_name = :file_name;
        """)
        with raw_store_engine.connect() as connection:
            connection.execute(query, {**log_data, 'file_name': file_name})
            connection.commit()
    except SQLAlchemyError as e:
        print("An util.ERROR occurred while updating the log:", str(e))

def import_csv_db(file_path,base_folder,friendly_name,sanitized_table_name):
    status=util.ERROR
    status_description=''
    total_valid_count=0
    total_invalid_count=0
    total_count=0
    valid_count=0
    invalid_count=0
    finished_path = os.path.join(base_folder, util.FINISHED)
    error_file_path= os.path.join(base_folder, util.ERROR)
    try:
        file_encoding=None
        file_metadata=util.extractFilenameMetadata(os.path.basename(file_path))
        encodingFileName=file_metadata['uuid']+file_metadata['file_name']+'.fileencoding'
        encodingFileName=os.path.join(util.getParentDirPath(file_path,1),encodingFileName)
        try:
            with open (encodingFileName,'r') as file:
                    file_encoding=file.read()
                    if file_encoding:
                        file_encoding.strip()
        except Exception as e:
            kafkaLog.generate_kafka_log("Warn",MODULE_NAME, "import_csv_db", f"could not find encoding file {encodingFileName} so set to utf-8 \n {str(e)}")
        print(f'Started loading to db for file {file_path} time {datetime.now()}')
        with open(file_path, 'r',encoding=file_encoding) as file:
            file_delimiter=util.detect_separator_known_encoding(file_path,file_encoding,raw_store_engine)
            csv_reader = csv.reader(file,delimiter=file_delimiter)
            headerValue=[]
            invalid_data_rows=[]
            valid_data_rows=[]
            valid_data=pd.DataFrame()
            for count, row in enumerate(csv_reader):
                total_count+=1
                if count==0:
                    total_count-=1
                    headerValue=row
                    kafkaLog.generate_kafka_log("INFO",MODULE_NAME, "import_csv_db", f"{os.path.basename(file_path)} File's Headers are as follows : \n {str(headerValue)}")
                #  Condition to check the data and ignore header line   
                elif len(row)<=len(headerValue)  and len(row) !=0 :
                    valid_count+=1
                    valid_data_rows.append(row)
                    if valid_count>=CHUNK_SIZE :
                        total_valid_count+=valid_count
                        valid_data=pd.DataFrame(valid_data_rows, columns=headerValue)
                        insert_data_into_table(valid_data, sanitized_table_name, engine, friendly_name,os.path.basename(file_path))
                        valid_data=None
                        valid_count=0
                        valid_data_rows=[]
                else :
                    invalid_count=+1
                    invalid_data_rows.append(row) 
                    if invalid_count>=CHUNK_SIZE :
                        total_invalid_count+=invalid_count
                        file_name=os.path.basename(file_path)
                        error_file_path = os.path.join(base_folder, util.ERROR,file_name)
                        pd.DataFrame(invalid_data_rows).to_csv(error_file_path,index= False,header=False,mode='a')
                        kafkaLog.generate_kafka_log("WARN", MODULE_NAME,"import_csv_db", f" {os.path.basename(file_path)} File from ({file_path}) has util.ERROR.\nError reading CSV Please find the errors in the in the location {error_file_path} of lenght {total_invalid_count} ")
                        invalid_count=0
                        invalid_data_rows=[]
            if valid_count>0:
                total_valid_count+=valid_count
                valid_data=pd.DataFrame(valid_data_rows, columns=headerValue)
                insert_data_into_table(valid_data, sanitized_table_name, engine, friendly_name,os.path.basename(file_path))
                valid_data=None
                valid_count=0
                valid_data_rows=[]
            if invalid_count>0:
                total_invalid_count+=invalid_count
                file_name=os.path.basename(file_path)
                error_file_path = os.path.join(base_folder, util.ERROR,file_name)
                pd.DataFrame(invalid_data_rows).to_csv(error_file_path,index= False,header=False,mode='a')
                kafkaLog.generate_kafka_log("WARN",MODULE_NAME, "import_csv_db", f" {os.path.basename(file_path)} File from ({file_path}) has util.ERROR.\nError reading CSV Please find the errors in the in the location {error_file_path} of lenght {total_invalid_count} ")
                invalid_count=0
                invalid_data_rows=[]
            if total_invalid_count > 0 and total_valid_count > 0:
                status=util.WARN
                status_description=f'{os.path.basename(file_path)} File contains both valid and invalid rows and the detected invalid rows are moved to util.ERROR folder {error_file_path} errorcount {total_invalid_count}'
                kafkaLog.generate_kafka_log("WARN", MODULE_NAME,"import_csv_db", status_description)
            elif total_invalid_count > 0:
                status=util.ERROR
                status_description=f'{os.path.basename(file_path)} File contains invalid data so file moved to {error_file_path}'
                kafkaLog.generate_kafka_log("ERROR", MODULE_NAME,"import_csv_db", status_description)
            elif total_valid_count > 0:
                status=util.FINISHED
                status_description=f' {os.path.basename(file_path)} File contains valid data so file moved to {finished_path}'
                kafkaLog.generate_kafka_log("INFO", MODULE_NAME,"import_csv_db", status_description)
            util.move_file_to_directory(file_path,finished_path)
            print(f'Ended loading to db for file {file_path} time {datetime.now()}')
        
    except Exception as e:
        stack_trace_str = traceback.format_exc()
        print(stack_trace_str)
        kafkaLog.generate_kafka_log("ERROR", MODULE_NAME,"import_csv_db", f"{os.path.basename(file_path)} File with the path ({file_path}) has util.ERROR.\nError reading CSV: {str(e)}")
        status=util.ERROR
        status_description=f'File contains invalid data so file moved to {error_file_path}, possible util.ERROR \n {str(stack_trace_str)}'
        error_path = os.path.join(base_folder, util.ERROR)
        util.move_file_to_directory(file_path, error_path)
    log_data = {
    'status': status,
    'file_initial_count':total_count,
    'file_upload_count': total_valid_count,
    'status_description': status_description,
    'file_error_count': total_count-total_valid_count
    }
    return log_data

def insert_data_into_table(data, table_name, engine, friendly_name,identifier):
    error_flag=True
    try:
        with engine.connect() as connection:
            connection.execution_options(isolation_level="AUTOCOMMIT").execute(text(f'CREATE SCHEMA IF NOT EXISTS "{friendly_name}"'))
            data.to_sql(name=table_name, con=engine, schema=friendly_name, if_exists='append', index=False, chunksize=CHUNK_SIZE)
            error_flag=False
            message = f"{identifier} Successfully inserted data into table {friendly_name}.{table_name}"
            kafkaLog.generate_kafka_log("INFO", MODULE_NAME,"insert_data_into_table", message)
    except Exception as main_e:
        stack_trace_str = traceback.format_exc()
        print(stack_trace_str)
        error_flag=True
        message=f"ERROR occurred while inserting data into table {friendly_name}.{table_name}:\n{main_e}"
        kafkaLog.generate_kafka_log("ERROR",MODULE_NAME, "insert_data_into_table", message)
    return error_flag

def process_csv_files(file_path, base_folder):
    file_path_in_progress=None
    error_path=None
    friendly_name=''
    try:
        error_path = os.path.join(base_folder, util.ERROR)
        message = f"{os.path.basename(file_path)} Start processing {file_path}."
        kafkaLog.generate_kafka_log("INFO",MODULE_NAME, "process_csv_files", message)
        
        file_metadata=util.extractFilenameMetadata(os.path.basename(file_path))
        friendly_name=file_metadata['schema']
        sanitized_table_name=util.sanitize_table_name(file_metadata['file_name'])
        
        log_data=import_csv_db(file_path,base_folder,friendly_name,sanitized_table_name)
        update_log(raw_store_engine,log_data,os.path.basename(file_path))
        message = f"{os.path.basename(file_path)} File ({file_path}) read completed & imported."
        kafkaLog.generate_kafka_log("INFO", MODULE_NAME,"process_csv_files", message)
    except Exception as e:
         stack_trace_str = traceback.format_exc()
         print(stack_trace_str)
         kafkaLog.generate_kafka_log("ERROR", MODULE_NAME,"process_csv_files", str(os.path.basename(file_path))+ " "+str(e))
         log_data = {
             'status': util.ERROR,             
             'status_description': str(e)
             }
         update_log_status(raw_store_engine,log_data,os.path.basename(file_path))
         if file_path_in_progress is None:
             print('file path in progress is none')
             util.move_file_to_directory(file_path,error_path)
         else:
             util.move_file_to_directory(file_path_in_progress,error_path)

import os

def process_available_csv_files(directory):
    try:
        futureLst=[]

        files = [file for file in os.listdir(directory) if file.endswith('.csv') and not file.startswith('.')  ]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_FILE_READ) as pool_pick:
            for file in files:
                file_path=os.path.join(directory,file)
                base_folder=util.getParentDirPath(file_path,3)
                futureLst.append(pool_pick.submit(process_csv_files,file_path, base_folder))
        concurrent.futures.wait(futureLst)
            
    except Exception as e:
        stack_trace_str = traceback.format_exc()
        print(stack_trace_str)


