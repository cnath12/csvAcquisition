import kafka_log_publisher as kafkaLog
import os,re
import util
import traceback
import load_csv as loadCsv
from datetime import datetime
from sqlalchemy import create_engine,text
import pandas as pd
import csv
MAX_MB_PER_FILE = int(util.config['Parameters']['max_mb_per_file'])*1024*1024
RAW_DATA_STORE_CONNECTION_STRING = util.config['Database']['smart_data_fabric_connection_string']
raw_store_engine = create_engine(RAW_DATA_STORE_CONNECTION_STRING)
CONNECTION_STRING = util.config['Database']['connection_string']
engine = create_engine(CONNECTION_STRING)
MODULE_NAME='spli_file'



def splitFile(file_path, base_folder, friendly_name, next_execution_time):
    kafkaLog.generate_kafka_log("INFO", MODULE_NAME,"splitFile", f'split start for the file {file_path}')
    file_split_path=None
    error_path=None
    in_progress_path=None
    print(f'started file {os.path.basename(file_path)} time {datetime.now()} ')
    try:
        
        in_progress_path = os.path.join(base_folder, util.IN_PROGRESS)
        error_path = os.path.join(base_folder, util.ERROR)
        file_split_path=os.path.join(base_folder, util.SPLIT)
        backup_path = os.path.join(base_folder, util.BACKUP)
        error_path = os.path.join(base_folder, util.ERROR)

        # tag the file if necessary and move the file to inprogress
        file_path=util.tag_file_uuid(file_path)
        file_path=util.move_file_to_directory(file_path, in_progress_path) 

        process_start_time=datetime.now()

        message = f"{os.path.basename(file_path)} Start processing {file_path}."
        kafkaLog.generate_kafka_log("INFO", MODULE_NAME,"splitFile", message)

        # calculate the file encoding and detect the file CSV seperator
        file_delimiter=util.detect_separator(file_path)
        headerValue=file_initial_validation(file_path=file_path,file_encoding=None,file_delimiter=file_delimiter)
        try:
             with open(file_path, 'r') as file:
                  split_function(base_folder=base_folder,file=file,file_path=file_path,file_split_path=file_split_path,backup_path=backup_path,friendly_name=friendly_name,file_encoding='utf-8',file_delimiter=file_delimiter,headerValue=headerValue,next_execution_time=next_execution_time)
             file_metadata=util.extractFilenameMetadata(os.path.basename(file_path))
             encodingFileName=file_metadata['uuid']+file_metadata['file_name']+'.fileencoding'
             encodingFileName=os.path.join(file_split_path,encodingFileName)
             with open (encodingFileName,'w') as file:
                  file.write('utf-8')
        except  UnicodeDecodeError  as e:
            print(f'Calculating the encoding and delimiter {os.path.basename(file_path)} time {datetime.now()} ')
            file_encoding,confidence=util.detect_encoding(file_path)
            file_delimiter=','
            if confidence==1.0:
                file_delimiter=util.detect_separator_known_encoding(file_path,file_encoding,raw_store_engine)
            else:
                file_encoding='utf-8'#default encoding
                file_delimiter=util.detect_separator(file_path)
                message = f"{os.path.basename(file_path)} Could not detect encoding so choose default endoding UTF-8."
                kafkaLog.generate_kafka_log("WARN", MODULE_NAME,"splitFile", message)
            print(f'Calculated encoding and delimiter {os.path.basename(file_path)} time {datetime.now()} ')
            kafkaLog.generate_kafka_log("WARN",MODULE_NAME, "splitFile", f'Calculated the file encoding as default did not work {file_encoding}')
            with open(file_path, 'r',encoding=file_encoding) as file:
                 split_function(base_folder=base_folder,file=file,file_path=file_path,file_split_path=file_split_path,backup_path=backup_path,friendly_name=friendly_name,file_encoding=file_encoding,file_delimiter=file_delimiter,headerValue=headerValue,next_execution_time=next_execution_time)
            file_metadata=util.extractFilenameMetadata(os.path.basename(file_path))
            encodingFileName=file_metadata['uuid']+file_metadata['file_name']+'.fileencoding'
            encodingFileName=os.path.join(file_split_path,encodingFileName)
            with open (encodingFileName,'w') as file:
                 file.write(str(file_encoding))
        print(f'Ended file {os.path.basename(file_path)} time {datetime.now()} ')                  
    except Exception as e:
        stack_trace_str = traceback.format_exc()
        print(stack_trace_str)
        kafkaLog.generate_kafka_log("ERROR",MODULE_NAME, "splitFile", str(e))
        util.move_file_to_directory(file_path, error_path)
        insertLog(big_file_name=os.path.basename(file_path),file_name=os.path.basename(file_path),base_folder=base_folder,friendly_name=friendly_name,table_name=None,next_execution_time=next_execution_time,process_start_time=process_start_time,process_end_time=process_start_time,status=util.ERROR,status_desc=str(e))
def split_function(base_folder,file,file_path,file_split_path,backup_path,friendly_name,file_encoding,file_delimiter,headerValue,next_execution_time):
        header = file_delimiter.join(headerValue)+'\n' if headerValue else ','.join(headerValue)+ '\n'# Read the header row
        fileNameOriginal = util.get_orig_filename(os.path.basename(file_path))
        sanitized_table_name = util.sanitize_table_name(fileNameOriginal)
        print(f'Create metadata time {datetime.now()} ')
        # create table DDL in csv db and metadata catalog in store db
        createMetadataCatalog(friendly_name=friendly_name,sanitized_table_name=sanitized_table_name,identifier=os.path.basename(file_path),header=headerValue)
        print(f'Created metadata time {datetime.now()} ')
        file_count = 1
        current_byte = 0
        row = file.readline()
        insert_csv_schedule_infos=[]
        unlock_out_files=[]
        while row:#start spliting writing the csv content to the file
            bigFileName=os.path.basename(file_path)
            fileName=os.path.basename(file_path)
            fileNameOriginal = util.get_orig_filename(bigFileName)
            # creating a hidden file
            fileName='.'+fileName.replace(fileNameOriginal,f'{util.SUB_FILE_IDENTIFER}_'+str(file_count)+f'_{util.SUB_FILE_IDENTIFER}_'+fileNameOriginal)
            output_file=os.path.join(file_split_path,fileName)
            with open(output_file, 'w',encoding=file_encoding) as out:
                out.write(header)
                process_start_time=datetime.now()
                print(f'started to write out file {fileName} time {datetime.now()} ')
                row_size = len(row)
                while current_byte + row_size <= MAX_MB_PER_FILE and row:
                    # if current_byte + row_size <= MAX_MB_PER_FILE:
                    out.write(row)
                    current_byte += row_size
                    row = file.readline()
                    if row:
                            row_size = len(row)
                out.close()
                kafkaLog.generate_kafka_log("INFO", MODULE_NAME,"splitFile", f'FileName:{fileNameOriginal},split to file {output_file}')
                unlock_out_files.append(output_file)
                sanitized_table_name = util.sanitize_table_name(fileNameOriginal)
                insertLogArgs = {
                                "big_file_name": bigFileName,
                                "file_name": fileName[1:],
                                "base_folder": base_folder,
                                "friendly_name": friendly_name,
                                "table_name": sanitized_table_name,
                                "next_execution_time": next_execution_time,
                                "process_start_time": process_start_time,
                                "process_end_time": datetime.now(),
                                "status": util.IN_PROGRESS,
                                "status_desc": None
                            }
                insert_csv_schedule_infos.append(insertLogArgs)
                file_count += 1
                current_byte = 0
                print(f'Ended out file {fileName} time {datetime.now()} ')
        kafkaLog.generate_kafka_log("INFO",MODULE_NAME, "splitFile", f'move file:{file_path},to location {backup_path}')
        [insertLog(**data) for data in insert_csv_schedule_infos]#upload the status to data base.
        [util.unlockFile(locked_file) for locked_file in unlock_out_files]
        insert_csv_schedule_infos=[]
        unlock_out_files=[]
        util.move_file_to_directory(file_path, backup_path)  
def insertLog(big_file_name,file_name,base_folder,friendly_name,table_name,next_execution_time,process_start_time,process_end_time,status,status_desc):
     log_data = {
            'folder_name': base_folder,
            'friendly_name': friendly_name,
            'table_name': table_name,
            'big_file_name':big_file_name,
            'file_name':file_name,
            'file_upload_count':0,
            'file_error_count':0,
            'process_start_time': process_start_time,
            'process_end_time': process_end_time,
            'next_execution_time': next_execution_time,
            'file_initial_count': 0,
            'status': status,
            'status_description':status_desc
        }
     loadCsv.insert_log(raw_store_engine, log_data)

def file_initial_validation(file_path,file_encoding,file_delimiter):
    fileNameOriginal = util.get_orig_filename(os.path.basename(file_path))
    sanitized_table_name = util.sanitize_table_name(fileNameOriginal)
    headerValue=[]
    if len(sanitized_table_name)>=63:
            raise Exception(f'File name {sanitized_table_name} length is beyond the limit')
    with open(file_path, 'r', encoding=file_encoding) as file:
        csv_reader = csv.reader(file,delimiter=file_delimiter)
        for index,row in enumerate(csv_reader):
                    if index==0:
                         headerValue=row
                         break
        if None in headerValue or ( len(headerValue) !=len([item for item in headerValue if item.strip()])) :
                raise Exception(f'{os.path.basename(file_path)} Invalid columns found in the file {file_path} : \n{headerValue} ')
            # replace all white space with _ 
        headerValue =[re.sub(r'\s+','_',re.sub(r'\n+','',val.replace('"','').replace("'",""))) for val in headerValue if val is not None and val!='']
        for index,column in enumerate(headerValue):
            if column.isnumeric():
                raise Exception(f'{os.path.basename(file_path)}  Invalid columns found in the file {file_path} : \n{headerValue}')
            if headerValue.count(column)>1:
                headerValue[index]=column+"_"+str(index)
    return headerValue


def createMetadataCatalog(friendly_name,sanitized_table_name,identifier,header):
    empty_df=pd.DataFrame()
    empty_df = pd.DataFrame(columns=header)
    
    metadataInfo=pd.DataFrame()
    with raw_store_engine.connect() as connection:
        result=connection.execute(text('''SELECT dci.topic_name as topic_name ,lower( ai_csv.engine) as engine,json_extract_path_text(ai_csv.db_info::json, 'host') as "host", 
                                        json_extract_path_text(ai_csv.db_info::json, 'database') as "database",
                                        ai_csv.id as application_info_id,acq.id as acquire_id, dci.id as dbz_connection_info_id
                                        FROM dbz_connection_info dci
                                        inner JOIN application_info ai_csv ON ai_csv.friendly_name = 'CSV Data Source'
                                        inner JOIN acquire_info acq ON acq.application_info_id = ai_csv.id
                                        WHERE dci.application_info_id = ai_csv.id AND dci.acquire_id = acq.id '''))
        metadataInfo = pd.DataFrame(result.fetchall(), columns=[
                                    'topic_name', 'engine', 'host', 'database', 'application_info_id', 'acquire_id', 'dbz_connection_info_id'])
    if metadataInfo is not None and not metadataInfo.empty:
        metadata_lable=str(metadataInfo['topic_name'][0])+friendly_name+str(metadataInfo['database'][0])+sanitized_table_name
        insert_data_into_table(empty_df, sanitized_table_name, engine, friendly_name,identifier)
        csvColumnInfoQuery=f'''SELECT table_schema,table_name,column_name,data_type ,COALESCE(character_maximum_length, numeric_precision) AS max_length FROM information_schema.columns WHERE table_schema = '{friendly_name}'
    AND table_name =lower('{sanitized_table_name}');'''
        with engine.connect() as connection:
            result=connection.execute(text(csvColumnInfoQuery))
            csvColumnInfo = pd.DataFrame(result.fetchall(), columns = ['table_schema','table_name','column_name','data_type','max_length'])
        for index, row in csvColumnInfo.iterrows():
            print(f'checking metadata insert for data :{row} \n index {index}')
            column=row['column_name']
            data_type="null"if row['data_type'] is None else "'"+ str(row['data_type'])+"'"
            max_length="null"if row['max_length'] is None else "'"+ str(row['max_length'])+"'"
            metadataInsertQuery=f'''INSERT INTO meta_data_catalog
                (
                            datasourcetype,
                            datasourcefullyqualifiedname,
                            datasourcename,
                            metadatalabel,
                            metadata_container,
                            metadata_element_name,
                            modified_date_time,
                            primary_key,
                            foreign_key,
                            ref_schema_name,
                            ref_table_name,
                            ref_column_name,
                            acquisitionsource,
                            max_length,
                            data_type,
                            application_info_id,
                            acquire_id,
                            dbz_connection_info_id
                )
                VALUES
                (
                            '{metadataInfo['engine'][0]}',
                            '{friendly_name}',
                            '{metadataInfo['host'][0]}',
                            '{metadata_lable}',
                            '{sanitized_table_name}',
                            '{column}',
                            'now()',
                            false,
                            false,
                            '',
                            '',
                            '',
                            '{metadataInfo['topic_name'][0]}',
                            {max_length},
                            {data_type},
                            {metadataInfo['application_info_id'][0]},
                {metadataInfo['acquire_id'][0]},
                {metadataInfo['dbz_connection_info_id'][0]}
                )'''
            with raw_store_engine.connect() as connection:
                res=connection.execute(text(f'''select count(*)from meta_data_catalog mdc where metadatalabel = '{metadata_lable}' and metadata_element_name ='{column}' ''')).fetchall()[0][0]
                print(f'count {res}')
                if connection.execute(text(f'''select count(*)from meta_data_catalog mdc where metadatalabel = '{metadata_lable}' and metadata_element_name ='{column}' ''')).fetchall()[0][0] == 0:
                    connection.execute(text(metadataInsertQuery))
                    connection.commit()
                    message = f"{identifier} MetaDataCatalog inserted for table {sanitized_table_name} query :\n{metadataInsertQuery}."
                    kafkaLog.generate_kafka_log("INFO",MODULE_NAME, "createMetadataCatalog", message)

def insert_data_into_table(data, table_name, engine, friendly_name,identifier):
    error_flag=True
    try:
        with engine.connect() as connection:
            try:
                 connection.execution_options(isolation_level="AUTOCOMMIT").execute(text(f'CREATE SCHEMA IF NOT EXISTS "{friendly_name}"'))
            except Exception as e:
                 print(str(e))
                 kafkaLog.generate_kafka_log("WARN", MODULE_NAME,'insert_data_into_table',"COULD NOT CREATE SCHEMA")
            data.to_sql(name=table_name, con=engine, schema=friendly_name, if_exists='append', index=False,)
            error_flag=False
            message = f"{identifier} Successfully inserted data into table {friendly_name}.{table_name}"
            kafkaLog.generate_kafka_log("INFO",MODULE_NAME, "insert_data_into_table", message)
    except Exception as main_e:
        stack_trace_str = traceback.format_exc()
        print(stack_trace_str)
        error_flag=True
        message=f"ERROR occurred while inserting data into table {friendly_name}.{table_name}:\n{main_e}"
        kafkaLog.generate_kafka_log("ERROR", MODULE_NAME,"insert_data_into_table", message)
    return error_flag
    