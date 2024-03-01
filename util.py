import csv
import codecs
from fileinput import filename
import chardet
import os,re
import uuid
from datetime import datetime
import kafka_log_publisher as kafkaLog
from sqlalchemy import text,create_engine
import configparser
import shutil



config = configparser.ConfigParser()
config.read('/app/config.ini')

IN_PROGRESS = 'in_progress'
SPLIT = os.path.join('in_progress', 'split')
ERROR = 'error'
FINISHED = 'finished'
WARN='warn'
BACKUP='backup'
MODULE_NAME='util'
FILE_UUID = config['Parameters']['file_prefix']
SUB_FILE_IDENTIFER='sub_count'
SCHEMA_IDENTIFIER='csv_schema'
FOLDER_BASE_LOCATION=os.path.join(' ','dataFiles','CSV_Data_Acquisition').strip()


RAW_DATA_STORE_CONNECTION_STRING = config['Database']['smart_data_fabric_connection_string']
raw_store_engine = create_engine(RAW_DATA_STORE_CONNECTION_STRING)



def detect_encoding(file_path):
    try:
        with open(file_path, 'rb') as file:
            raw_data = file.read()
            result = chardet.detect(raw_data)
            encoding = result['encoding']
            confidence = result['confidence']
            return encoding, confidence
    except Exception as e:
        return 'utf-8',0

def detect_separator(file_path):
    try:
        with codecs.open(file_path, 'r') as file:
            first_line = file.readline().strip()
            dialect = csv.Sniffer().sniff(first_line, [',', '|', ';'])
            return dialect.delimiter
    except Exception as e:
        return ','

def detect_separator_known_encoding(file_path,encoding,raw_store_engine):
    try:
        with codecs.open(file_path, 'r', encoding=encoding) as file:
            first_line = file.readline().strip()
            with raw_store_engine.connect() as connection:
                result=connection.execute(text(str('select * from file_seperator_list')))
            dialect = csv.Sniffer().sniff(first_line, result.fetchone()[0])
            return dialect.delimiter
    except Exception as e:
        return ','

def tag_file_uuid(file_path):
# to the given file path this function adds a schema and unique identifier so this can be used for split later
    file_name = os.path.basename(file_path)
    if not file_name.startswith(SCHEMA_IDENTIFIER):
            # add uuid to file name if not present
            dir_name = os.path.dirname(file_path)
            uuid_part = f"{FILE_UUID}_{int(datetime.now().timestamp() * 1000)}_{uuid.uuid4()}_{FILE_UUID}"
            customer_dir=getParentDirName(file_path,1)
            friendly_name=get_friendly_name_from_db(customer_dir)
            schema_part=f'{SCHEMA_IDENTIFIER}_{friendly_name}_{SCHEMA_IDENTIFIER}'
            rename_file =  schema_part+'_'+uuid_part+ '_'+file_name
            rename_path=os.path.join(dir_name,rename_file)
            os.rename(file_path,rename_path)
            kafkaLog.generate_kafka_log("INFO",MODULE_NAME, "tagFileUUID", f"{os.path.basename(rename_path)} Renamed file ({file_path}) to {rename_path}")
            return rename_path
    else:
        return file_path    

def unlockFile(file_path):
    # if the file is hidden ie starts will . it will remove the first . form the file
    file_name = os.path.basename(file_path)
    if file_name.startswith('.'):
        file_dir=getParentDirPath(file_path,1)
        file_name=file_name[1:]
        rename_path=os.path.join(file_dir,file_name)
        os.rename(file_path,rename_path)
        return rename_path

def get_friendly_name_from_db(config_info):
    print('config info ',config_info)
    friendly_name=None
    with raw_store_engine.connect() as connection:
        result = connection.execute(text('SELECT friendly_name FROM csv_schedule_info WHERE config_info = :config_info'), {'config_info': config_info})
        friendly_name = result.fetchone()[0] 
    return friendly_name if friendly_name else ''

def  get_initial_count(file_path):
    with open(file_path, 'r') as file:
            csv_reader = csv.reader(file)
            line_count = sum(1 for _ in csv_reader)
            return line_count

def ensure_directories_exist(base_folder):
    try:
        for folder in [IN_PROGRESS, FINISHED, ERROR,BACKUP,SPLIT]:
            folder_path = os.path.join(base_folder, folder)
            if not os.path.exists(folder_path):
                os.makedirs(os.path.join(folder_path), exist_ok=True)
                kafkaLog.generate_kafka_log("INFO", MODULE_NAME,"ensure_directories_exist", f"{folder} Directory Created")       
    except (PermissionError, FileNotFoundError) as e:
        message = f"Error occurred while creating directories: {e}"
        kafkaLog.generate_kafka_log("ERROR", MODULE_NAME,"ensure_directories_exist", message)

def move_file_to_directory(file_path, target_directory):
    """
    Move the given file to the target directory.
    """
    os.makedirs(target_directory, exist_ok=True)
    try:
        destination_location=os.path.join(target_directory,  os.path.basename(file_path))
        shutil.move(file_path,destination_location )
        kafkaLog.generate_kafka_log("INFO",MODULE_NAME, "move_file_to_directory", f"{os.path.basename(file_path)} Moved File from ({file_path}) to {destination_location}")
        return destination_location
    except Exception as e:
         kafkaLog.generate_kafka_log("ERROR", MODULE_NAME,"move_file_to_directory", f"{os.path.basename(file_path)} Could not move File from ({file_path}) to {target_directory},{str(e)}")



def getParentDirName(path,level):
    # return the folder name if level is 0 returns file name
    parent_directory=path
    try:
        for i in range(level):
            parent_directory = os.path.dirname(path)
            path=parent_directory
    except Exception as e:
        print (e)
    return os.path.basename(parent_directory)

def getParentDirPath(path, level):
    # return the folder name if level is 0 returns file name
    parent_directory=path
    try:
        for i in range(level):
            parent_directory = os.path.dirname(path)
            path=parent_directory
    except Exception as e:
        print (e)
    return parent_directory

def extractFilenameMetadata(fileName):
    pattern=rf'{SCHEMA_IDENTIFIER}_(.*?)_{SCHEMA_IDENTIFIER}_{FILE_UUID}_(.*?)_{FILE_UUID}_{SUB_FILE_IDENTIFER}_(.*?)_{SUB_FILE_IDENTIFER}_(.*?)\.csv'
    match = re.search(pattern, fileName,re.IGNORECASE)
    if match:
        schema=match.group(1)
        uuid=match.group(2)
        sub_file_no=match.group(3)
        file_name=match.group(4)
        return {'schema':schema,'uuid':uuid,'sub_file_no':sub_file_no,'file_name':file_name}
    else:
        pattern=rf'{SCHEMA_IDENTIFIER}_(.*?)_{SCHEMA_IDENTIFIER}_{FILE_UUID}_(.*?)_{FILE_UUID}_(.*?)\.csv'
        match = re.search(pattern, fileName)
        if match:
            schema=match.group(1)
            uuid=match.group(2)
            file_name=match.group(3)
            return  {'schema':schema,'uuid':uuid,'sub_file_no':None,'file_name':file_name}
        else:
            return  {'schema':None,'uuid':None,'sub_file_no':None,'file_name':fileName}

def get_orig_filename(fileName):
    return extractFilenameMetadata(os.path.basename(fileName))['file_name']

def sanitize_table_name(name):
    # REMOVE THE FILE uuid tag and fetch the file name
    name = extractFilenameMetadata(os.path.basename(name))['file_name']
    sanitizedName=re.sub(r'[^a-zA-Z0-9_]', '_', name).lower()
    sanitizedName=re.sub(r'^[^a-zA-Z]+', '', sanitizedName)
    if name!=sanitizedName:
        message = f"{name}Table name {name} was sanitized to {sanitizedName}."
        kafkaLog.generate_kafka_log("WARN",MODULE_NAME, "sanitize_table_name", message)
    return sanitizedName