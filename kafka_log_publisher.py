from kafka import KafkaProducer
import json
import time
import configparser

config = configparser.ConfigParser()
config.read('/app/config.ini')

APPLICATION_NAME = config['Parameters']['application_name']
KAFKA_BOOTSTRAP_SERVERS = config['Parameters']['kafka_bootstrap_servers']
KAFKA_TOPIC_NAME = config['Parameters']['kafka_topic_name']


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)



def send_json_message_to_kafka(message):
    try:
        producer.send(KAFKA_TOPIC_NAME, value=message)
    except Exception as e:
        print("Error Could not publish the logs to logwritter",str(e))

def generate_kafka_log(log_level,module_name,method_name,log_message ):
    message = {
        'logId':"",
        'timestamp':int(time.time()),
        'logLevel':log_level,
        'applicationName': APPLICATION_NAME,
        'moduleName': module_name,
        'methodName': method_name,
        'logMessage': log_message,
        'userId':"SYSTEM",
        'clientBrowser':"",
    }
    send_json_message_to_kafka(message)