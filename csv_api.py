from flask import Flask, request, jsonify
import os
import json
import time
from apscheduler.schedulers.background import BackgroundScheduler
from crontab import CronTab
import logging
import subprocess
import configparser
from kafka import KafkaProducer
config = configparser.ConfigParser()
config.read('/app/config.ini')
app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

APPLICATION_NAME = config['Parameters']['application_name']
KAFKA_BOOTSTRAP_SERVERS = config['Parameters']['kafka_bootstrap_servers']
KAFKA_TOPIC_NAME = config['Parameters']['kafka_topic_name']
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
def send_json_message_to_kafka(message):
    producer.send(KAFKA_TOPIC_NAME, value=message)
def generate_kafka_log(log_level,method_name,log_message ):
    message = {
        'logId':"",
        'timestamp':int(time.time()),
        'logLevel':log_level,
        'applicationName': APPLICATION_NAME,
        'moduleName': "CSV_PROCESSOR_API",
        'methodName': method_name,
        'logMessage': log_message,
        'userId':"SYSTEM",
        'clientBrowser':"",
    }
    send_json_message_to_kafka(message)
@app.route('/csvprocess/setup', methods=['POST'])
def setup_csv_process():
    logging.info("API started")
    try:
        if not request.is_json:
            return jsonify({"error": "Missing JSON in request"}), 400
        data = request.get_json()
        message=f"Received data: {data}"
        logging.info(message)
        generate_kafka_log("INFO", "setup_csv_process", message)
        foldername = data.get('folderName')
        croninfo = data.get('cronInfo')
        friendlyName = data.get('friendlyName')
        
        if not foldername or not croninfo:
            generate_kafka_log("ERROR", "setup_csv_process", "Invalid inputs")
            return jsonify({"error": "Invalid inputs"}), 400

        base_path = f"/dataFiles/CSV_Data_Acquisition/{foldername}"
        os.makedirs(base_path, exist_ok=True)
        message=f"Created base path: {base_path}"
        generate_kafka_log("INFO", "setup_csv_process", message)
        logging.info(message)
        os.makedirs(os.path.join(base_path, "in_progress"), exist_ok=True)
        message=f"Created in_progress path: {os.path.join(base_path, 'in_progress')}"
        generate_kafka_log("INFO", "setup_csv_process", message)
        logging.info(message)
        os.makedirs(os.path.join(base_path, "finished"), exist_ok=True)
        message=f"Created finished path: {os.path.join(base_path, 'finished')}"
        generate_kafka_log("INFO", "setup_csv_process", message)
        logging.info(message)
        os.makedirs(os.path.join(base_path, "upload"), exist_ok=True)
        message = f"Created upload path: {os.path.join(base_path, 'upload')}"
        generate_kafka_log("INFO", "setup_csv_process", message)
        logging.info(message)
        os.makedirs(os.path.join(base_path, "error"), exist_ok=True)
        message = f"Created error path: {os.path.join(base_path, 'error')}"
        generate_kafka_log("INFO", "setup_csv_process", message)
        logging.info(message)
        cron = CronTab(user='root') 
        command = f"/usr/local/bin/python3 /app/dir_to_py.py '{base_path}' '{friendlyName}' '{croninfo}' >> /var/log/'{friendlyName}'.log 2>&1"
        generate_kafka_log("INFO", "setup_csv_process", f"Corn command: {command}")
        job = cron.new(command=command)
        job.setall(croninfo)
        job.set_comment(f'{friendlyName}')
        cron.write()
        return jsonify({"message": "Directories created successfully"}), 200

    except Exception as e:
        logging.error("An error occurred: %s", str(e))
        return jsonify({"error": "An error occurred"}), 500

@app.route('/csvprocess/cronjobinfo', methods=['POST'])
def get_cronjob_info():
    try:
        if not request.is_json:
            generate_kafka_log("ERROR", "get_cronjob_info", "Missing JSON in request")
            return jsonify({"error": "Missing JSON in request"}), 400
        data = request.get_json()
        message=f"Received data: {data}"
        generate_kafka_log("INFO", "get_cronjob_info", message)
        logging.info(message)
        friendly_name = data.get('friendlyName')
        cron = CronTab(user='root')
        jobs = cron.find_comment(f'{friendly_name}')
        result = subprocess.run(['service', 'cron', 'status'], stdout=subprocess.PIPE)
        output = result.stdout.decode('utf-8').strip()
        if output != "cron is running.":
            return jsonify({"error": output}), 500
        job_details = []
        for job in jobs:
            job_details.append(str(job))
        return jsonify(job_details), 200
    except Exception as e:
        message=f"An error occurred:\n{e}"
        generate_kafka_log("INFO", "get_cronjob_info", message)
        logging.error(message)
        return jsonify({"error": "An error occurred"}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=20000)
