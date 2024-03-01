# Use an official Python image from the DockerHub
FROM python:3.11-slim

# Set the working directory in the Docker container
WORKDIR /app

# Copy the dependencies file to the working directory
COPY requirements.txt .

# Install any dependencies
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y cron

# Copy the other necessary files for the script to run
COPY dir_to_py.py .
COPY csv_api.py .
COPY config.ini .
COPY split_file.py .
COPY kafka_log_publisher.py .
COPY load_csv.py .
COPY util.py .
COPY scheduler.py .

# Expose volume for external CSV files
VOLUME ["/dataFiles"]

# Set the Flask application environment variable
ENV FLASK_APP=csv_api.py

# Grant execute permissions to the script

# Start cron service and execute the scheduler script followed by Flask
#ENTRYPOINT service cron start && flask run --host=0.0.0.0 --port=20000 & /usr/local/bin/python3 /app/scheduler.py >> /var/log/scheduler.log 2>&1
ENTRYPOINT service cron start && exec flask run --host=0.0.0.0 --port=20000 & /usr/local/bin/python3 /app/scheduler.py >> /var/log/scheduler.log 2>&1

# This command runs your application along with cron, start as service (To check Status - service cron status, To check job list - crontab -l)
