#!/bin/bash
set -e

# Install requirements if the requirements.txt file exists
if [ -e "/opt/airflow/requirements.txt" ]; then
    $(command python) pip install --upgrade pip
    $(command -v pip) install --user -r requirements.txt
fi

# Initialize the Airflow database if it doesn't exist
if [ ! -f "/opt/airflow/airflow.db" ]; then
    airflow db init && \
    airflow users create \
        --username admin \
        --firstname John \
        --lastname Tan \
        --role Admin \
        --email johntan@admin.com \
        --password admin123

fi 

# Upgrade the Airflow database
$(command -v airflow) db upgrade

# Start the Airflow webserver
exec airflow webserver