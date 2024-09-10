FROM apache/airflow:2.6.0-python3.9

USER root

# Install git
RUN apt-get update && apt-get install -y git

# Switch back to the airflow user
USER airflow
