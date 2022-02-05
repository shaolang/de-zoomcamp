FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow
COPY requirements.txt .

USER root
RUN apt-get update -qq && apt-get install vim -qqq

RUN pip install --no-cache-dir -r requirements.txt

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

WORKDIR $AIRFLOW_HOME
COPY scripts scripts
RUN chmod +x scripts

USER 1000
