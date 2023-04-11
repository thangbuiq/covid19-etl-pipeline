FROM python:3.9.16-slim

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

USER root 

ARG spark_version="3.3.2"
ARG hadoop_version="3"
ARG scala_version
ARG spark_checksum="4cd2396069fbe0f8efde2af4fd301bf46f8c6317e9dea1dd42a405de6a38380635d49b17972cb92c619431acece2c3af4c23bfdf193cedb3ea913ed69ded23a1"
ARG openjdk_version="17"

ENV APACHE_SPARK_VERSION="${spark_version}" \
    HADOOP_VERSION="${hadoop_version}"

RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    "openjdk-17-jre-headless" \
    ca-certificates-java && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster/app

RUN apt-get update --yes && apt-get install -y curl wget tar

# Add repository code

COPY requirements.txt /opt/dagster/app
RUN pip install --upgrade pip && pip install -r requirements.txt

WORKDIR /opt/dagster/app
COPY . /opt/dagster/app/elt_pipeline
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "etl_pipeline"]
