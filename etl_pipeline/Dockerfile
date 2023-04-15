FROM python:3.9.16-slim

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

USER root 


RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    "openjdk-17-jre-headless" \
    ca-certificates-java && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster/app

# Add repository code

COPY requirements.txt /opt/dagster/app
RUN pip install --upgrade pip && pip install -r requirements.txt

WORKDIR /opt/dagster/app
COPY . /opt/dagster/app/elt_pipeline
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "etl_pipeline"]
