<!--
  Title: Covid19-ETL-Datapipeline
  Description: A Data Engineering Project that implements an ETL data pipeline using Dagster, Spark, Plotly, Dash
  Author: thangbuiq
  -->
# Covid19-ETL-Datapipeline
![GitHub last commit](https://img.shields.io/github/last-commit/thangbuiq/covid19-etl-pipeline?color=green) ![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dash)

This repository contains a data engineering project that implements an **ETL (Extract, Transform, Load)** data pipeline using `Dagster, Spark, Plotly, Dash`. The goal of this project is to extract data related to Covid-19 from various sources, transform it to a standard format, and load it into a database. The transformed data is then used to create interactive dashboards using `Plotly` and `Dash`.

## Project Structure
The project is structured as follows:

- dagster, dagster-home, etl_pipeline: Contains the Dagster pipeline code (Spark data transformation)
- spark: Contains the Spark initialization
- notebooks: Contains the code for testing and for the interactive dashboards (Plotly + Dash)
- covid-19-dataset: Contains the raw and transformed data

## Technologies Used
> **The following technologies have been used in this project:**

* Dagster: A data orchestrator for machine learning, analytics, and ETL.
* Spark: An open-source distributed computing system used for big data processing.
* MySQL: An open-source relational database management system.
* PostgreSQL: An open-source relational database management system.
* MinIO: An open-source object storage server.
* Plotly: An open-source data visualization library.
* Dash: An open-source Python framework for building analytical web applications.


> **Cre(dataset):** https://www.kaggle.com/datasets/imdevskp/corona-virus-report
