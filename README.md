# Project Data Warehouse

## Description
In this project, we build 
an ETL pipeline for a database hosted on Redshift. 
First, we'll load data 
from S3 to staging tables on Redshift and then execute 
SQL statements that create the analytics tables 
from these staging tables.

## Project Datasets

Upstream data is stored in S3 buckets.
- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data. The json file in s3://udacity-dend/log_json_path.json contains the meta information to load s3://udacity-dend/log_data.

As a **sanity check**, run the code in s3.py to see if the data
in the s3 buckets is accessible.

## Create a Redshift Cluster

Run create_cluster.py to create a Redshift cluster. The steps include
- Create an IAM role
- Create a Redshift cluster
- Open an incoming TCP port to access the cluster endpoint

Test in the AWS console to see if the cluster is created successfully.

## Create Tables

Run create_tables.py to create the staging tables and the analytics tables.
The schema of the tables is defined in sql_queries.py.

## ETL Pipeline

Run etl.py to 
- load data from S3 to staging tables on Redshift (function load_staging_tables).
- insert data from staging tables to analytics tables on Redshift (function insert_tables).

## Query Analytics Tables

Use the AWS console (query editor) to query the analytics tables to see if the ETL pipeline works as expected.



