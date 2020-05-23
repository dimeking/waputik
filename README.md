# Data Lakes with Spark (Amazon EMR) 

### Summary

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

We need to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional & fact tables for their analytics team to continue finding insights in what songs their users are listening to. Test the analytics tables and ETL pipeline by running queries.

### Project Description

Build an ETL pipeline for a Data Lake hosted on S3. We will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. Deploy this Spark process on a cluster using AWS EMR.

### Pre-Requisites
* AWS EMR Spark Hadoop Cluster, Security Group (Allow SSH)
* Ensure pyspark is installed in a python3 environment
* Jupyter Notebook (Testing)

### How to Run & Test

* Create an AWS EMR Cluster
* Add AWS Key & Secret to dl.cfg
* Secure Copy etl.py & dl.cfg to Cluster (Master node). 
```sh
$ scp -i ~/<your_file.pem> etl.py dl.cfg hadoop@<your_master_public_dns>:/home/hadoop
```
* Connect to Cluster (Master node). 
```sh
$ ssh -i ~/<your_file.pem> etl.py hadoop@<your_master_public_dns>
```
* Run the Spark script to create fact-dimension tables onto the S3.
```sh
$ spark-submit --master yarn etl.py
```
* Output parquet files are available at s3a://hr-dend/sparkify_tables

### Files
| Filename |  |
| ------ | ------ |
| etl.py: | Extract & Load (Copy song & log datafiles from S3 onto AWS EMR Spark Cluster), Transform  (Transform all the raw data to analytics data via Spark Dataframes on EMR), Persist (store partitioned analytics tables onto S3). |
| dl.cfg | Config file with AWS parameters. |

### Analytics Tables Design
##### Raw Data  
- song_data
- log_data

##### Dimension Tables
- songs_table
- artists_table
- users_table
- time_table

##### Facts Table
- songplays_table

##### PARTITION, SORT
- year & artist_id used for partition of songs_table
- year & month used for partition of time_table
- year & month used for partition of songplays_table
- song_id, artist_id, user_id, timestamp, songplay_id used for sorting

```sh
# songs in music database
songs_table_schema:
root
 |-- song_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- year: long (nullable = true)
 |-- duration: double (nullable = true)

# artists in music database
artists_table_schema:
root
 |-- artist_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- location: string (nullable = true)
 |-- lattitude: double (nullable = true)
 |-- longitude: double (nullable = true)

# users in the app
users_table_schema:
root
 |-- user_id: long (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- level: string (nullable = true)

# timestamps of records in songplays broken down into specific units
time_table_schema:
root
 |-- start_time: long (nullable = true)
 |-- hour: short (nullable = true)
 |-- day: short (nullable = true)
 |-- week: short (nullable = true)
 |-- month: short (nullable = true)
 |-- year: short (nullable = true)
 |-- weekday: string (nullable = true)

# records in log data associated with song plays i.e. records with page 'NextSong'
# ensure that a user is using a single session at any particular time
songplays_table_schema:
root
 |-- songplay_id: long (nullable = false)
 |-- start_time: long (nullable = true)
 |-- user_id: long (nullable = true)
 |-- level: string (nullable = true)
 |-- song_id: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- session_id: long (nullable = true)
 |-- location: string (nullable = true)
 |-- user_agent: string (nullable = true)
 |-- month: short (nullable = true)
 |-- year: short (nullable = true)
 
 ```

### Acknowledgement
Author: Hari Raja
Framework: Udacity
Date: May 22 2020
