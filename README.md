# **Project: Data Lake**      


# Introduction    
A startup called **Sparkify** has grown their user base and song database and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.      
As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.      

# Project Description
In this project, I build ETL pipeline for a data lake hosted on S3. This project extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. I deployed this Spark process on a cluster using AWS.    

# Project Datasets
## Datasets  
- There are two datasets that reside in S3. Here are the S3 links for each:
 1. **Song data** : s3://udacity-dend/song_data (for example log_data/2018/11/2018-11-12-events.json)      
 2. **Log data** : s3://udacity-dend/log_data (for example log_data/2018/11/2018-11-12-events.json)           

### 1. Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.             

        - song_data/A/B/C/TRABCEI128F424C983.json
        - song_data/A/A/B/TRAABJL12903CDCF1A.json      

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.     

```json
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}```

### 2. Log Dataset
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.
The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.           

          - log_data/2018/11/2018-11-12-events.json
          - log_data/2018/11/2018-11-13-events.json

## Schema for Song Play Analysis
Using the song and event datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

![Image of Tables](image/Tables_cluster.PNG)

### Fact Table
1. **songplays** - records in event data associated with song plays i.e. records with page NextSong
   - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
1. **users** - users in the app
   - user_id, first_name, last_name, gender, level
2. **songs** - songs in music database
   - song_id, title, artist_id, year, duration
3. **artists** - artists in music database
   - artist_id, name, location, lattitude, longitude
4. **time** - timestamps of records in songplays broken down into specific units
   - start_time, hour, day, week, month, year, weekday

![Image of Records](image/Records.PNG)

## Build ETL Pipeline
1. Loading data from S3 to staging tables on Redshift.
2. Loading data from staging tables to analytics tables on Redshift.
3. Running the analytic queries on Redshift database.

![Image of Records](image/SE_editor.PNG)


# Starting the program  
- [x] Add IAM role info to "dwh.cfg".
- [x] Execute "create_table.py" is where you'll create your fact and dimension tables for the star schema in Redshift..  
- [x] Execute "etl.py"is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.     
