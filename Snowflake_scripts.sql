-------------------------------------------------------------------------------------------
-- Step 1: To start, let's set the Role and Warehouse context
    -- USE ROLE: https://docs.snowflake.com/en/sql-reference/sql/use-role
    -- USE WAREHOUSE: https://docs.snowflake.com/en/sql-reference/sql/use-warehouse
-------------------------------------------------------------------------------------------

---> set the Role
USE ROLE accountadmin;

---> set the Warehouse
USE WAREHOUSE compute_wh;

-------------------------------------------------------------------------------------------
-- Step 2: With context in place, let's now create a Database, Schema, and Table
    -- CREATE DATABASE: https://docs.snowflake.com/en/sql-reference/sql/create-database
    -- CREATE SCHEMA: https://docs.snowflake.com/en/sql-reference/sql/create-schema
    -- CREATE TABLE: https://docs.snowflake.com/en/sql-reference/sql/create-table
-------------------------------------------------------------------------------------------

---> create the Spotify Database
CREATE OR REPLACE DATABASE spotify_db;

---> create the staging Schema
CREATE OR REPLACE SCHEMA spotify_db.staging;

---> create the Album Table
CREATE OR REPLACE TABLE spotify_db.staging.album_table (
    album_id VARCHAR(50),
    album_name VARCHAR(100),
    release_date DATE,
    total_tracks INT,
    url VARCHAR(255),
    album_type VARCHAR(255),
    pipe_loaded_ts TIMESTAMP
);

---> create the Artist Table
CREATE OR REPLACE TABLE spotify_db.staging.artist_table (
    artist_id VARCHAR(255),
    artist_name VARCHAR(255),
    external_url VARCHAR(255),
    pipe_loaded_ts TIMESTAMP
);

---> create the Songs Table
CREATE OR REPLACE TABLE spotify_db.staging.songs_table (
    song_id VARCHAR(50),
    song_name VARCHAR(255),
    duration_ms NUMBER,
    url VARCHAR(255),
    popularity VARCHAR(255),
    song_added DATE,
    album_id VARCHAR(50),
    artist_id VARCHAR(50),
    pipe_loaded_ts TIMESTAMP
);


-------------------------------------------------------------------------------------------
-- Step 3: To create storage integration for aws s3
    -- create file format
-------------------------------------------------------------------------------------------

--> create storage integration for aws
create or replace storage integration spotify_s3_integration
    type = external_stage
    storage_provider = s3
    enabled = true
    storage_aws_role_arn = "<aws_arn>" -- provide AWS ARN present in the role for snowflake in AWS
    storage_allowed_locations = ('s3://spotify-streaming-etl-pipeline/')
    comment = 'Creating connection to S3'

--> to see content of integration
--> Copy "STORAGE_AWS_IAM_USER_ARN" and "STORAGE_AWS_EXTERNAL_ID" value, inorder to mention in Tust relationship of snowflake role in aws
desc integration spotify_s3_integration;

--> create file format
CREATE OR REPLACE FILE FORMAT s3_parquet_format
TYPE = 'PARQUET'

-------------------------------------------------------------------------------------------
-- Step 4: To connect to S3, let's create a exteranal stage
    -- Creating an S3 Stage: https://docs.snowflake.com/en/user-guide/data-load-s3-create-stage
-------------------------------------------------------------------------------------------

--> Create stage for album data in S3
CREATE OR REPLACE STAGE spotify_db.staging.aws_album_stage 
    URL = 's3://spotify-streaming-etl-pipeline/transformed_data/album_data/' 
    STORAGE_INTEGRATION = spotify_s3_integration
    FILE_FORMAT = s3_parquet_format;

--> query the stage
LIST @spotify_db.staging.aws_album_stage ;


--> Create stage for artist data in S3
CREATE OR REPLACE STAGE spotify_db.staging.aws_artist_stage 
    URL = 's3://spotify-streaming-etl-pipeline/transformed_data/artist_data/' 
    STORAGE_INTEGRATION = spotify_s3_integration
    FILE_FORMAT = s3_parquet_format;

LIST @spotify_db.staging.aws_artist_stage;

    
--> Create stage for songs data in S3
CREATE OR REPLACE STAGE spotify_db.staging.aws_songs_stage
    URL = 's3://spotify-streaming-etl-pipeline/transformed_data/songs_data/' 
    STORAGE_INTEGRATION = spotify_s3_integration
    FILE_FORMAT = s3_parquet_format;

LIST @spotify_db.staging.aws_songs_stage;


-------------------------------------------------------------------------------------------
-- Step 5: Now let's Snowpipe which automates loading of data from eternal stage(S3) to snowflake tales 
-------------------------------------------------------------------------------------------
create or replace schema pipe;

-- 1. Album
create or replace pipe spotify_db.pipe.album_jobs_pipe
auto_ingest = true 
as
COPY INTO spotify_db.staging.album_table FROM(
select
        $1:album_id::VARCHAR(50) AS album_id,
        $1:album_name::VARCHAR(100) AS album_name,
        $1:album_release_date::DATE AS release_date,
        $1:album_total_tracks::INT AS total_tracks,
        $1:album_url::VARCHAR(255) AS url,
        $1:album_type::VARCHAR(255) AS album_type,
        CURRENT_TIMESTAMP() AS pipe_loaded_ts
from
@spotify_db.staging.aws_album_stage
)
FILE_FORMAT = (FORMAT_NAME = 's3_parquet_format')
ON_ERROR = 'SKIP_FILE';

-- To see content of snowpipe
-- Copy "notification_channel" value to mention in AWS's SQS queue ARN (S3's event notification)
-- SQS notifies snowpipe whenever a object is added to specific S3 folder 
DESC PIPE spotify_db.pipe.album_jobs_pipe;

-- select * from spotify_db.staging.album_table;
-------------------------------------
--- 2. Artist

create or replace pipe spotify_db.pipe.artist_jobs_pipe
auto_ingest = true 
as
COPY INTO spotify_db.staging.artist_table FROM(
select
        $1:artist_id::VARCHAR(255) AS artist_id,
        $1:artist_name::VARCHAR(255) AS artist_name,
        $1:artist_url::VARCHAR(255) AS external_url,
        CURRENT_TIMESTAMP() AS pipe_loaded_ts
from
@spotify_db.staging.aws_artist_stage
)
FILE_FORMAT = (FORMAT_NAME = 's3_parquet_format')
ON_ERROR = 'SKIP_FILE';

-- Copy "notification_channel" value to mention in AWS's SQS queue ARN (S3's event notification)
DESC PIPE spotify_db.pipe.artist_jobs_pipe;

-- select * from spotify_db.staging.album_table;

-------------------------------------
--3. Songs

create or replace pipe spotify_db.pipe.songs_jobs_pipe
auto_ingest = true 
as
COPY INTO spotify_db.staging.songs_table FROM(
select
        $1:song_id::VARCHAR(50) AS song_id,
        $1:song_name::VARCHAR(255) AS song_name,
        $1:song_duration::NUMBER AS duration_ms,
        $1:song_url::VARCHAR(255) AS url,
        $1:song_popularity::VARCHAR(255) AS popularity,
        $1:song_added::DATE AS song_added,
        $1:album_id::VARCHAR(50) AS album_id,
        $1:artist_id::VARCHAR(50) AS artist_id,
        CURRENT_TIMESTAMP() AS pipe_loaded_ts
from
@spotify_db.staging.aws_songs_stage
)
FILE_FORMAT = (FORMAT_NAME = 's3_parquet_format')
ON_ERROR = 'SKIP_FILE';

-- Copy "notification_channel" value to mention in AWS's SQS queue ARN (S3's event notification)
DESC PIPE spotify_db.pipe.songs_jobs_pipe;


-------------------------------------------------------------------------------------------
-- Step 6: Query loaded table data
-------------------------------------------------------------------------------------------

select * from spotify_db.staging.album_table;
select * from spotify_db.staging.artist_table;
select * from spotify_db.staging.songs_table;
