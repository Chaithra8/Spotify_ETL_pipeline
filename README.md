# Spotify_ETL_pipeline

The objective of this project is to build automated ETL pipeline for capturing Spotify palylists data, transform and store in Snowflake.
Below is teh architecture of the entire project:
![alt text](https://github.com/Chaithra8/Spotify_ETL_pipeline/blob/main/Spotify_ETL_Architecture.png)

AWS is chosen as the main cloud platform, services used to accomplish this project are:
  - Spotify API
  - AWS IAM
  - S3 bucket
  - AWS Lambda
  - AWS Glue (Pyspark)
  - Amazon EventBridge
  - AWS Cloudwatch
  - Amazon SQS (S3 event notification)
  - Snowflake
  - Snowpipe

Project workflow is mentioned below:
1. Spotify playlist data is extracted using AWS lamdba function via spotify API. This function is scheduled to run every day using EventBridge.
2. Extracted data is stored in S3 bucket(raw_data) as json file.
3. Upon loading the raw files to S3 another lamdba function is triggered.
4. This lamdba function starts glue jobs written in PySpark.
5. Glue job contains logic to transform raw data, generate 3 parquet files (album, artist and songs data) and store them to another S3 folder(transformed_data/) under separate sub-folder for each file. Once the transformation is completed, it moves the raw data(.json) from "raw_data/to_process" folder to "raw_data/processed" folder of S3 bucket to avoid re-computation of same files.
6. Once the files are uploaded to "transformed" folder of S3 bucket, Snowpipe is triggered via SQS queue.
7. S3 data is referenced using snowflake's external stages created for each sub-folder(album, artist, song) present under "transformed_data/" folder and 3 snowpipes are created to copy data from external stage to snowflake table.
9. Upon trigger, Snowpipe automatically loads transformed data from S3 to Snowflake tables.

**For more detailed steps, please go through below link - **

[Link to my medium blog](https://medium.com/@spchaithrajp/spotify-etl-pipeline-aws-pyspark-snowflake-46c25fb69f95)
