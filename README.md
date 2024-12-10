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
  - S3 event notification (SQS Queue)
  - Snowflake
  - Snowpipe

Project workflow is mentioned below:
1. Spotify playlist data is extracted using AWS lamdba function via spotify API. This function is scheduled to run every day using EventBrige.
2. Extracted data is stored in S3 bucket(raw_data)
3. Upon loading the raw files to S3 another lamdba function is triggered.
4. This lamdba function starts glue jobs written with the help of PySpark.
5. Glue job contains logic to transform raw data, generate 3 parquet files (album, artist and songs data) and store it onto another S3 folder(transformed_data). Once the transformation is completed it moves the raw data from "raw_data/to_process" folder to "raw_data/processed" folder of S3 bucket to avoid re-computation of same files.
6. Once the files are uploaded to "transformed" folder of S3 bucketSnowpipe.
7. Snowpipe loads transformed data from S3 to Snowflake tables.
