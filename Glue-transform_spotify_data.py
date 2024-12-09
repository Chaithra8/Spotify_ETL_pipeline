import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
BUCKET = "spotify-streaming-etl-pipeline"
S3_SOURCE_PATH = "s3://spotify-streaming-etl-pipeline/raw_data/to_process/"
S3_TARGET_PATH = "s3://spotify-streaming-etl-pipeline/transformed_data/"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

raw_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [S3_SOURCE_PATH]
    }
)

# Read raw spotify playlist stored in S3
raw_df = raw_dyf.toDF()

# Explode nested items structure into multiple rows
df_exploded = raw_df.withColumn("items", explode(col("items"))).cache()

# ----------------------------------------------------------------------------------------------------------------------

# 1. To fetch list of albums

#  Extract album information from each item
df_album = df_exploded.select(
    col("items.track.album.id").alias("album_id"),
    col("items.track.album.name").alias("album_name"),
    col("items.track.album.release_date").alias("album_release_date"),
    col("items.track.album.total_tracks").alias("album_total_tracks"),
    col("items.track.album.external_urls.spotify").alias("album_url"),
    col("items.track.album.album_type").alias("album_type")
).dropDuplicates(["album_id"])

# If the 'release_date' is a 4-digit year, append '-01-01' to make it a valid date
df_album_final = df_album.withColumn(
    'album_release_date',
    when(
        length(df_album['album_release_date']) == 4,  # Check if it's just a year
        to_date(concat(df_album['album_release_date'], lit('-01-01')), 'yyyy-MM-dd')  # Append '-01-01' and convert to date
    ).otherwise(
        to_date(df_album['album_release_date'], 'yyyy-MM-dd')  # If it's already a full date, just convert it
    )
)

# Load album list to S3
df_album_final.write.mode("append").parquet(S3_TARGET_PATH+"album_data/")

# ----------------------------------------------------------------------------------------------------------------------

# 2. To fetch list of artists
# Extracting artists data from each item
df_artists_ele = df_exploded.select(col("items.track.artists")).alias("artists")

# Exploding artists array to flatten it further
df_artists_exploded = df_artists_ele.withColumn("artist", explode(col("artists")))

# Extracting artist details
df_artist_final = df_artists_exploded.select(
    col("artist.id").alias("artist_id"),
    col("artist.name").alias("artist_name"),
    col("artist.external_urls.spotify").alias("artist_url")
).dropDuplicates(["artist_id"])

# Load artist list to S3
df_artist_final.write.mode("append").parquet(S3_TARGET_PATH+"artist_data/")

# ----------------------------------------------------------------------------------------------------------------------

# 3. To fetch list of songs 

df_songs = df_exploded.select(
    col("items.track.id").alias("song_id"),
    col("items.track.name").alias("song_name"),
    col("items.track.duration_ms").alias("song_duration"),
    col("items.track.external_urls.spotify").alias("song_url"),
    col("items.track.popularity").alias("song_popularity"),
    col("items.added_at").alias("song_added"),
    col("items.track.album.id").alias("album_id"),
    col("items.track.album.artists").getItem(0).getField("id").alias("artist_id")
).dropDuplicates(["song_id"])

df_songs_final = df_songs.withColumn('song_added', to_date(col("song_added")))

# Load songs list to S3
df_songs_final.write.mode("append").parquet(S3_TARGET_PATH+"songs_data/")

job.commit()

# ----------------------------------------------------------------------------------------------------------------------
# To avoid processing same files by glue jobs processed files are moved to different folder
# Copying processed files to 'processed' folder and deleting the same in 'to_process' folder
s3 = boto3.client('s3')

for file in s3.list_objects(Bucket=BUCKET, Prefix="raw_data/to_process/")['Contents']:
    raw_file = file['Key']
    if raw_file.split('.')[-1] == "json":
        # Moving data from 'to_process' to 'processed' folder
        s3.copy_object(
            Bucket=BUCKET,
            CopySource={'Bucket': BUCKET, 'Key': raw_file},
            Key="raw_data/processed/" + raw_file.split("/")[-1]
        )
        
        # delete raw_data from 'to_process' folder
        s3.delete_object(Bucket=BUCKET, Key=raw_file)