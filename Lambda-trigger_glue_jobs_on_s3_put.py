import boto3

glue_client = boto3.client('glue')

def lambda_handler(event, context):
    # Trigger Glue job when a new file is uploaded to S3
    response = glue_client.start_job_run(
        JobName='transform_spotify_data'
    )
    
    return {
        'statusCode': 200,
        'body': f'Glue Job started: {response["JobRunId"]}'
    }
