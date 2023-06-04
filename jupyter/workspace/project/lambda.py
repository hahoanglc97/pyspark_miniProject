import json
import boto3

glue = boto3.client('glue')
def lambda_handler(event, context):
    # name like glue job name
    gluejobname="glueCDC-pyspark"
    
    # give glue information about bucket name and file name need change
    bucketName = event["Records"][0]["s3"]["bucket"]["name"]
    fileName = event["Records"][0]["s3"]["object"]["key"]
    
    print('Lambda trigger')
    print(bucketName, fileName)
        
    try:
        response = glue.start_job_run(
            JobName = gluejobname,
            Arguments = {
                '--s3_target_path_key': fileName,
                '--s3_target_path_bucket': bucketName
            } 
        )
        
    except Exception as e:
        print(e)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
