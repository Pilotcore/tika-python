import tika
import os
import boto3
import urllib
from tika import parser
from botocore.exceptions import ClientError
tika.initVM()

def read_to_tmp(bucket_name, s3_key, aws_region=None):
    file_name = os.path.join('/tmp', os.path.basename(s3_key))
    s3 = boto3.resource('s3', aws_region)
    obj = s3.Object(bucket_name, s3_key)
    try:
        obj.download_file(file_name)
    except ClientError as e:
        print("Received error: %s", e)
        print("e.response['Error']['Code']: %s", e.response['Error']['Code'])
        return None
    return file_name

def handler(event, context):
    results = []
    print("Record: {}".format(event['Records']))
    print("Context: {}".format(context))
    object_name = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    aws_region = event['Records'][0]['awsRegion']
    local_tmp_file = read_to_tmp(bucket_name, object_name, aws_region)
    if local_tmp_file:
        parsed = parser.from_file(local_tmp_file)
        metadata = parsed["metadata"]
        content = parsed.get("content", "")
        results.append({ "metadata" : metadata, "content" : content})
    else:
        print('Error fetching file from S3.')
    return {"msg": results}
