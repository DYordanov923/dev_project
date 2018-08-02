

import boto
import boto3
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from PIL import ImageFile
import time
import datetime
from time import mktime
from datetime import datetime

def get_bucket(bucket_name):
    aws_connection = S3Connection()
    return aws_connection.get_bucket(bucket_name)


session = boto3.session.Session(profile_name=profile or None)
client = session.client('s3', region_name ='us-east-1')
bucket_name ='cn-saved-logs'
bucket = get_bucket(bucket_name)


print("loop")
for key in bucket.list():
    print(key.name.encode('utf-8'))

print('done')
