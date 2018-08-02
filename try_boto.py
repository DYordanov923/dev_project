
import boto
from boto.s3.connection import S3Connection
import boto
import boto3
from boto.s3.connection import S3Connection
from boto.s3.key import Key


s3 = boto.connect_s3()  
buckets = s3.get_all_buckets() 
for key in buckets:
    print(key.name)


s3 = boto.connect_s3()
bucket = s3.get_bucket('ops-sofia-dev')
#full_path ="/home/ubuntu/environments/img_dir4/" 




print("loop")
for key in bucket.list():
#    p = full_path+key.name
    print(key.name)
#    key.get_contents_to_filename(p)    
print("done")

