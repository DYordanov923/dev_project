
import Queue
import boto
import boto3
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from PIL import ImageFile
import time
import datetime
from time import mktime
from datetime import datetime
import time
import datetime
from time import mktime
from datetime import datetime


s3 = boto.connect_s3()
bucket = s3.get_bucket('ops-sofia-dev')

print("loop")
for key in bucket.list():
    print(key.name.encode('utf-8'))
    dt = time.strptime(key.last_modified[:19], "%Y-%m-%dT%H:%M:%S")
    dt2 = datetime.fromtimestamp(mktime(dt))
    ts = time.mktime(dt2.timetuple())
    print("v2-"+str(dt2))
    print('v3-' + str(ts))
print("done")

