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



k = Key()

k.key = 'myfile'
k.set_contents_from_filename('/home/ubuntu/environments/img_dir2/a0.jpeg')

print("uploaded")

def get_image_resolution(s3, bucket=None, key=None):
    end = 1024
    parser = ImageFile.Parser()
    chunk = s3.get_object(Bucket=bucket, Key=key, Range='bytes={}-{}'.format(0, end))
    print("done")
    while chunk:
        parser.feed(chunk["Body"].read())
        if parser.image:
            break
        end += 1024
        time.sleep(0.1)
        chunk = s3.get_object(Bucket=mybucket, Key=mykey, Range='bytes={}-{}'.format(0, end))
    return parser.image.size


session = boto3.session.Session()
client = session.client('s3')
mykey = 'myfile'
mybucket ='ops-sofia-dev'
size = get_image_resolution(client, bucket=mybucket, key=mykey)
print(str(size))
input_queue = Queue.Queue()

bucket = s3.get_bucket('ops-sofia-dev')
images = [i.name for i in bucket.list()]
print(images)
for image in images:
        input_queue.put({
            'image': image,
})

print('queue:')
while not input_queue.empty():
          job = input_queue.get(True, 1)
          image = job["image"]
          print(image)
          size = get_image_resolution(client, bucket=mybucket, key=image)
          print(str(size))
