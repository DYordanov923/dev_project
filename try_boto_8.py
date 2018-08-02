import glob
import os
import Queue
import boto
import boto3
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from PIL import ImageFile, Image
import time
import datetime
from time import mktime
from datetime import datetime

s3 = boto.connect_s3()
bucket = s3.get_bucket('ops-sofia-dev')



def get_image_resolution(s3, bucket=None, key=None):
    end = 1024
    parser = ImageFile.Parser()
    chunk = s3.get_object(Bucket=bucket, Key=key, Range='bytes={}-{}'.format(0, end))
    while chunk:
        parser.feed(chunk["Body"].read())
        if parser.image:
            break
        end += 1024
        time.sleep(0.1)
        chunk = s3.get_object(Bucket=bucket, Key=key, Range='bytes={}-{}'.format(0, end))
    return parser.image.size


session = boto3.session.Session()
client = session.client('s3')
input_queue = Queue.Queue()

bucket = s3.get_bucket('ops-sofia-dev')
images = [i.name for i in bucket.list()]
print(images)
for image in images:
        input_queue.put({
            'image': image,
})



mybucket = 'ops-sofia-dev'
print('queue:')
while not input_q<ueue.empty():
          job = input_queue.get(True, 1)
          image = job["image"]
          print(image)
          mykey = image
          size = get_image_resolution(client, bucket=mybucket, key=mykey)
          print(str(size))

def enqueue_images(path):
	images = [filename for filename in glob.glob(path+'*.jpeg')]
	for image in images:
		input_queue.put({
			'image': image,
        })
	return input_queue


def check_image(image):

	im = Image.open(image)
	width , height = im.size
	#img_id, _, _ = name.partition('.')
	size = (width, height)

	time_st = os.stat(image).st_mtime

	#print(img_id+" ----> "+ str(width), str(height) + " time_st: "+ str(time_st))

	return image, size, time_st


path = '/home/ubuntu/environments/img_dir2/'

i_q = enqueue_images(path)


print('upload__')
while not i_q.empty():
      job = i_q.get(True, 1)
      image = job["image"]
      image_path, size, time_st= check_image(image)
      print(image_path, size, time_st)
      mykey = os.path.basename(image_path)
      k = Key(bucket)
      k.key = mykey
      k.set_contents_from_filename(image_path)


for image in images:
        input_queue.put({
            'image': image,
})
print('queue:')
bucket = 'ops-sofia-dev'
while not input_queue.empty():
          job = input_queue.get(True, 1)
          image = job["image"]
          print(image)
          mykey= image
          size = get_image_resolution(client, bucket=bucket, key=mykey)
          print(str(size))


