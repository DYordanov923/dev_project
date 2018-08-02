from dev_image import store_log_info, load_df
from upload_img import auto_resize_img, upload_to_S3
from download_image import download_from_S3
import time
import Queue
import datetime
import os
import boto
import boto3
import time
import datetime
from time import mktime
from datetime import datetime
import glob
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from PIL import ImageFile, Image


session = boto3.session.Session()
client = session.client('s3')

s3 = boto.connect_s3()
bucket = s3.get_bucket('ops-sofia-dev')


print("done")


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


def create_S3_queue(bucket):
   input_queue = Queue.Queue()
   images = [i.name for i in bucket.list()]
   img_ts = []
   for key in bucket.list():
        dt = time.strptime(key.last_modified[:19], "%Y-%m-%dT%H:%M:%S")
        dt2 = datetime.fromtimestamp(mktime(dt))
        ts = time.mktime(dt2.timetuple())
        img_ts.append(ts)
   for image,ts in zip(images,img_ts):
        input_queue.put({
            'image': image,
            'time_st':ts,
   })
   return input_queue

#input_queue = create_S3_queue(bucket)
mybucket = 'ops-sofia-dev'
#print('queue:')
#while not input_queue.empty():
#          job = input_queue.get(True, 1)
#          image = job["image"]
#          time_st = job["time_st"]
#          print(image, time_st)
#          mykey = image
#          size = get_image_resolution(client, bucket=mybucket, key=mykey)
#          print(str(size))


path = '/home/ubuntu/environments/img_dir2/log_dataframe.csv'
path_2 = '/home/ubuntu/environments/img_dir4/'

def write_log():
	rnd = 0
	while True:
		print("------------\n")
		input_queue = create_S3_queue(bucket)
		while not input_queue.empty():
			job = input_queue.get(True, 1)
			image = job["image"]
                        time_st = job["time_st"]
                        img_id = image
                        mykey = image 
                        size = get_image_resolution(client, bucket=mybucket, key=mykey)
		#	print(img_id, size, time_st)
			
			duration = store_log_info(img_id, size, time_st, path)
                        print(image+" duration " + str(duration))
		log_df = load_df(path)
		print(log_df)
		rnd+=1
		#wait 4s for next iteratration
		time.sleep(4)
                if rnd %4 ==0:
                        download_from_S3()
			print("\n   Resize...")
			for i in range(4):
           
				auto_resize_img(path_2)
                        upload_to_S3()


write_log()
print("DONE!!!!")
