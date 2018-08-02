import time
import Queue
import datetime
import os
import boto
import boto3
import random
import time
import datetime
import numpy as np
from time import mktime
from datetime import datetime
import glob
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from PIL import ImageFile, Image
import scipy.misc

session = boto3.session.Session()
client = session.client('s3')

s3 = boto.connect_s3()
bucket = s3.get_bucket('ops-sofia-dev')

input_queue= Queue.Queue()

VALID_RESOLUTIONS = [
	(1920, 1080),
	(1280, 720),
	(720, 480),
	(704, 480),
]

def is_not_valid_resolution(width, height):

	res = (width,height)
	print("resolution: "+ str(res))

	if res not in VALID_RESOLUTIONS:
		flag = True
	else:
		flag = False
	return flag



def auto_resize_img(path):

	list_images= [filename for filename in glob.glob(path+'*.jpg')]
	random_img = random.choice(list_images)
	random_img_raw = Image.open(random_img)
	width, height = random_img_raw.size

	if is_not_valid_resolution(width,height):
		new_img = random_img_raw.resize((1920,1080))
	else:
		new_img = random_img_raw.resize((400,400))
	new_img.save(random_img,'jpeg')



def upload_to_S3():


    def enqueue_images(path):
        images = [filename for filename in glob.glob(path+'*.jpg')]
        for image in images:
                input_queue.put({
                        'image': image,
        })
        return input_queue

    path = '/home/ubuntu/environments/img_dir4/'
    i_q = enqueue_images(path)

    print('upload__')
    while not i_q.empty():
        job = i_q.get(True, 1)
        image = job["image"]
        
        print(image)
        mykey = os.path.basename(image)
        k = Key(bucket)
        k.key = mykey
        k.set_contents_from_filename(image)



#upload_to_S3()


names= ['ccr-1-10_FBNHD.jpg',
		'ccr-1-12_CNBCHD.jpg',
		'ccr-1-13_WEATHHD.jpg',

		'ccr-1-10_FBNNN.jpg',
		'ccr-1-12_CNDDDD.jpg',
		'ccr-1-13_WEATTHD.jpg',

		'ccr-1-10_FBNBD.jpg',
		'ccr-1-12_CNBCCD.jpg']

#path = '/home/ubuntu/environments/img_dir4/'

def create_images_v2(width, height, path, name):
	width = width
	height = height
	channels = 3
	path = path + name
	img = np.zeros((height, width, channels), dtype=np.uint8)
	scipy.misc.imsave(path, img)


def build_img():
	cnt = 0
	for name in names:
		if cnt < 4:
		   width, height = 1920, 1080
		else:
			width, height = 400, 400
		create_images_v2(width, height, path, name)
		cnt = cnt + 1
	print("Done")




#build_img()
upload_to_S3()
