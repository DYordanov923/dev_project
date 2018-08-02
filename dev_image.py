
from PIL import Image
import glob
import os
import time
import threading
import numpy as np
from datetime import datetime
import random
import pandas as pd
import scipy.misc
import signal
import Queue
from pathlib import Path
import boto
import boto3
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from PIL import ImageFile, Image

lock = threading.Lock()



VALID_RESOLUTIONS = [
	(1920, 1080),
	(1280, 720),
	(720, 480),
	(704, 480),
]


interrupted = False
input_queue = Queue.Queue()

#path="/home/danny/enviroments/img_dir2/"




def create_df():
	col_names =  ['ID', 'Label', 'Last_label','Duration', 'Time_stamp']
	my_df  = pd.DataFrame(columns = col_names)
	return my_df


def load_df(path):
	df = pd.read_csv(path, index_col=False)
	return df


def save_df(df, path):
	df.to_csv(path, index=False)




def df_add_row(size, img_id, time_st, my_df):
	#assing labels based on image resolution
	if size not in VALID_RESOLUTIONS:
		label = 1
		last_label = 1
		duration="ongoing"

	else:

		label = 0
		last_label = 0
		duration="NA"
#	print(img_id+" label: "+ str(label))
	my_df.loc[len(my_df)] = [img_id, label, last_label, duration, time_st]
        return duration

def df_update_row(size, img_id, time_st, my_df):
	
	# assign label based on image resolution, last_label - the label in previous iteration, duration and time_stamp
	# if last_label is different then calculate how long the image was with label = 1
	if size not in VALID_RESOLUTIONS:

		label = 1
		last_label = my_df.loc[my_df['ID'] == img_id, 'Label']
		duration = 'ongoing'
		

	else:

		label = 0
		last_label = my_df.loc[my_df['ID'] == img_id, 'Label']
	#	print(last_label)
		if int(label) != int(last_label):
			last_time_st = my_df.loc[my_df['ID'] == img_id, 'Time_stamp']
			duration = time_st - last_time_st
			#print(duration)
		else:
			duration="NA"

	#add values to the dataframe
#	print(img_id+" label: "+ str(label))
	my_df.loc[my_df['ID'] == img_id, ['Label']] = label
	my_df.loc[my_df['ID'] == img_id, ['Last_label']] = last_label
	my_df.loc[my_df['ID'] == img_id, ['Duration']] = duration
	my_df.loc[my_df['ID'] == img_id, ['Time_stamp']] = time_st
        return duration


def store_log_info(img_id, size, time_st, path):
        lock.acquire()
	file_ = Path(path)
	rnd = ""
	if file_.exists():
	   my_df = load_df(path)
	   rnd = 1
	else:
	   my_df=create_df()
	   rnd = 0
#	print(rnd)
	current_ids = []
	flag = any(my_df.ID == img_id)

	if flag:

	  duration = df_update_row(size, img_id, time_st, my_df)
#	   print("update")

	else:

	  duration = df_add_row(size, img_id, time_st, my_df)
#	   print("add")
	   

	save_df(my_df, path)
        lock.release()
        return duration
#	print("Done!")
path = "/home/ubuntu/environments/img_dir2/log_dataframe.csv"
#path="/home/danny/enviroments/img_dir2/log/log_dataframe.csv"
#file_2="/home/danny/enviroments/img_dir4/ccr-1-4_FDDDD.jpg"
#file_3="/home/danny/enviroments/img_dir4/ccr-1-10_FJJHD.jpg"
#im = Image.open("/home/danny/enviroments/img_dir4/ccr-1-4_FDDDD.jpg")
#width, height = im.size
#size = (width, height)
#name = os.path.basename(file_2)



def check_image(image):

	im = Image.open(image)
	width , height = im.size
	name = os.path.basename(image)
	#img_id, _, _ = name.partition('.')
	size = (width, height)

	time_st = os.stat(image).st_mtime

	#print(img_id+" ----> "+ str(width), str(height) + " time_st: "+ str(time_st))

	return name, size, time_st




#store_log_info(img_id, size, time_st, path)

#img_id, size, time_st = check_image(file_3)

#store_log_info(img_id, size, time_st, path)



def enqueue_images(path):
	images = [filename for filename in glob.glob(path+'*.jpeg')]
	for image in images:
		input_queue.put({
			'image': image,
	})
	return input_queue

#path1="/home/danny/enviroments/img_dir2/"
#input_queue = enqueue_images(path1)


def is_not_valid_resolution(width, height):

	res = (width,height)
#	print("resolution: "+ str(res))

	if res not in VALID_RESOLUTIONS:
		flag = True
	else:
		flag = False
	return flag


def auto_resize_img(path):

	list_images= [filename for filename in glob.glob(path+'*.jpeg')]
	random_img = random.choice(list_images)
	random_img_raw = Image.open(random_img)
	width, height = random_img_raw.size

	if is_not_valid_resolution(width,height):
		new_img = random_img_raw.resize((1920,1080))
	else:
		new_img = random_img_raw.resize((400,400))
	new_img.save(random_img,'jpeg')


#write_log(input_queue)
###########################################################################
