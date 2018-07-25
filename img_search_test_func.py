from PIL import Image
import glob
import os
import time
import numpy as np
from datetime import datetime
import random
import pandas as pd
import scipy.misc


def auto_resize_img(path):

	list_images= [filename for filename in glob.glob(path+'*.jpeg')]
	random_img = random.choice(list_images)
	random_img_raw = Image.open(random_img)
	width, height = random_img_raw.size

	if (width!=1920 and height !=1080):
		new_img = random_img_raw.resize((1920,1080))
	else:
		new_img = random_img_raw.resize((400,400))
	new_img.save(random_img,'jpeg')


#function to create black images. num: number of images to create, path: directory, name: prefix of the image name
def create_images(width, height, num, path, name):
	width = width
	height = height
	channels = 3

	for i in range(num):
		img = np.zeros((height, width, channels), dtype=np.uint8)
		scipy.misc.imsave(str(path)+str(name)+str(i)+".jpeg", img)
