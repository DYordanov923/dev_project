import datetime
from time import mktime
from datetime import datetime

import time
import threading
import Queue
from pathlib import Path
import pandas as pd
import scipy.misc
from PIL import ImageFile, Image

lock = threading.Lock()


def create_df():
	col_names =  ['ID', 'Label', 'Last_label','Duration','Iteration', 'Time_stamp', 'Time_stamp_seconds']
	my_df  = pd.DataFrame(columns = col_names)
	return my_df


def load_df(path):
	df = pd.read_csv(path, index_col=False)
	return df


def save_df(df, path):
	df.to_csv(path, index=False)




def df_add_row(size, img_id, ts_seconds, ts_raw, my_df, iteration, VALID_RESOLUTIONS):
	
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
	my_df.loc[len(my_df)] = [img_id, label, last_label, duration, iteration, ts_raw ,ts_seconds]
        return duration

def df_update_row(size, img_id, ts_seconds,ts_raw, my_df, iteration, VALID_RESOLUTIONS):

	df_2 = my_df[my_df['Iteration'] == (iteration-1)]
 #       print(df_2)
	# assign label based on image resolution, last_label - the label in previous iteration, duration and time_stamp
	# if last_label is different then calculate how long the image was with label = 1
	if size not in VALID_RESOLUTIONS:

		label = 1
		last_label =int(df_2.loc[df_2['ID'] == img_id, 'Label'])
		duration = 'ongoing'
		
#                print("---", last_label)
	else:

		label = 0
		last_label = int(df_2.loc[df_2['ID'] == img_id, 'Label'])
#		print("----", last_label)
		if int(label) != int(last_label):
			last_time_st = int(df_2.loc[df_2['ID'] == img_id, 'Time_stamp_seconds'])
			duration = int(ts_seconds - last_time_st)
			#print(duration)
		else:
			duration="NA"

#       my_df.loc[len(my_df)] = [img_id, label, last_label, duration, iteration, time_st]
	#add values to the dataframe
#	print(img_id+" label: "+ str(label))
       
        my_df.loc[len(my_df)] = [img_id, label, last_label, duration, iteration, ts_raw, ts_seconds]
#	my_df.loc[my_df['ID'] == img_id, ['Label']] = label
#	my_df.loc[my_df['ID'] == img_id, ['Last_label']] = last_label
#	my_df.loc[my_df['ID'] == img_id, ['Duration']] = duration
#	my_df.loc[my_df['ID'] == img_id, ['Iteration']] = iteration
#	my_df.loc[my_df['ID'] == img_id, ['Time_stamp']] = time_st
        return duration



def convert_time_stamp(time_st):
       dt = time.strptime(time_st, "%Y-%m-%dT%H:%M:%S")
       dt2 = datetime.fromtimestamp(mktime(dt))
       ts_seconds = time.mktime(dt2.timetuple())
       ts_raw = time_st
       return ts_seconds, ts_raw




def store_log_info(img_id, size, time_st, path, VALID_RESOLUTIONS, my_df, iteration):
        lock.acquire()

        ts_seconds, ts_raw = convert_time_stamp(time_st)
 #       print(ts_raw)
	flag = any(my_df.ID == img_id)
 #       print("iter: -- ",iteration)
        df_range = my_df[my_df['Iteration'].between((iteration-4), iteration, inclusive=True)]
        df_4 = df_range[df_range['ID'] == img_id]

    #rnd = df.loc[df['ID'] == img_id, 'iteration']
	if flag:
        	duration = df_update_row(size, img_id, ts_seconds, ts_raw, my_df, iteration, VALID_RESOLUTIONS)
#	   print("update")

	else:

        	duration = df_add_row(size, img_id, ts_seconds, ts_raw, my_df, iteration, VALID_RESOLUTIONS)
#	   print("add")
	save_df(my_df, path)
#        df_range = my_df[my_df['Iteration'].between(0, 4, inclusive=True)]
#        df_4 = df_range[df_range['ID'] == img_id]
       
        lock.release()
        return df_4
