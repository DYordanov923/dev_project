

import threading
import Queue
from pathlib import Path
import pandas as pd
import scipy.misc
from PIL import ImageFile, Image

lock = threading.Lock()


def create_df():
	col_names =  ['ID', 'Label', 'Last_label','Duration','Iteration', 'Time_stamp']
	my_df  = pd.DataFrame(columns = col_names)
	return my_df


def load_df(path):
	df = pd.read_csv(path, index_col=False)
	return df


def save_df(df, path):
	df.to_csv(path, index=False)




def df_add_row(size, img_id, time_st, my_df, iteration, VALID_RESOLUTIONS):
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
	my_df.loc[len(my_df)] = [img_id, label, last_label, duration, iteration, time_st]
        return duration

def df_update_row(size, img_id, time_st, my_df, iteration, VALID_RESOLUTIONS):
	
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
	my_df.loc[my_df['ID'] == img_id, ['Iteration']] = iteration
	my_df.loc[my_df['ID'] == img_id, ['Time_stamp']] = time_st
        return duration



def store_log_info(img_id, size, time_st, path, VALID_RESOLUTIONS, my_df, iteration):
        lock.acquire()
#	file_ = Path(path)
#	rnd = ""
#        iteration = ""
#	if file_.exists():
#	   my_df = load_df(path)
#	   rnd = 1 
#	   iteration = my_df["Iteration"].iloc[-1] + 1
#	else:
#	   my_df=create_df()
#	   rnd = 0
#	   iteration = 0
#	print(rnd)

#	current_ids = []
	flag = any(my_df.ID == img_id)
    #rnd = df.loc[df['ID'] == img_id, 'iteration']
	if flag:
        	duration = df_update_row(size, img_id, time_st, my_df, iteration, VALID_RESOLUTIONS)
#	   print("update")

	else:

        	duration = df_add_row(size, img_id, time_st, my_df, iteration, VALID_RESOLUTIONS)
#	   print("add")
	save_df(my_df, path)
        lock.release()
        return duration
