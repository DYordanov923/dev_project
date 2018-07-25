from img_search_func import scan
from img_search_test_func import create_images


path="/home/danny/enviroments/img_dir2/"
create_images(1920,1080,4,path,"b")
create_images(400,400,4,path,"a")
scan(path,False)