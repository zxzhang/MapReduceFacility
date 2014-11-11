import os 
import shutil

dirpath = "/tmp/localhost:8888/input/"
if os.path.exists(dirpath):
    shutil.rmtree(dirpath)

os.makedirs(dirpath)

shutil.copy("apple_data.txt_aa", dirpath + "apple_data.txt_aa")
shutil.copy("apple_data.txt_ab", dirpath + "apple_data.txt_ab")
