
import argparse
import re
import subprocess
import time 
import datetime 
  

# argument parsing
parser = argparse.ArgumentParser(description="Make some samples (names of people).")
parser.add_argument("logfile", type=str, help="celery log file")
args = parser.parse_args()

pre_lines = subprocess.check_output(f"grep \"Executing step 'null_step'\" {args.logfile}", shell=True).decode('ascii')
post_lines = subprocess.check_output(f"grep \"Execution returned status OK\" {args.logfile}", shell=True).decode('ascii')

pre_list = pre_lines.strip().split("\n")
post_list = post_lines.strip().split("\n")
#print(pre_list)
#print(post_list)

pre_stamps = []
for line in pre_list:
    matches = re.search(r"\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d", line)
    if matches:
        pre_stamps.append(matches.group(0))

post_stamps = []
for line in post_list:
    matches = re.search(r"\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d", line)
    if matches:
        post_stamps.append(matches.group(0))

sample_times = zip(pre_stamps, post_stamps)

for sample in sample_times:
    #print(sample)
    pre = sample[0]
    post = sample[1]
    pre_element = datetime.datetime.strptime(pre,"%Y-%m-%d %H:%M:%S,%f") 
    pre_timestamp = datetime.datetime.timestamp(pre_element) 
    post_element = datetime.datetime.strptime(post,"%Y-%m-%d %H:%M:%S,%f") 
    post_timestamp = datetime.datetime.timestamp(post_element) 
    #print(pre_timestamp) 
    #print(post_timestamp) 
    print(round(post_timestamp - pre_timestamp, 3)) 

