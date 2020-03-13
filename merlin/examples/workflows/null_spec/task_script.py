
import argparse
import re
import subprocess
import time 
import datetime 
import os
  

# argument parsing
parser = argparse.ArgumentParser(description="Make some samples (names of people).")
parser.add_argument("logfile", type=str, help="celery log file")
args = parser.parse_args()

pre_lines = subprocess.check_output(f"grep \" succeeded in \" {args.logfile}", shell=True).decode('ascii')

pre_list = pre_lines.strip().split("\n")

task_durations = []
for line in pre_list:
    matches = re.search(r"\d+.\d+s:", line)
    if matches:
        match = matches.group(0)
        match = float(match.strip("s:"))
        task_durations.append(match)

print(task_durations)
