import argparse
import datetime
import glob
import os
import re
import subprocess
import sys


SAMPLES = [1, 10, 100, 1000, 10000, 100000]

# argument parsing
parser = argparse.ArgumentParser(description="Make some samples (names of people).")
parser.add_argument("output_path", type=str, help="path to original output")
parser.add_argument("split_data_path", type=str, help="path to split data")
parser.add_argument("c", type=int, help="concurrency")
args = parser.parse_args()

filled_c = str(args.c).zfill(3)

logpattern = os.path.join(args.split_data_path, "split_log_*")
errpattern = os.path.join(args.output_path, "*.err")
args.logfile = glob.glob(logpattern)
args.errfile = glob.glob(errpattern)

if len(args.logfile) == 0:
    print(f"{logpattern} returned no glob matches!")
    sys.exit()
if len(args.errfile) == 0:
    print(f"{errpattern} returned no glob matches!")
    sys.exit()

logmap = {}
i = 1
for log in args.logfile:
    if i == 1000000:
        break
    logmap[i] = log
    i *= 10


def single_task_times():
    for k, v in logmap.items():
        task_durations = []
        try:
            pre_lines = subprocess.check_output(f'grep " succeeded in " {v}', shell=True).decode("ascii")

            pre_list = pre_lines.strip().split("\n")

            for line in pre_list:
                matches = re.search(r"\d+.\d+s:", line)
                if matches:
                    match = matches.group(0)
                    match = float(match.strip("s:"))
                    task_durations.append(match)
        except Exception:
            print(f"c{filled_c}_s{k} task times : ERROR")
            continue

        print(f"c{filled_c}_s{k} task times : " + str(task_durations))


def merlin_run_time():
    total = 0
    for err in args.errfile:
        pre_line = subprocess.check_output(f'grep "real" {err}', shell=True).decode("ascii")
        pre_line = pre_line.strip()
        matches = re.search(r"\d\.\d\d\d", pre_line)
        match = matches[0]
        result = float(match)
        total += result
    try:
        print(f"c{filled_c} merlin run : " + str(result))
    except Exception:
        result = None
        print(f"c{filled_c} merlin run : ERROR -- result={result}, args.errfile={args.errfile}")


def start_verify_time():
    for k, v in logmap.items():
        all_timestamps = []
        try:
            pre_line = subprocess.check_output(f'grep -m2 "verify" {v} | tail -n1', shell=True).decode("ascii")
            pre_line = pre_line.strip()
            matches = re.search(r"\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d", pre_line)
            match = matches[0]
            element = datetime.datetime.strptime(match, "%Y-%m-%d %H:%M:%S,%f")
            timestamp = datetime.datetime.timestamp(element)
            all_timestamps.append(timestamp)
        except Exception:
            print(f"c{filled_c}_s{k} start verify : ERROR")
            continue
        try:
            print(f"c{filled_c}_s{k} start verify : " + str(all_timestamps[0]))
        except Exception:
            print(f"c{filled_c}_s{k} start verify : ERROR")


def start_run_workers_time():
    for k, v in logmap.items():
        all_timestamps = []
        try:
            pre_line = subprocess.check_output(f'grep -m1 "" {v}', shell=True).decode("ascii")
            pre_line = pre_line.strip()
            matches = re.search(r"\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d", pre_line)
            match = matches[0]
            element = datetime.datetime.strptime(match, "%Y-%m-%d %H:%M:%S,%f")
            timestamp = datetime.datetime.timestamp(element)
            all_timestamps.append(timestamp)
        except Exception:
            continue
        earliest = min(all_timestamps)
        print(f"c{filled_c}_s{k} start run-workers : " + str(earliest))


def start_sample1_time():
    for k, v in logmap.items():
        all_timestamps = []
        try:
            pre_line = subprocess.check_output(f"grep -m1 \"Executing step 'null_step'\" {v}", shell=True).decode("ascii")
            pre_line = pre_line.strip()
            matches = re.search(r"\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d", pre_line)
            match = matches[0]
            element = datetime.datetime.strptime(match, "%Y-%m-%d %H:%M:%S,%f")
            timestamp = datetime.datetime.timestamp(element)
            all_timestamps.append(timestamp)
        except Exception:
            print(f"c{filled_c}_s{k} start samp1 : ERROR")
            continue
        earliest = min(all_timestamps)
        print(f"c{filled_c}_s{k} start samp1 : " + str(earliest))


def main():
    single_task_times()
    merlin_run_time()
    start_verify_time()
    start_run_workers_time()
    start_sample1_time()


if __name__ == "__main__":
    main()
