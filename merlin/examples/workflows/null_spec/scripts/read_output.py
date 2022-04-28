import argparse
import datetime
import glob
import os
import re
import subprocess
import sys


# argument parsing
parser = argparse.ArgumentParser(description="Make some samples (names of people).")
parser.add_argument("path", type=str, help="path to spec output")
parser.add_argument("c", type=int, help="concurrency")
parser.add_argument("s", type=int, help="n of samples")
args = parser.parse_args()

logpattern = os.path.join(args.path, "*.log")
errpattern = os.path.join(args.path, "*.err")
args.logfile = glob.glob(logpattern)
args.errfile = glob.glob(errpattern)

if len(args.logfile) == 0:
    print(f"{logpattern} returned no glob matches!")
    sys.exit()
if len(args.errfile) == 0:
    print(f"{errpattern} returned no glob matches!")
    sys.exit()


def single_task_times():
    task_durations = []
    for log in args.logfile:
        try:
            pre_lines = subprocess.check_output(f'grep " succeeded in " {log}', shell=True).decode("ascii")

            pre_list = pre_lines.strip().split("\n")

            for line in pre_list:
                matches = re.search(r"\d+.\d+s:", line)
                if matches:
                    match = matches.group(0)
                    match = float(match.strip("s:"))
                    task_durations.append(match)
        except Exception as e:
            print(f"single_task_times Exception= {e}\n")
            continue

    print(str(task_durations))


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
        print(f"c{args.c}_s{args.s} merlin run : " + str(result))
    except Exception as e:
        result = None
        print(f"c{args.c}_s{args.s} merlin run : ERROR -- result={result}, args.errfile={args.errfile}\n{e}")


def start_verify_time():
    all_timestamps = []
    for log in args.logfile:
        try:
            pre_line = subprocess.check_output(f'grep -m1 "verify" {log}', shell=True).decode("ascii")
            pre_line = pre_line.strip()
            matches = re.search(r"\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d", pre_line)
            match = matches[0]
            element = datetime.datetime.strptime(match, "%Y-%m-%d %H:%M:%S,%f")
            timestamp = datetime.datetime.timestamp(element)
            all_timestamps.append(timestamp)
        except Exception as e:
            continue
        try:
            print(f"c{args.c}_s{args.s} start verify : " + str(all_timestamps[0]))
        except Exception as e:
            print(f"c{args.c}_s{args.s} start verify : ERROR\n{e}")


def start_run_workers_time():
    all_timestamps = []
    for log in args.logfile:
        try:
            pre_line = subprocess.check_output(f'grep -m1 "" {log}', shell=True).decode("ascii")
            pre_line = pre_line.strip()
            matches = re.search(r"\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d", pre_line)
            match = matches[0]
            element = datetime.datetime.strptime(match, "%Y-%m-%d %H:%M:%S,%f")
            timestamp = datetime.datetime.timestamp(element)
            all_timestamps.append(timestamp)
        except Exception:
            continue
    earliest = min(all_timestamps)
    print(f"c{args.c}_s{args.s} start run-workers : " + str(earliest))


def start_sample1_time():
    all_timestamps = []
    for log in args.logfile:
        try:
            pre_line = subprocess.check_output(f"grep -m1 \"Executing step 'null_step'\" {log}", shell=True).decode("ascii")
            pre_line = pre_line.strip()
            matches = re.search(r"\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d\d\d", pre_line)
            match = matches[0]
            element = datetime.datetime.strptime(match, "%Y-%m-%d %H:%M:%S,%f")
            timestamp = datetime.datetime.timestamp(element)
            all_timestamps.append(timestamp)
        except Exception:
            continue
    earliest = min(all_timestamps)
    print(f"c{args.c}_s{args.s} start samp1 : " + str(earliest))


def main():
    try:
        single_task_times()
        merlin_run_time()
        start_verify_time()
        start_run_workers_time()
        start_sample1_time()
        sys.exit()
    except Exception as ex:
        print(ex)
        sys.exit(1)


if __name__ == "__main__":
    main()
