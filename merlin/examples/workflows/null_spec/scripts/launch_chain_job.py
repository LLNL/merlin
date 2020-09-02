import argparse
import os
import shutil
import socket
import subprocess


parser = argparse.ArgumentParser(description="Launch 1 merlin workflow chain job")
parser.add_argument("run_id", type=int, help="The ID of this run")
parser.add_argument("concurrency", type=int, help="The concurrency of this chain")
parser.add_argument("output_path", type=str, help="the output path")
parser.add_argument("spec_path", type=str, help="path to the spec to run")
parser.add_argument("script_path", type=str, help="path to the make samples script")
parser.add_argument("seconds", type=int, default=1, help="n seconds to wait per sim")
args = parser.parse_args()

machine = socket.gethostbyaddr(socket.gethostname())[0]
if "quartz" in machine:
    machine = "quartz"
elif "pascal" in machine:
    machine = "pascal"

# launch n_samples * n_conc merlin workflow jobs
submit_path = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))
concurrency = int(args.concurrency)
if concurrency > 32:
    nodes = int(concurrency / 32)
else:
    nodes = 1
samples = [1, 10, 100, 1000, 10000, 100000]

# calculate estimated total time, including all samples
real_times = []
for sample in samples:
    samp_per_worker = float(sample) / float(concurrency)
    if (samp_per_worker / 60) < 1.0:
        real_time = 4
    elif (samp_per_worker / 60) < 3.0:
        real_time = 10
    else:
        real_time = samp_per_worker / 60
        real_time *= 1.5
        real_time = int(round(real_time, 0))
    real_times.append(real_time)
sample = samples[0]
total_time = sum(real_times)

if machine == "quartz":
    account = "lbpm"
    partition = "pdebug"
elif machine == "pascal":
    account = "wbronze"
    partition = "pvis"
if total_time > 60:
    partition = "pbatch"
if total_time > 1440:
    total_time = 1440
if total_time < 180:
    total_time = 180

output_path = os.path.join(args.output_path, f"run_{args.run_id}")
os.makedirs(output_path, exist_ok=True)
os.chdir(output_path)
os.mkdir("scripts")

submit = "submit_chain.sbatch"
command = f"sbatch -J c{concurrency}r{args.run_id}_chain --time {total_time} -N {nodes} -p {partition} -A {account} {submit} {sample} {int(concurrency/nodes)} {args.run_id} {concurrency} {args.seconds}"
shutil.copyfile(os.path.join(submit_path, submit), submit)
shutil.copyfile(args.spec_path, "null_chain.yaml")
shutil.copyfile(args.script_path, os.path.join("scripts", "make_samples.py"))
lines = subprocess.check_output(command, shell=True).decode("ascii")
