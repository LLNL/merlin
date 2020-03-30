import os
import shutil
import subprocess

parser = argparse.ArgumentParser(description="Launch 35 merlin workflow jobs")
parser.add_argument("run_id", type=int, help="The ID of this run")
args = parser.parse_args()

# ensure no workers or tasks from previous runs still exist
subprocess.check_output("merlin stop-workers", shell=True).decode("ascii")
subprocess.check_output("merlin purge ../null_spec.yaml -f", shell=True).decode("ascii")

# launch 35 merlin workflow jobs
submit_path = "."
concurrencies = [1, 2, 4, 8, 16, 32, 64]
nodes = [1, 1, 1, 1, 1, 1, 2]
samples = [1, 10, 100, 1000, 10000]
output_path = f"null_results/run_{args.run_id}"
os.makedirs(output_path)
for i, concurrency in enumerate(concurrencies):
    c_name = os.path.join(output_path, f"c_{concurrency}")
    if not os.path.isdir(c_name):
        os.mkdir(c_name)
    os.chdir(c_name)
    for sample in samples:
        s_name = os.path.join(c_name, f"s_{sample}")
        if not os.path.isdir(s_name):
            os.mkdir(s_name)
        os.chdir(s_name)
        submit = f"submit_{nodes[i]}_node.sbatch"
        command = f"sbatch {submit} {sample} {int(concurrency/nodes[i])}"
        shutil.copyfile(os.path.join(submit_path, submit), submit)
        lines = subprocess.check_output(command, shell=True).decode("ascii")
        os.chdir(f"..")
    os.chdir(f"..")
