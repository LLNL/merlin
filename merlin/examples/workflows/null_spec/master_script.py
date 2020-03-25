import subprocess
import os
import shutil

submit_path = "/g/g13/bay1/merlin/merlin/examples/workflows/null_spec/submit.sbatch"
concurrencies = [32]
samples = [1,10,100,1000,10000]
output_path = "/g/g13/bay1/null_results"
for concurrency in concurrencies:
    c_name = os.path.join(output_path, f"c_64")
    if not os.path.isdir(c_name):
        os.mkdir(c_name)
    os.chdir(c_name)
    for sample in samples:
        s_name = os.path.join(c_name, f"s_{sample}")
        if not os.path.isdir(s_name):
            os.mkdir(s_name)
        os.chdir(s_name)
        command = f"sbatch submit.sbatch {sample} {concurrency}"
        shutil.copyfile(submit_path, "submit.sbatch")
        lines = subprocess.check_output(command, shell=True).decode('ascii')
        os.chdir(f"..")
    os.chdir(f"..")
