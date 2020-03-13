import subprocess
import os
import shutil

submit_path = "/g/g13/bay1/merlin/merlin/examples/workflows/null_spec/submit.sbatch"
concurrencies = [4,8,16,32]
samples = [1,10,100,1000]
for concurrency in concurrencies:
    if not os.path.isdir(f"c_{concurrency}"):
        os.mkdir(f"c_{concurrency}")
    os.chdir(f"c_{concurrency}")
    for sample in samples:
        if not os.path.isdir(f"s_{sample}"):
            os.mkdir(f"s_{sample}")
        os.chdir(f"s_{sample}")
        command = f"sbatch submit.sbatch {sample} {concurrency}"
        shutil.copyfile(submit_path, "submit.sbatch")
        lines = subprocess.check_output(command, shell=True).decode('ascii')
        os.chdir(f"..")
    os.chdir(f"..")
