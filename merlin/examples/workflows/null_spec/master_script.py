import subprocess
import os
import shutil

submit_path = "/g/g13/bay1/merlin/merlin/examples/workflows/null_spec"
concurrencies = [1,2,4,8,16,32,64]
nodes =         [1,1,1,1, 1, 1, 2]
samples = [1,10,100,1000,10000]
output_path = "/g/g13/bay1/null_results"
#output_path = "/g/g13/bay1/merlin/merlin/examples/workflows/null_spec"
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
        submit = f"submit{nodes[i]}.sbatch"
        command = f"sbatch {submit} {sample} {int(concurrency/nodes[i])}"
        shutil.copyfile(os.path.join(submit_path, submit), submit)
        lines = subprocess.check_output(command, shell=True).decode('ascii')
        os.chdir(f"..")
    os.chdir(f"..")

