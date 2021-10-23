import argparse
import glob
import json

import numpy as np


parser = argparse.ArgumentParser("Generate some samples!")
parser.add_argument(
    "-sim_dirs",
    help="The base simulation directory, usually '$(run_simulation.workspace)/$(MERLIN_GLOB_PATH)'",
)
args = parser.parse_args()

simulation_dirs = args.sim_dirs
sim_output_files = glob.glob(f"{simulation_dirs}/simulation_results.json")

all_results = {}
for i, sim_output_file in enumerate(sim_output_files):
    with open(sim_output_file) as f:
        data = json.load(f)

    all_results.update(data)

with open("current_results.json", "w") as outfile:
    json.dump(all_results, outfile)

X = []
y = []
for key in all_results.keys():
    X.append(all_results[key]["Inputs"])
    y.append(all_results[key]["Outputs"])

X = np.array(X)
y = np.array(y)

if len(X.shape) == 1:
    X = X.reshape(-1, 1)
if len(y.shape) == 1:
    y = y.reshape(-1, 1)

np.savez("current_results.npz", X=X, y=y)
