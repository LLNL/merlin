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

np.savez("current_results.npz", all_results)
