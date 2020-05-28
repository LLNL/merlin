import argparse
import glob

import numpy as np

import Ofpp


descript = """Using parameters to edit OpenFOAM parameters"""
parser = argparse.ArgumentParser(description=descript)

parser.add_argument(
    "-data", "--data_dir", help="The home directory of the data directories"
)
parser.add_argument("-merlin_paths", nargs="+", help="The path of all merlin runs")

args = parser.parse_args()

DATA_DIR = args.data_dir
X = args.merlin_paths

dir_names = [DATA_DIR + "/" + Xi + "/cavity" for Xi in X]

num_of_timesteps = 10
U = []
enstrophy = []

for i, dir_name in enumerate(dir_names):
    for name in glob.glob(dir_name + "/[0-9]*/U"):
        if name[-4:] == "/0/U":
            continue
        U.append(Ofpp.parse_internal_field(name))

    for name in glob.glob(dir_name + "/[0-9]*/enstrophy"):
        if name[-12:] == "/0/enstrophy":
            continue
        enstrophy.append(Ofpp.parse_internal_field(name))

resolution = np.array(enstrophy).shape[-1]
U = np.array(U).reshape(len(dir_names), num_of_timesteps, resolution, 3)
enstrophy = np.array(enstrophy).reshape(
    len(dir_names), num_of_timesteps, resolution
) / float(resolution)

np.savez("data.npz", U, enstrophy)
