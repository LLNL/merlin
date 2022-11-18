import argparse

import numpy as np


# argument parsing
parser = argparse.ArgumentParser(description="Make some samples (names of people).")
parser.add_argument("--number", type=int, action="store", help="the number of samples you want to make")
parser.add_argument("--filepath", type=str, default="samples_file.npy", help="output file")
args = parser.parse_args()

# sample making
result = np.random.random((args.number, 1))

np.save(args.filepath, result)
