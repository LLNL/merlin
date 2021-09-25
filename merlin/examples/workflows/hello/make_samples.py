import argparse

import names
import numpy as np


# argument parsing
parser = argparse.ArgumentParser(description="Make some samples (names of people).")
parser.add_argument("--number", type=int, action="store", help="the number of samples you want to make")
parser.add_argument("--filepath", type=str, help="output file")
args = parser.parse_args()

# sample making
all_names = np.loadtxt(names.FILES["first:female"], dtype=str, usecols=0)
selected_names = np.random.choice(all_names, size=args.number)

result = ""
name_list = list(selected_names)
result = "\n".join(name_list)

with open(args.filepath, "w") as f:
    f.write(result)
