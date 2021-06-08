import argparse
import json

import numpy as np


def N_Rosenbrock(X):
    X = X.T
    total = 0
    for i in range(X.shape[0] - 1):
        total += 100 * ((X[i + 1] - X[i] ** 2) ** 2) + (1 - X[i]) ** 2

    return total


parser = argparse.ArgumentParser("Generate some samples!")
parser.add_argument("-ID")
parser.add_argument("-inputs", nargs="+")
args = parser.parse_args()

run_id = args.ID
inputs = args.inputs
inputs = np.array(inputs).astype(np.float)
outputs = N_Rosenbrock(inputs)

results = {run_id: {"Inputs": inputs.tolist(), "Outputs": outputs.tolist()}}

json.dump(results, open("simulation_results.json", "w"), indent=4)
