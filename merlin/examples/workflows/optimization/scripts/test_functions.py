import argparse
import json
import math

import numpy as np


def rosenbrock(X):
    X = X.T
    total = 0
    for i in range(X.shape[0] - 1):
        total += 100 * ((X[i + 1] - X[i] ** 2) ** 2) + (1 - X[i]) ** 2

    return total


def rastrigin(X, A=10):
    first_term = A * len(inputs)

    return first_term + sum([(x**2 - A * np.cos(2 * math.pi * x)) for x in X])


def ackley(X):
    firstSum = 0.0
    secondSum = 0.0
    for x in X:
        firstSum += x**2.0
        secondSum += np.cos(2.0 * np.pi * x)
    n = float(len(X))

    return -20.0 * np.exp(-0.2 * np.sqrt(firstSum / n)) - np.exp(secondSum / n) + 20 + np.e


def griewank(X):
    term_1 = (1.0 / 4000.0) * sum(X**2)
    term_2 = 1.0
    for i, x in enumerate(X):
        term_2 *= np.cos(x) / np.sqrt(i + 1)
    return 1.0 + term_1 - term_2


parser = argparse.ArgumentParser("Generate some samples!")
parser.add_argument(
    "-function",
    help="Which test function do you want to use?",
    choices=["ackley", "griewank", "rastrigin", "rosenbrock"],
    default="rosenbrock",
)
parser.add_argument("-ID", help="Insert run_id here")
parser.add_argument("-inputs", help="Takes one input at a time", nargs="+")
args = parser.parse_args()

run_id = args.ID
inputs = args.inputs
function_name = args.function

inputs = np.array(inputs).astype(np.float)

test_function = locals()[function_name]
outputs = test_function(inputs)

results = {run_id: {"Inputs": inputs.tolist(), "Outputs": outputs.tolist()}}

json.dump(results, open("simulation_results.json", "w"), indent=4)
