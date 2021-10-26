import argparse
import ast
import json
import pickle

import numpy as np
from scipy.optimize import minimize
from scipy.stats import multivariate_normal


parser = argparse.ArgumentParser("Learn surrogate model form simulation")
parser.add_argument(
    "-learner_dir",
    help="Learner directory (joblib file), usually '$(learner.workspace)'",
)
parser.add_argument("-bounds", help="ranges to scale results in form '[(min,max,type),(min, max,type)]'")
parser.add_argument("-input_uncerts", help="The standard deviation for each input dimension")
parser.add_argument("-method", help="The optimizer method", default="trust-constr")

args = parser.parse_args()

method = args.method
learner_dir = args.learner_dir

surrogate = pickle.load(open(f"{learner_dir}/surrogate.pkl", "rb"))
all_iter_results = np.load(f"{learner_dir}/all_iter_results.npz", allow_pickle=True)

existing_X = all_iter_results["X"]
existing_y = all_iter_results["y"]


def process_bounds(args):
    if args.bounds is not None:
        raw = ast.literal_eval(args.bounds)
        processed = np.array(raw, dtype=float).tolist()
        return processed


def process_uncerts(args):
    if args.input_uncerts is not None:
        raw = ast.literal_eval(args.input_uncerts)
        processed = np.array(raw, dtype=float)
        return processed


def mean_and_std(x0, surrogate, x_deltas):
    predictions = predictions_around(x0, surrogate, x_deltas)
    return predictions.mean(), predictions.std()


def get_x_deltas(x_std=None, n_samples=500):
    if x_std is None:
        return 0
    else:
        x_cov = np.asarray(x_std) ** 2
        x_deltas = multivariate_normal.rvs(np.zeros(x_cov.shape), cov=x_cov, size=n_samples)
        return x_deltas


def func_percentile(x, maximize=False):
    func = prediction_percentile(x, surrogate, x_deltas, risk_percentile)

    if maximize:
        func_to_min = -1.0 * func
    else:
        func_to_min = func

    return func_to_min


def predictions_around(x0, surrogate, x_deltas):
    x_samples = x0 + x_deltas
    predictions = surrogate.predict(x_samples)

    return predictions


def prediction_percentile(x0, surrogate, x_deltas, percentile=50):
    predictions = predictions_around(x0, surrogate, x_deltas)

    return np.percentile(predictions, percentile)


bounds = process_bounds(args)
input_uncerts = process_uncerts(args)

for bound in bounds:
    print(bound)

print(input_uncerts)
# Find the best point so far

# Choose the function for minimization and loading the surrogate model
func = func_percentile
risk_percentile = 5
eval_percents = (0, 5, 25, 50, 75, 95, 100)

x_deltas = get_x_deltas(x_std=input_uncerts, n_samples=existing_X.shape[0])

# Return the median
existing_f = np.array([func(x) for x in existing_X])

best_f_i = existing_f.argmin()  # min value because minimize!
best_f_x = existing_X[best_f_i]

old_y_mean, old_y_std = mean_and_std(best_f_x, surrogate, x_deltas)
old_y_percentiles = prediction_percentile(best_f_x, surrogate, x_deltas, eval_percents).tolist()

delta_mults = [0.0, 0.5, 1.0, 1.5, 2.0]
x_deltas_big = {}
for d in delta_mults:
    x_deltas_big[d] = get_x_deltas(x_std=d * input_uncerts, n_samples=10000)

best_point_variations = {}
for d in x_deltas_big:
    x_samples_big = best_f_x + x_deltas_big[d]
    y_samples_big = surrogate.predict(x_samples_big)
    best_point_variations[d] = {}
    best_point_variations[d]["percentiles"] = np.percentile(y_samples_big, eval_percents).tolist()

    counts, bins = np.histogram(y_samples_big, bins=20)
    best_point_variations[d]["histogram_counts"] = counts.tolist()
    best_point_variations[d]["histogram_bins"] = bins.tolist()

# Optimum code

minimizer_args = {"bounds": bounds, "method": method, "options": {"disp": True}}

x0 = best_f_x + get_x_deltas(x_std=input_uncerts)[0]
optimum_func = minimize(func, x0=x0, **minimizer_args)

start_f = func(x0)

opt_x = optimum_func.x.tolist()
opt_y = (optimum_func.fun).tolist()
init_x = x0.tolist()
init_y = (start_f).tolist()

opt_y_mean, opt_y_std = mean_and_std(optimum_func.x, surrogate, x_deltas)
opt_y_percentiles = prediction_percentile(optimum_func.x, surrogate, x_deltas, eval_percents).tolist()

results = {
    "Results": {
        "Predicted Optimum Coordinate": opt_x,
        "Optimum Function Value": opt_y,
        "Optimum y Mean": opt_y_mean.tolist(),
        "Optimum y Std": opt_y_std.tolist(),
        "Optimum percentiles": opt_y_percentiles,
        "Best simulation coordinate": best_f_x.tolist(),
        "Best sim y Mean": old_y_mean.tolist(),
        "Best sim y Std": old_y_std.tolist(),
        "Best sim percentiles": old_y_percentiles,
        "Best sim sig mult variations": best_point_variations,
        "Evaluation Percents": eval_percents,
    }
}

json.dump(results, open("optimization_results.json", "w"), indent=4)

np.save(open("optimum.npy", "wb"), opt_x)
np.save(open("old_best.npy", "wb"), best_f_x)
