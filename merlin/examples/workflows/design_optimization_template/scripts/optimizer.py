import numpy as np
import json
from joblib import dump, load
from scipy.stats import multivariate_normal
from scipy.optimize import minimize
import argparse

parser = argparse.ArgumentParser("Learn surrogate model form simulation")
parser.add_argument("-collector_dir", help="Collector directory (.npz file), usually '$(collector.workspace)'")
parser.add_argument("-learner_dir", help="Learner directory (joblib file), usually '$(learner.workspace)'")
args = parser.parse_args()

collector_dir = args.collector_dir
learner_dir = args.learner_dir

surrogate = load(f'{learner_dir}/surrogate.joblib')
from_file = np.load(f'{collector_dir}/all_results.npz', allow_pickle=True)

out_data = from_file['arr_0'].item()

X = []
y = []

for i in out_data.keys():
    X.append(out_data[i]['Inputs'])
    y.append(out_data[i]['Outputs'])

existing_X = np.array(X)
existing_y = np.array(y)

def mean_and_std(x0, surrogate, x_deltas):
    predictions = predictions_around(x0, surrogate, x_deltas)
    return predictions.mean(), predictions.std()

def get_x_deltas(x_std=None, n_samples=500):
    if x_std is None:
        return 0
    else:
        x_cov = np.asarray(x_std)**2
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

# Find the best point so far

# Choose the function for minimization and loading the surrogate model
func = func_percentile
risk_percentile = 5
eval_percents = (0,5,25,50,75,95,100)

# X_deltas
input_uncerts = np.array([0.01, 0.01])

x_deltas = get_x_deltas(x_std=input_uncerts, n_samples=existing_X.shape[0])

# Return the median
existing_f = np.array([func(x) for x in existing_X])

best_f_i = existing_f.argmin() # min value because minimize!
best_f_x = existing_X[best_f_i]

old_y_mean, old_y_std = mean_and_std(best_f_x, surrogate, x_deltas)
old_y_percentiles = prediction_percentile(best_f_x, surrogate, x_deltas, eval_percents).tolist()

delta_mults = [0.0, 0.5, 1.0, 1.5, 2.0]
x_deltas_big ={}
for d in delta_mults:
    x_deltas_big[d] = get_x_deltas(x_std=d*input_uncerts, n_samples=10000)

best_point_variations = {}
for d in x_deltas_big:
    x_samples_big = best_f_x + x_deltas_big[d]
    y_samples_big = surrogate.predict(x_samples_big)
    best_point_variations[d] = {}
    best_point_variations[d]['percentiles'] = np.percentile(y_samples_big, eval_percents).tolist()

    counts, bins = np.histogram(y_samples_big, bins=20)
    best_point_variations[d]['histogram_counts'] = counts.tolist()
    best_point_variations[d]['histogram_bins'] = bins.tolist()

# Optimum code

dim_0_range = [-2, 2]
dim_1_range = [-1, 3]

bounds = [dim_0_range, dim_1_range]
method = 'trust-constr'

minimizer_args = {'bounds':bounds, 'method':method, 'options':{'disp':True}}

x0 = best_f_x + get_x_deltas(x_std=input_uncerts)[0]
optimum_func = minimize(func, x0=x0, **minimizer_args)

start_f = func(x0)

opt_x = optimum_func.x.tolist()
opt_y = (optimum_func.fun).tolist()
init_x = x0.tolist()
init_y = (start_f).tolist()

opt_y_mean, opt_y_std = mean_and_std(optimum_func.x, surrogate, x_deltas)
opt_y_percentiles = prediction_percentile(optimum_func.x, surrogate, x_deltas, eval_percents).tolist()

results = {"Results":{"Predicted Optimum Coordinate":opt_x,
                      "Optimum Function Value":opt_y,
                      "Optimum y Mean":opt_y_mean.tolist(),
                      "Optimum y Std": opt_y_std.tolist(),
                      "Optimum percentiles": opt_y_percentiles,
                      "Best simulation coordinate":best_f_x.tolist(),
                      "Best sim y Mean": old_y_mean.tolist(),
                      "Best sim y Std": old_y_std.tolist(),
                      "Best sim percentiles": old_y_percentiles,
                      "Best sim sig mult variations" : best_point_variations,
                      "Evaluation Percents": eval_percents}
           }

json.dump(results, open('optimization_results.json', 'w'), indent=4)

np.save(open('optimum.npy','wb'), opt_x)
np.save(open('old_best.npy','wb'), best_f_x)
