import argparse

import numpy as np
from joblib import dump, load
from sklearn.ensemble import RandomForestRegressor


parser = argparse.ArgumentParser("Learn surrogate model form simulation")
parser.add_argument(
    "-collector_dir",
    help="Collector directory (.npz file), usually '$(collector.workspace)'",
)
args = parser.parse_args()

collector_dir = args.collector_dir
current_iter_npz = np.load(f"{collector_dir}/current_results.npz", allow_pickle=True)
current_iter_data = current_iter_npz["arr_0"].item()

try:
    prev_iter_npz = np.load("all_iter_results.npz", allow_pickle=True)
    prev_iter_data = prev_iter_npz["arr_0"].item()
    data = dict(prev_iter_data, **current_iter_data)
except:
    data = current_iter_data

X = []
y = []

for i in data.keys():
    X.append(data[i]["Inputs"])
    y.append(data[i]["Outputs"])

X = np.array(X)
y = np.array(y)

surrogate = RandomForestRegressor(max_depth=4, random_state=0, n_estimators=100)
surrogate.fit(X, y)

dump(surrogate, "surrogate.joblib")
np.savez("all_iter_results.npz", data)
