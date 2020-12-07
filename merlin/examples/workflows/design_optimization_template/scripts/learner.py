import numpy as np
from joblib import dump, load
from sklearn.ensemble import RandomForestRegressor
import argparse

parser = argparse.ArgumentParser("Learn surrogate model form simulation")
parser.add_argument("-collector_dir", help="Collector directory (.npz file), usually '$(collector.workspace)'")
args = parser.parse_args()

collector_dir = args.collector_dir
from_file = np.load(f'{collector_dir}/all_results.npz', allow_pickle=True)

out_data = from_file['arr_0'].item()

X = []
y = []

for i in out_data.keys():
    X.append(out_data[i]['Inputs'])
    y.append(out_data[i]['Outputs'])

X = np.array(X)
y = np.array(y)

surrogate = RandomForestRegressor(max_depth=4, random_state=0, n_estimators=100)
surrogate.fit(X, y)

dump(surrogate, 'surrogate.joblib')
