import argparse

import numpy as np


def loguniform(low=-1, high=3, size=None, base=10):
    return np.power(base, np.random.uniform(low, high, size))


parser = argparse.ArgumentParser("Generate some samples!")
parser.add_argument("-n", help="number of samples", default=100, type=int)
parser.add_argument("-outfile", help="name of output .npy file", default="samples")

args = parser.parse_args()

N_SAMPLES = args.n
REYNOLD_RANGE = [1, 100]
LIDSPEED_RANGE = [0.1, 100]
BASE = 10

x = np.empty((N_SAMPLES, 2))
x[:, 0] = np.random.uniform(LIDSPEED_RANGE[0], LIDSPEED_RANGE[1], size=N_SAMPLES)
vi_low = np.log10(x[:, 0] / REYNOLD_RANGE[1])
vi_high = np.log10(x[:, 0] / REYNOLD_RANGE[0])
x[:, 1] = [
    loguniform(low=vi_low[i], high=vi_high[i], base=BASE) for i in range(N_SAMPLES)
]

np.save(args.outfile, x)
