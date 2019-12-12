import argparse
import sys

import numpy as np


try:
    import cPickle as pickle
except ImportError:
    import pickle


def setup_argparse():
    parser = argparse.ArgumentParser(description="Use a regressor to make a prediction")

    parser.add_argument(
        "-infile", help=".npy file with data to predict", default="new_x.npy"
    )
    parser.add_argument(
        "-reg", help="pickled regressor file", default="random_forest_reg.pkl"
    )
    parser.add_argument(
        "-outfile", help="file to store the new predictions", default="new_y.npy"
    )

    return parser


def predict(args):
    regr = pickle.load(open(args.reg, "rb"))

    X = np.load(args.infile)

    new_y = regr.predict(X)
    np.save(open(args.outfile, "wb"), new_y)


def main():
    parser = setup_argparse()
    args = parser.parse_args()
    predict(args)


if __name__ == "__main__":
    sys.exit(main())
