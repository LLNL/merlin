import argparse
import ast
import sys

import numpy as np

from merlin.common.util_sampling import scale_samples


def process_args(args):
    n_samples = args.n
    n_dims = args.dims
    sample_type = args.sample_type
    if sample_type == "random":
        x = np.random.random((n_samples, n_dims))
    elif sample_type == "grid":
        subdivision = int(pow(n_samples, 1 / float(n_dims)))
        temp = [np.linspace(0, 1.0, subdivision) for i in range(n_dims)]
        X = np.meshgrid(*temp)
        x = np.stack([xx.flatten() for xx in X], axis=1)

    if args.scale is not None:
        limits = []
        do_log = []
        scales = ast.literal_eval(args.scale)
        for scale in scales:
            limits.append((scale[0], scale[1]))
            if scale[2] == "log":
                do_log.append(True)
            else:
                do_log.append(False)
        x = scale_samples(x, limits, do_log=do_log)

    np.save(args.outfile, x)


def setup_argparse():
    parser = argparse.ArgumentParser("Generate some samples!")
    parser.add_argument("-n", help="number of samples", default=100, type=int)
    parser.add_argument("-dims", help="number of dimensions", default=2, type=int)
    parser.add_argument(
        "-sample_type",
        help="type of sampling. options: random, grid. If grid, will try to get close to the correct number of samples",
        default="random",
    )
    parser.add_argument(
        "-scale",
        help='ranges to scale results in form "[(min,max,type),(min, max,type)]" where type = "linear" or "log"',
    )
    parser.add_argument("-outfile", help="name of output .npy file", default="samples")
    return parser


def main():
    try:
        parser = setup_argparse()
        args = parser.parse_args()
        process_args(args)
        sys.exit()
    except Exception as ex:
        print(ex)
        sys.exit(1)


if __name__ == "__main__":
    main()
