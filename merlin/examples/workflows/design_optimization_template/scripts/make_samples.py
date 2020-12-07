import argparse
import ast
import sys
import numpy as np
from scipy.stats.distributions import norm

import pyDOE as doe

from merlin.common.util_sampling import scale_samples

def process_scale(args):
    if args.scale is not None:
        raw = ast.literal_eval(args.scale)
        processed = np.array(raw,dtype=float).tolist()
        return processed

def process_args(args):
    np.random.seed(args.seed)
    n_samples = args.n
    n_dims = args.dims
    hard_bounds = args.hard_bounds
    sample_type = args.sample_type
    if sample_type == "random":
        x = np.random.random((n_samples, n_dims))
    elif sample_type == "grid":
        subdivision = int(pow(n_samples, 1 / float(n_dims)))
        temp = [np.linspace(0, 1.0, subdivision) for i in range(n_dims)]
        X = np.meshgrid(*temp)
        x = np.stack([xx.flatten() for xx in X], axis=1)
    elif sample_type == "lhs":
        x = doe.lhs(n_dims, samples=n_samples)
    elif sample_type == "lhd":
        _x = doe.lhs(n_dims, samples=n_samples)
        x = norm(loc=0.5, scale=0.125).ppf(_x)
    elif sample_type == "star":
        _x = doe.doe_star.star(n_dims)[0]
        x = 0.5*(_x + 1.0) # transform to center at 0.5 (range 0-1)
    else:
        raise ValueError(sample_type+' is not a valid choice for sample_type!')

    scales = process_scale(args)

    if scales is not None:
        limits = []
        do_log = []
        for scale in scales:
            limits.append((scale[0], scale[1]))
            if len(scale) < 3:
                scale.append('linear')
            if scale[2] == 'log':
                   do_log.append(True)
            else:
               do_log.append(False)
        x = scale_samples(x, limits, do_log=do_log)

    # scale the whole box
    x = args.scale_factor*x

    # add x0
    if args.x0 is not None:
        x0 = np.load(args.x0)
        if scales is not None:
            sa = args.scale_factor*np.array(scales)[:,:2].astype('float')
            center = np.mean(sa,axis=1)
        else:
            center = args.scale_factor*0.5
        x = x+x0-center

        # replace the first entry with x0 for the random ones
        if sample_type == 'lhs' or sample_type == 'lhd':
            x[0] = x0
        else: # add it for the stencil points
            x = np.insert(x,0,x0,axis=0)

        if args.x1 is not None:
            x1 = np.load(args.x1)
            line_range = np.linspace(0,1,args.n_line+1,endpoint=False)[1:]
            line_samples = x0 + np.outer(line_range,(x1-x0))
            x = np.vstack((x,line_samples))

    if hard_bounds:
        if scales is None:
            x = np.clip(x, 0, 1)
        else:
            for i,dim in enumerate(scales):
                x[:,i] = np.clip(x[:,i],dim[0],dim[1])

    print(x)

    np.save(args.outfile, x)


def setup_argparse():
    parser = argparse.ArgumentParser("Generate some samples!")
    parser.add_argument("-seed", help="random number seed for generating samples", default=None, type=int)
    parser.add_argument("-n", help="number of samples", default=100, type=int)
    parser.add_argument("-dims", help="number of dimensions", default=2, type=int)
    parser.add_argument(
        "-sample_type",
        help="type of sampling. options: random, grid, lhs, lhd, star. If grid, will try to get close to the correct number of samples. for lhd min-max correspond to +- 3 sigma range",
        default="random",
    )
    parser.add_argument( "-scale",
            help='ranges to scale results in form "[(min,max,type),(min, max,type)]" where type = "linear" or "log" (optional: defaults to linear if omitted)')
    parser.add_argument( "-scale_factor",
        help='scale factor to appy to all ranges (stacks with -scale)', type=float, default=1.0)
    parser.add_argument("-outfile", help="name of output .npy file", default="samples")
    parser.add_argument("-x0", help="file with optional point to center samples around, will be added as first entry", default=None)
    parser.add_argument("-x1", help="file with x1 to add points between x0 and x1 (non inclusive) along a line", default=None)
    parser.add_argument("-n_line", help="number of samples along a line between x0 and x1", default=100, type=int)
    parser.add_argument("--hard-bounds", help="force all points to lie within -scale", action='store_true')
    return parser


def main():
    parser = setup_argparse()
    args = parser.parse_args()
    process_args(args)


if __name__ == "__main__":
    sys.exit(main())
