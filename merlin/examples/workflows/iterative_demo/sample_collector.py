import argparse
import os
import sys
from concurrent.futures import ProcessPoolExecutor


def load_samples(sample_file_path):
    """Read in sample file, returning a flat list of all samples inside"""
    samples = []
    print("Reading: {}".format(sample_file_path))
    with open(os.path.abspath(sample_file_path), "r") as sample_file:
        for sample in sample_file:
            samples.append(sample.strip())

    return samples


def serialize_samples(sample_file_paths, outfile, nproc):
    """Writes out collection of samples, one entry per line"""
    with ProcessPoolExecutor(max_workers=nproc) as executor:
        all_samples = [
            sample
            for sample_list in executor.map(load_samples, sample_file_paths)
            for sample in sample_list
        ]

    with open(outfile, "w") as outfile:
        for sample in all_samples:
            outfile.write("{}\n".format(sample))


def setup_argparse():
    parser = argparse.ArgumentParser(
        description="Read in samples from list of output files"
    )

    parser.add_argument(
        "sample_file_paths", help="paths to sample output files", default="", nargs="+"
    )

    parser.add_argument("--np", help="number of processors to use", type=int, default=1)

    parser.add_argument(
        "-outfile", help="Collected sample outputs", default="all_names.txt"
    )
    return parser


def main():
    parser = setup_argparse()
    args = parser.parse_args()

    # Collect sample files into single file
    sample_paths = [sample_path for sample_path in args.sample_file_paths]
    serialize_samples(sample_paths, args.outfile, args.np)


if __name__ == "__main__":
    sys.exit(main())
