import sys
import argparse

def process_args(args):
    result = []
    samples = []
    for path in args.paths:
        with open(path, "r") as sample_file:
            for sample in sample_file:
                sample = sample.strip()
                samples.append(sample)

    print("Samples:")
    for idx, sample in enumerate(samples):
        print(sample)

    with open(args.outfile, "w") as outfile:
        for sample in samples:
            outfile.write("{}\n".format(sample))


def setup_argparse():
    parser = argparse.ArgumentParser(
        description="Read in samples from list of output files"
    )

    parser.add_argument(
        "paths", help="paths to sample output files", default="",
        nargs='+'
    )

    parser.add_argument("-outfile", help="Collected sample outputs", default="all_names.txt")
    return parser


def main():
    parser = setup_argparse()
    args = parser.parse_args()
    process_args(args)


if __name__ == "__main__":
    sys.exit(main())
