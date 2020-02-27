import sys
import argparse

import pandas as pd

def process_args(args):
    samples = []
    for sample_file_path in args.sample_file_paths:
        with open(sample_file_path, "r") as sample_file:
            for sample in sample_file:
                sample = sample.strip()
                samples.append(sample)

    return samples
                
def setup_argparse():
    parser = argparse.ArgumentParser(
        description="Read in and analyze samples from plain text file"
    )

    parser.add_argument(
        "sample_file_paths", help="paths to sample files", default="",
        nargs='+'
    )
    parser.add_argument("--results", help="Name of output json file", default="samples.json")
    
    return parser


def main():
    parser = setup_argparse()
    args = parser.parse_args()
    samples = process_args(args)

    # Lets do some statistics on these samples
    namesdf = pd.DataFrame({"Name":samples})

    # Count occurences of each name
    names = namesdf["Name"].value_counts()

    names.to_json(args.results)
    
if __name__ == "__main__":
    sys.exit(main())
