import argparse
import json
import sys


def process_args(args):
    results = {
        "inputs": {"X": args.X, "Y": args.Y, "Z": args.Z},
        "outputs": {
            "X+Y+Z": args.X + args.Y + args.Z,
            "X*Y*Z": args.X * args.Y * args.Z,
        },
    }

    with open(args.outfile, "w") as f:
        json.dump(results, f)


def setup_argparse():
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument("X", metavar="X", type=float, help="The x dimension of the sample.")
    parser.add_argument("Y", metavar="Y", type=float, help="The y dimension of the sample.")
    parser.add_argument("Z", metavar="Z", type=float, help="The z dimension of the sample.")
    parser.add_argument("-outfile", help="Output file name", default="hello_world_output.json")
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
