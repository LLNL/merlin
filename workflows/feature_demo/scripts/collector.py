import argparse
import json
import sys


def process_args(args):
    result = []
    for path in args.instring.split("\n"):
        with open(path, "r") as json_file:
            result.append(json.load(json_file))

    with open(args.outfile, "w") as outfile:
        json.dump(result, outfile)


def setup_argparse():
    parser = argparse.ArgumentParser(
        description="Convert a list of json files into numpy"
    )

    parser.add_argument(
        "-instring", help="text containing list of json files", default=""
    )
    parser.add_argument("-outfile", help="json file", default="results.json")
    return parser


def main():
    parser = setup_argparse()
    args = parser.parse_args()
    process_args(args)


if __name__ == "__main__":
    sys.exit(main())
