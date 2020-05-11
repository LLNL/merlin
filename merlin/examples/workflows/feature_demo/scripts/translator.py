import argparse
import json
import sys

import numpy as np


def setup_argparse():
    parser = argparse.ArgumentParser(description="Translate .json into numpy")

    parser.add_argument(
        "-input",
        help=".json file with X and y data in each sample",
        default="results.json",
    )
    parser.add_argument(
        "-output", help=".npz file with the arrays", default="results.npz"
    )
    parser.add_argument(
        "-schema", help="schema for a single sample data", default="features.json"
    )
    return parser


def process_args(args):
    # data = cb.load_node(args.input)
    samples = json.load(open(args.input, "r"))
    schema = json.load(open(args.schema, "r"))

    input_array_dict = {}
    output_array_dict = {}
    for s in samples:
        make_data_array_dict(input_array_dict, s["inputs"], schema["inputs"])
        make_data_array_dict(output_array_dict, s["outputs"], schema["outputs"])

    for path in output_array_dict:
        output_array_dict[path] = np.array(output_array_dict[path])

    for path in input_array_dict:
        input_array_dict[path] = np.array(input_array_dict[path])

    X = np.vstack([input_array_dict[x] for x in input_array_dict])

    output_array_dict["X"] = X.T

    np.savez(args.output, **output_array_dict)


def generate_scalar_path_pairs(node, schema, path=""):
    for child in node:
        # only process children that are schema compatible
        if child in schema.keys():
            if isinstance(node[child], dict):
                if isinstance(schema[child], dict):
                    for pair in generate_scalar_path_pairs(
                        node[child], schema[child], path=path + child + "/"
                    ):
                        yield pair
            else:
                if not isinstance(schema[child], dict):
                    yield path + child, node[child]


def make_data_array_dict(d, node, schema):
    for path, datum in generate_scalar_path_pairs(node, schema):
        if path in d:
            d[path].append(datum)
        else:
            d[path] = [datum]


def main():
    parser = setup_argparse()
    args = parser.parse_args()
    process_args(args)


if __name__ == "__main__":
    sys.exit(main())
