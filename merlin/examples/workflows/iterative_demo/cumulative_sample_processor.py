import argparse
import os
import sys
from concurrent.futures import ProcessPoolExecutor

import matplotlib.pyplot as plt
import pandas as pd


def iter_df_from_json(json_file):
    """
    Reads a single iterations' set of processed samples from json,
    extracts iteration number from the file name and returns dataframe with
    processed samples along with an 'Iter' column.

    Parameters
    ----------
    json_file: str
        path to json file with name formatted 'iter_<num>_results.json', with
        <num> indicating iteration number.

    Returns
    -------
    iter_frame: pandas.DataFrame
        Columns: <Name>, <Count>, <Iter>.
    """
    file_parts = os.path.basename(json_file).split("_")
    iter_num = int(file_parts[1])

    iter_vals = pd.read_json(json_file, orient="values", typ="series")
    iter_frame = pd.DataFrame(iter_vals, columns=["Count"])
    iter_frame["Iter"] = iter_num

    return iter_frame


def load_samples(sample_file_paths, nproc):
    """Loads all iterations' processed samples into a single pandas DataFrame in parallel"""
    with ProcessPoolExecutor(max_workers=nproc) as executor:
        iter_dfs = [iter_frame for iter_frame in executor.map(iter_df_from_json, sample_file_paths)]
    all_iter_df = pd.concat(iter_dfs)

    return all_iter_df


def setup_argparse():
    parser = argparse.ArgumentParser(description="Read in and analyze samples from plain text file")

    parser.add_argument("sample_file_paths", help="paths to sample files", default="", nargs="+")

    parser.add_argument("--np", help="number of processors to use", type=int, default=1)

    parser.add_argument("--hardcopy", help="Name of cumulative plot file", default="cum_results.png")

    return parser


def main():
    try:
        parser = setup_argparse()
        args = parser.parse_args()

        # Load all iterations' data into single pandas dataframe for further analysis
        all_iter_df = load_samples(args.sample_file_paths, args.np)

        # PLOTS:
        # counts vs index for each iter range (1, [1,2], [1-3], [1-4], ...)
        # num names vs iter
        # median, min, max counts vs iter -> same plot
        fig, ax = plt.subplots(nrows=2, ncols=1, constrained_layout=True, sharex=True)

        iterations = sorted(all_iter_df.Iter.unique())

        max_counts = []
        min_counts = []
        med_counts = []
        unique_names = []

        for it in iterations:
            max_counts.append(all_iter_df[all_iter_df["Iter"] <= it]["Count"].max())
            min_counts.append(all_iter_df[all_iter_df["Iter"] <= it]["Count"].min())
            med_counts.append(all_iter_df[all_iter_df["Iter"] <= it]["Count"].median())

            unique_names.append(len(all_iter_df[all_iter_df["Iter"] <= it].index.value_counts()))

        ax[0].plot(iterations, min_counts, label="Minimum Occurances")
        ax[0].plot(iterations, max_counts, label="Maximum Occurances")

        ax[0].plot(iterations, med_counts, label="Median Occurances")

        ax[0].set_ylabel("Counts")
        ax[0].legend()

        ax[1].set_xlabel("Iteration")
        ax[1].set_ylabel("Unique Names")
        ax[1].plot(iterations, unique_names)

        fig.savefig(args.hardcopy, dpi=150)
        sys.exit()
    except Exception as ex:
        print(ex)
        sys.exit(1)


if __name__ == "__main__":
    main()
