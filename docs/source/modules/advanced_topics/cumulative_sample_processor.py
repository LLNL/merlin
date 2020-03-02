import os
import sys
import argparse

import matplotlib.pyplot as plt
import pandas as pd

def setup_argparse():
    parser = argparse.ArgumentParser(
        description="Read in and analyze samples from plain text file"
    )

    parser.add_argument(
        "sample_file_paths", help="paths to sample files", default="",
        nargs='+'
    )
    parser.add_argument("--hardcopy", help="Name of cumulative plot file", default="cum_results.png")
    
    return parser


def main():
    parser = setup_argparse()
    args = parser.parse_args()

    # Load all iterations' data into single pandas dataframe for further analysis
    iter_dfs = []
    for sample_file_path in args.sample_file_paths:
        file_parts = os.path.basename(sample_file_path).split('_')
        iter_num = int(file_parts[1])

        itervals = pd.read_json(sample_file_path, orient='values', typ='series')
        iterframe = pd.DataFrame(itervals, columns=['Count'])
        iterframe['Iter'] = iter_num

        iter_dfs.append(iterframe)

    all_iter_df = pd.concat(iter_dfs)
    
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
        max_counts.append(all_iter_df[all_iter_df['Iter'] <= it]['Count'].max())
        min_counts.append(all_iter_df[all_iter_df['Iter'] <= it]['Count'].min())
        med_counts.append(all_iter_df[all_iter_df['Iter'] <= it]['Count'].median())

        unique_names.append(len(all_iter_df[all_iter_df['Iter'] <= it].index.value_counts()))

        # Plot all counts in iterations 1 -> it
        # ax[0][0].plot([i for i in range(unique_names[it-1])],
        #               all_iter_df[all_iter_df['Iter'] <= it].groupby(level=0).sum()['Count'].values,
        #               label='Iter {}'.format(it))

    # ax[0].set_xlabel('Unique Names')
    # ax[0].set_ylabel('Counts')

    #ax[0][0].legend()

    ax[0].plot(iterations,
                  min_counts,
                  label='Minimum Occurances')
    ax[0].plot(iterations,
                  max_counts,
                  label='Maximum Occurances')

    ax[0].plot(iterations,
                  med_counts,
                  label='Median Occurances')
    # ax[0].set_xlabel('Iteration')
    ax[0].set_ylabel('Counts')
    ax[0].legend()

    ax[1].set_xlabel('Iteration')
    ax[1].set_ylabel('Unique Names')
    ax[1].plot(iterations,
                  unique_names)
    
    fig.savefig(args.hardcopy, dpi=150)


if __name__ == "__main__":
    sys.exit(main())
