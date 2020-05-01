import argparse
import sys

from faker import Faker


fake = Faker()


def process_args(args):
    n_samples = args.n
    # n_batches = args.b
    outfile = args.outfile

    with open(outfile, "w") as name_file:
        for samp in range(n_samples):
            name_file.write("{}\n".format(fake.first_name_male()))

        # for batch in range(n_batches):
        #     for samp in range(n_samples):
        #         name_file.write('{}, {}\n'.format(batch, fake.first_name_male()))
        # name_batch = ','.join([fake.first_name_male() for samp in range(n_samples)])
        # name_file.write('{}\n'.format(name_batch))


def setup_argparse():
    parser = argparse.ArgumentParser("Generate some names!")
    parser.add_argument("-n", help="number of names", default=100, type=int)
    parser.add_argument("-b", help="number of batches of names", default=5, type=int)
    parser.add_argument(
        "-outfile", help="name of output .csv file", default="samples.csv"
    )
    return parser


def main():
    parser = setup_argparse()
    args = parser.parse_args()
    process_args(args)


if __name__ == "__main__":
    sys.exit(main())
