import argparse

import names


# argument parsing
parser = argparse.ArgumentParser(description="Make some samples (names of people).")
parser.add_argument(
    "--number", type=int, action="store", help="the number of samples you want to make"
)
parser.add_argument("--filepath", type=str, help="output file")
args = parser.parse_args()

# sample making
result = ""
name_list = []
for i in range(args.number):
    name_list.append(names.get_full_name())

for name in name_list:
    result += name + "\n"

with open(args.filepath, "w") as f:
    f.write(result)
