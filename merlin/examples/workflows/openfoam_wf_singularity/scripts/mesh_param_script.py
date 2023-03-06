import argparse


def positive_numbers(x):
    x = float(x)
    if not x > 0:
        raise argparse.ArgumentTypeError("Needs to be  a positive number")
    return x


descript = """Using parameters to edit OpenFOAM parameters"""
parser = argparse.ArgumentParser(description=descript)

parser.add_argument("-x", "--width", type=positive_numbers, default=10, help="length")
parser.add_argument("-y", "--height", type=positive_numbers, default=10, help="height")
parser.add_argument("-r", "--resolution", type=int, default=20, help="Resolution")
parser.add_argument("-p", "--piso", type=int, default=0, help="pisoFoam?")
parser.add_argument("-scripts_dir", help="Name of the scripts directory", default="./")

args = parser.parse_args()
WIDTH = args.width
HEIGHT = args.height
RESOLUTION = args.resolution
SCRIPTS_DIRECTORY = args.scripts_dir
PISO = args.piso

template = open(SCRIPTS_DIRECTORY + "blockMesh_template.txt", "r")

WIDTH = float(WIDTH)
HEIGHT = float(HEIGHT)

tmp = template.read()
tmp = tmp.replace("WIDTH", str(WIDTH))
tmp = tmp.replace("HEIGHT", str(HEIGHT))

X_RESOLUTION = int(RESOLUTION * WIDTH / HEIGHT)

tmp = tmp.replace("X_RESOLUTION", str(X_RESOLUTION))
tmp = tmp.replace("Y_RESOLUTION", str(RESOLUTION))

if PISO:
    tmp = tmp.replace("lid", "movingWall")
    print("Changed lid to movingWall")

f = open("blockMeshDict.txt", "w+")
f.write(tmp)
f.close()

template.close()
