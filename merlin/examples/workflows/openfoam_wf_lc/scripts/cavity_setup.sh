#!/bin/bash

MERLIN_INFO=$1
OPENFOAM_DIR=$2

cp -r $OPENFOAM_DIR/OpenFOAM-5.0/tutorials/incompressible/icoFoam/cavity/cavity $MERLIN_INFO/

cd $MERLIN_INFO/cavity

echo "***** Setting Up Mesh *****"
python $MERLIN_INFO/scripts/mesh_param_script.py -scripts_dir $MERLIN_INFO/scripts/
mv blockMeshDict.txt system/blockMeshDict

echo "***** Setting Control Dictionary *****"
sed -i "30s/.*/writeControl    runTime;/" system/controlDict
sed -i "26s/.*/endTime         1;/" system/controlDict
sed -i "32s/.*/writeInterval   .1;/" system/controlDict
