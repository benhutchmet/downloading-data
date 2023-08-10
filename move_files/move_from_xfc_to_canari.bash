#!/bin/bash

# Usage: bash move_from_xfc_to_canari.bash <model> <variable> <experiment>

# for example: bash move_from_xfc_to_canari.bash HadGEM3-GC31-MM psl dcppA-hindcast
# nohup bash move_from_xfc_to_canari.bash HadGEM3-GC31-MM > output_HadGEM.log &

# This script moves the files from the xfc directory to the canari directory

# Check that the no of args is correct
if [ $# -ne 3 ]; then
    echo "Usage: bash move_from_xfc_to_canari.bash <model> <variable> <experiment>"
    exit 1
fi

# Set the model and variable
model=$1
variable=$2
experiment=$3

# Set the scratch base dir
scratch_base_dir="/work/scratch-nopw2/benhutch"

# Set the canari base dir
canari_base_dir="/gws/nopw/j04/canari/users/benhutch"

# Set the xfc base dir
xfc_base_dir="/work/xfc/vol5/user_cache/benhutch"

# Set the model group
# model name and family
# set up an if loop for the model name
if [ "$model" == "BCC-CSM2-MR" ]; then
    model_group="BCC"
elif [ "$model" == "MPI-ESM1-2-HR" ]; then
    model_group="MPI-M"
elif [ "$model" == "CanESM5" ]; then
    model_group="CCCma"
elif [ "$model" == "CMCC-CM2-SR5" ]; then
    model_group="CMCC"
elif [ "$model" == "HadGEM3-GC31-MM" ]; then
    model_group="MOHC"
elif [ "$model" == "EC-Earth3" ]; then
    model_group="EC-Earth-Consortium"
elif [ "$model" == "EC-Earth3-HR" ]; then
    model_group="EC-Earth-Consortium"
elif [ "$model" == "MRI-ESM2-0" ]; then
    model_group="MRI"
elif [ "$model" == "MPI-ESM1-2-LR" ]; then
    model_group="DWD"
elif [ "$model" == "FGOALS-f3-L" ]; then
    model_group="CAS"
elif [ "$model" == "CNRM-ESM2-1" ]; then
    model_group="CNRM-CERFACS"
elif [ "$model" == "MIROC6" ]; then
    model_group="MIROC"
elif [ "$model" == "IPSL-CM6A-LR" ]; then
    model_group="IPSL"
elif [ "$model" == "CESM1-1-CAM5-CMIP5" ]; then
    model_group="NCAR"
elif [ "$model" == "NorCPM1" ]; then
    model_group="NCC"
else
    echo "[ERROR] Model not recognised"
    exit 1
fi

# if model is HadGEM3 or EC-Earth3
# then move from scratch to canari
if [ "$model" == "HadGEM3-GC31-MM" ] || [ "$model" == "EC-Earth3" ]; then
    # set up the input files
    # MOVE THESE FROM SCRATCH TO CANARI
    files="${scratch_base_dir}/${variable}/${model}/outputs/mergetime/*.nc"
    
    # set up the output files
    output_dir="${canari_base_dir}/${experiment}/data/${variable}/${model}"
    # check that the output dir exists
    if [ ! -d "$output_dir" ]; then
        mkdir -p "$output_dir"
    fi

    # iterate over the files
    for file in $files; do
        # get the filename
        filename=$(basename -- "$file")
        
        # Check if the file exists in the output dir
        if [ ! -f "${output_dir}/${filename}" ]; then
            # move the file
            mv $file $output_dir
        else
            echo "[WARNING] File ${filename} already exists in ${output_dir}"
        fi
    done

else
    # set up the input files from xfc
    # check that this returns the files
    files="${xfc_base_dir}/${model_group}/${model}/*.nc"
    
    # set up the output files
    output_dir="${canari_base_dir}/${experiment}/data/${variable}/${model}"
    # check that the output dir exists
    if [ ! -d "$output_dir" ]; then
        mkdir -p "$output_dir"
    fi

    # Check if the file exists in the output dir
    for file in $files; do
        # get the filename
        filename=$(basename -- "$file")
        
        # Check if the file exists in the output dir
        if [ ! -f "${output_dir}/${filename}" ]; then
            # move the file
            mv $file $output_dir
        else
            echo "[WARNING] File ${filename} already exists in ${output_dir}"
        fi
    done
fi