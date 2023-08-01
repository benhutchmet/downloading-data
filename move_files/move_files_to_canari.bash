#!/bin/bash

# Usage: bash move_files.bash <variable>

# # Enable debugging
# set -x

# Load the models.bash file
source /home/users/benhutch/multi-model/models.bash || { echo "Failed to source models.bash"; exit 1; }

# Get the argument
arg=$1
echo "Argument: $arg"

# Set up the canari base dir
canari_base_dir="/gws/nopw/j04/canari/users/benhutch/${arg}"

# Echo the canari base dir
echo "Canari base dir: $canari_base_dir"

# Set up the xfc base dir
xfc_base_dir="/work/xfc/vol5/user_cache/benhutch/${arg}"

# Based on the argument, assign the appropriate variable
if [ "$arg" = "sfcWind" ]; then
    models=("${wind_speed_ESGF_models[@]}")
elif [ "$arg" = "tas" ]; then
    models=("${tas_ESGF_models[@]}")
elif [ "$arg" = "rsds" ]; then
    models=("${rsds_ESGF_models[@]}")
else
    echo "Unsupported argument"
    exit 1
fi

# Print models
echo "Models: ${models[@]}"

# Iterate over the models
for model in "${models[@]}"; do
    echo "Processing model: $model"
    # Create a directory for each model if it doesn't exist
    if [ ! -d "$model" ]; then
        echo "Directory $model does not exist. Creating it."
        mkdir "$model"
    else
        echo "Directory $model already exists."
    fi

    # Find the files in canari which match the model
    echo "Finding files in xfc which match the model"

    # Set up the files which will be moved from xfc to canari
    xfc_files=${xfc_base_dir}/${model}/*.nc

    # Set up the canari directory
    canari_dir="${canari_base_dir}/${model}"
    # make the canari directory if it doesn't exist
    mkdir -p "$canari_dir"

    # Iterate over the files
    for xfc_file in $xfc_files; do
        # Check if the file exists in canari
        canari_file="${canari_dir}/$(basename $xfc_file)"

        # If the file doesn't exist in canari, move it
        if [ ! -f "$canari_file" ]; then
            echo "Moving file $xfc_file to $canari_dir"
            mv "$xfc_file" "$canari_dir"
        else
            echo "File $canari_file already exists. Not moving."
        fi

    done

done

# Disable debugging
set +x