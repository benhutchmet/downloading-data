#!/bin/bash

# Script for downloading the wget scripts for historical data from the ESGF server
#
# Usage: bash historical_data_download_wget_scripts.bash <variable> <experiment_id> <model>
#
# Example: bash historical_data_download_wget_scripts.bash tas historical BCC-CSM2-MR
#

# Source the dictionaries file
source "/home/users/benhutch/downloading-data/dictionaries.bash"

# Echo all of the models to try extracting the data from
echo "The models are: ${models[@]}"

# Echo the data nodes which we will try extracting the data from
echo "The data nodes are: ${data_nodes[@]}"

# Check that the correct number of arguments were provided
if [ "$#" -ne 3 ]; then
    echo "Illegal number of arguments provided"
    echo "Usage: bash historical_data_download_wget_scripts.bash <variable> <experiment_id> <model>"
    echo "Example: bash historical_data_download_wget_scripts.bash tas historical BCC-CSM2-MR"
    exit 1
fi

# Check that the variable provided is valid
# must be either: psl, sfcWind, tas or rsds
if [ "$1" != "psl" ] && [ "$1" != "sfcWind" ] && [ "$1" != "tas" ] && [ "$1" != "rsds" ] && [ "$1" != "tos" ]; then
    echo "Invalid variable provided"
    echo "Usage: bash historical_data_download_wget_scripts.bash <variable> <experiment_id> <model>"
    echo "Example: bash historical_data_download_wget_scripts.bash tas historical BCC-CSM2-MR"
    exit 1
fi

# Extract the variable name from the command line argument
# Also extract the experiment_id
variable_id=$1
experiment_id=$2
model=$3

# Set up the constants for the url to be used
activity_id="CMIP"
latest="true"
project="CMIP6"
table_id="Amon"
# limit="10000"

# Echo the string we are looking for
echo "[INFO] models: ${models[@]}"
echo "[INFO] data nodes: ${data_nodes[@]}"
echo "[INFO] variable: ${variable_id}"
echo "[INFO] activity_id: ${activity_id}"
echo "[INFO] latest: ${latest}"
echo "[INFO] project: ${project}"
echo "[INFO] experiment_id: ${experiment_id}"
echo "[INFO] table_id: ${table_id}"


# Echo the variable name
echo "[INFO] looping over the models and data nodes for the variable: ${variable_id} and experiment_id: ${experiment_id} and model: ${model}"

# Loop over the data nodes
for data_node in "${all_nodes[@]}"; do
    
    # Echo the current data node
    echo "[INFO] data node: ${data_node}"

    # Construct the url            
    url="https://esgf-data.dkrz.de/esg-search/wget?project=${project}&experiment_id=${experiment_id}&source_id=${model}&table_id=${table_id}&variable_id=${variable_id}&latest=${latest}&data_node=${data_node}&limit=${limit}"

    # Echo the url
    echo "[INFO] url: ${url}"

    # Construct the wget script name
    wget_script_name="${model}_${data_node}_${variable_id}.wget"

    # Echo the wget script name
    echo "[INFO] wget script name: ${wget_script_name}"

    wget_script_dir="${canari_dir}/${experiment_id}/${variable_id}/${model}"
    mkdir -p "${wget_script_dir}"

    wget_script="${wget_script_dir}/${wget_script_name}"

    # Download the wget script
    wget -O "${wget_script}" "${url}"

    # Echo the wget script
    echo "[INFO] wget script: ${wget_script}"

    if grep -q "No files were found that matched the query" "${wget_script}" ; then
        echo "Removing ${wget_script} because it contains the string 'No files were found that matched the query'"
        rm "${wget_script}"
    else
        echo "[INFO] wget script exists"
        echo "[INFO] url: ${url}"

        # Make the wget script executable
        chmod +x "${wget_script}"
    fi

done
