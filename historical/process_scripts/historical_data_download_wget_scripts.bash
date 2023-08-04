#!/bin/bash

# Script for downloading the wget scripts for historical data from the ESGF server
#
# Usage: bash historical_data_download_wget_scripts.bash <variable> <experiment_id>
#
# Example: bash historical_data_download_wget_scripts.bash tas historical
#

# Source the dictionaries file
source "/home/users/benhutch/downloading-data/dictionaries.bash"

# Echo the historical models
echo "The historical models are: ${historical_models[@]}"

# Echo the data nodes which we will try extracting the data from
echo "The data nodes are: ${data_nodes[@]}"

# Check that the correct number of arguments were provided
if [ "$#" -ne 1 ]; then
    echo "Illegal number of arguments provided"
    echo "Usage: bash historical_data_download_wget_scripts.bash <variable>"
    echo "Example: bash historical_data_download_wget_scripts.bash tas"
    exit 1
fi

# Check that the variable provided is valid
# must be either: psl, sfcWind, tas or rsds
if [ "$1" != "psl" ] && [ "$1" != "sfcWind" ] && [ "$1" != "tas" ] && [ "$1" != "rsds" ] && [ "$1" != "tos" ]; then
    echo "Invalid variable provided"
    echo "Usage: bash historical_data_download_wget_scripts.bash <variable>"
    echo "Example: bash historical_data_download_wget_scripts.bash tas"
    exit 1
fi

# Extract the variable name from the command line argument
# Also extract the experiment_id
variable_id=$1
experiment_id=$2

# Set up the constants for the url to be used
activity_id="CMIP"
latest="true"
project="CMIP6"
table_id="Amon"
limit="10000"

# Echo the string we are looking for
echo "[INFO] models: ${historical_models[@]}"
echo "[INFO] data nodes: ${data_nodes[@]}"
echo "[INFO] variable: ${variable_id}"
echo "[INFO] activity_id: ${activity_id}"
echo "[INFO] latest: ${latest}"
echo "[INFO] project: ${project}"
echo "[INFO] experiment_id: ${experiment_id}"
echo "[INFO] table_id: ${table_id}"


# Echo the variable name
echo "[INFO] looping over the models and data nodes for the variable: ${variable_id}"

# Loop over the data nodes
for data_node in "${nodes[@]}"; do
    
    # Echo the current data node
    echo "[INFO] data node: ${data_node}"

    # Construct the url            
    url="https://esgf-data.dkrz.de/esg-search/wget?project=${project}&experiment_id=${experiment_id}&source_id=${model}&table_id=${table_id}&variable_id=${variable_id}&latest=${latest}&activity_id=${activity_id}&data_node=${data_node}&limit=${limit}"

    # Echo the url
    echo "[INFO] url: ${url}"

    # Construct the wget script name
    wget_script_name="${model}_${data_node}_${variable_id}.wget"

    # Echo the wget script name
    echo "[INFO] wget script name: ${wget_script_name}"

    # Download the wget script
    wget -O "${canari_dir}/${experiment_id}/${variable_id}/${model}/${wget_script_name}" "${url}"

    # Echo the wget script
    echo "[INFO] wget script: ${wget_scripts_dir}/${wget_script_name}"

    if grep -q "No files were found that matched the query" "${wget_scripts_dir}/${wget_script_name}"; then
        echo "Removing ${wget_scripts_dir}/${wget_script_name} because it contains the string 'No files were found that matched the query'"
        rm "${wget_scripts_dir}/${wget_script_name}"
    else
        echo "[INFO] wget script exists"
        echo "[INFO] url: ${url}"

        # Make the wget script executable
        chmod +x "${wget_scripts_dir}/${wget_script_name}"
    fi

done