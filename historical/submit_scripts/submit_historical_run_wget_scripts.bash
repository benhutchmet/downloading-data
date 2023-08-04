#!/bin/bash
#
# Submits the wget scripts to download the historical data
#
# Usage: bash historical_submit_historical_data_download_wget_scripts.bash <variable> <experiment_id>
#
# Example: bash historical_submit_historical_data_download_wget_scripts.bash tas historical
#

# Source the dictionaries
source "/home/users/benhutch/downloading-data/dictionaries.bash"

# Check that the correct number of arguments were provided
if [ "$#" -ne 2 ]; then
    echo "Illegal number of arguments provided"
    echo "Usage: bash historical_submit_historical_data_download_wget_scripts.bash <variable> <experiment_id>"
    echo "Example: bash historical_submit_historical_data_download_wget_scripts.bash tas historical"
    exit 1
fi

# Check that the variable provided is valid
# must be either: psl, sfcWind, tas or rsds
if [ "$1" != "psl" ] && [ "$1" != "sfcWind" ] && [ "$1" != "tas" ] && [ "$1" != "rsds" ] && [ "$1" != "tos" ]; then
    echo "Invalid variable provided"
    echo "Usage: bash historical_submit_historical_data_download_wget_scripts.bash <variable> <experiment_id>"
    echo "Example: bash historical_submit_historical_data_download_wget_scripts.bash tas historical"
    exit 1
fi

# Extract the variable name from the command line argument
variable_id=$1
experiment_id=$2

# Set up the extractor script
EXTRACTOR="/home/users/benhutch/downloading-data/historical/process_scripts/historical_run_wget_scripts.bash"

# Echo the variable name
echo "[INFO] looping over the models and data nodes for the variable: ${variable_id}"

# Loop over the models
for model in "${models[@]}"; do
    
    # Echo the current model
    echo "[INFO] model: ${model}"

    # Loop over the data nodes
    for data_node in "${data_nodes[@]}"; do
    # Echo the current data node
    echo "[INFO] data node: ${data_node}"

    # Set up the output directory
    OUTPUT_DIR="${canari_dir}/${experiment_id}/${variable_id}/${model}/lotus-output"
    mkdir -p "${OUTPUT_DIR}"

    # Set up the output and error files
    OUTPUT_FILE="${OUTPUT_DIR}/${model}_${data_node}_${variable_id}_run_wget_scripts.out"
    ERROR_FILE="${OUTPUT_DIR}/${model}_${data_node}_${variable_id}_run_wget_scripts.err"

    # Run the extractor script
    sbatch -p short-serial -t 15:00 -o "${OUTPUT_FILE}" -e "${ERROR_FILE}" ${EXTRACTOR} ${variable_id} ${model} ${data_node} ${experiment_id}

    done

done