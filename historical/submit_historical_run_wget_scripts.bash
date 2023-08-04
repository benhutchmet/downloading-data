#!/bin/bash
#
# Submits the wget scripts to download the historical data
#
# Usage: bash historical_submit_historical_data_download_wget_scripts.bash <variable>
#
# Example: bash historical_submit_historical_data_download_wget_scripts.bash tas
#

# Source the dictionaries
source "/home/users/benhutch/downloading-data/dictionaries.bash"

# Check that the correct number of arguments were provided
if [ "$#" -ne 1 ]; then
    echo "Illegal number of arguments provided"
    echo "Usage: bash historical_submit_historical_data_download_wget_scripts.bash <variable>"
    echo "Example: bash historical_submit_historical_data_download_wget_scripts.bash tas"
    exit 1
fi

# Check that the variable provided is valid
# must be either: psl, sfcWind, tas or rsds
if [ "$1" != "psl" ] && [ "$1" != "sfcWind" ] && [ "$1" != "tas" ] && [ "$1" != "rsds" ]; then
    echo "Invalid variable provided"
    echo "Usage: bash historical_submit_historical_data_download_wget_scripts.bash <variable>"
    echo "Example: bash historical_submit_historical_data_download_wget_scripts.bash tas"
    exit 1
fi

# Extract the variable name from the command line argument
variable_id=$1

# Set up the extractor script
EXTRACTOR="/home/users/benhutch/downloading-data/historical/historical_run_wget_scripts.bash"

# Echo the variable name
echo "[INFO] looping over the models and data nodes for the variable: ${variable_id}"

# Loop over the models
for model in "${historical_models[@]}"; do
    # Echo the current model
    echo "[INFO] model: ${model}"

    # Loop over the data nodes
    for data_node in "${data_nodes[@]}"; do
    # Echo the current data node
    echo "[INFO] data node: ${data_node}"

    # Set up the output directory
    OUTPUT_DIR="${wget_scripts_dir}/${variable_id}/${model}/lotus-output"
    mkdir -p "${OUTPUT_DIR}"

    # Set up the output and error files
    OUTPUT_FILE="${OUTPUT_DIR}/${model}_${data_node}_${variable_id}_run_wget_scripts.out"
    ERROR_FILE="${OUTPUT_DIR}/${model}_${data_node}_${variable_id}_run_wget_scripts.err"

    # Run the extractor script
    sbatch -p short-serial -t 10:00 -o "${OUTPUT_FILE}" -e "${ERROR_FILE}" ${EXTRACTOR} ${variable_id} ${model} ${data_node}

    done
done