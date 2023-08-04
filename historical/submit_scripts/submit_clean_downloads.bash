#!/bin/bash
#
# Submits jobs to remove empty files
#
# Usage: bash submit_clean_downloads.bash <variable> <experiment_id>
#
# Example: bash submit_clean_downloads.bash tas historical
#

# Source the dictionaries
source "/home/users/benhutch/downloading-data/dictionaries.bash"

# Check that the correct number of arguments were provided is correct
if [ "$#" -ne 2 ]; then
    echo "Illegal number of arguments provided"
    echo "Usage: bash submit_clean_downloads.bash <variable> <experiment_id>"
    echo "Example: bash submit_clean_downloads.bash tas historical"
    exit 1
fi

# Check that the variable provided is valid
# must be either: psl, sfcWind, tas or rsds or tos
if [ "$1" != "psl" ] && [ "$1" != "sfcWind" ] && [ "$1" != "tas" ] && [ "$1" != "rsds" ] && [ "$1" != "tos" ]; then
    echo "Invalid variable provided"
    echo "Usage: bash submit_clean_downloads.bash <variable> <experiment_id>"
    echo "Example: bash submit_clean_downloads.bash tas historical"
    exit 1
fi

# Extract the variable name from the command line argument
# Also extract the experiment_id
variable_id=$1
experiment_id=$2

# Set up the extractor script
EXTRACTOR="/home/users/benhutch/downloading-data/historical/process_scripts/clean_downloads.bash"

# Check that the extractor script exists
if [ ! -f "${EXTRACTOR}" ]; then
    echo "The extractor script does not exist: ${EXTRACTOR}"
    exit 1
fi

# Echo the variable name
echo "[INFO] looping over the models for the variable: ${variable_id} and experiment_id: ${experiment_id}"

# Loop over the models
for model in "${models[@]}"; do

    # Echo the current model
    echo "[INFO] model: ${model}"

    # Set up the output directory for the LOTUS scripts
    OUTPUT_DIR="${canari_dir}/${experiment_id}/${variable_id}/${model}/lotus-output"
    mkdir -p "${OUTPUT_DIR}"

    # Set up the output and error files
    OUTPUT_FILE="${OUTPUT_DIR}/${model}_${variable_id}_clean_downloads.out"
    ERROR_FILE="${OUTPUT_DIR}/${model}_${variable_id}_clean_downloads.err"

    # Run the extractor script
    sbatch -p short-serial -t 08:00 -o "${OUTPUT_FILE}" -e "${ERROR_FILE}" ${EXTRACTOR} ${variable_id} ${experiment_id} ${model}

done