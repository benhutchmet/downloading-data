#!/bin/bash
#
# Looks in the folders to see if the data has been downloaded
# and removes empty files
#
# Usage: bash clean_downloads.bash <variable> <experiment_id> <model>
#
# Example: bash clean_downloads.bash tas historical NorCPM1
#

# Source the dictionaries
source "/home/users/benhutch/downloading-data/dictionaries.bash"

# Check that the correct number of arguments were provided
if [ "$#" -ne 3 ]; then
    echo "Illegal number of arguments provided"
    echo "Usage: bash clean_downloads.bash <variable> <experiment_id> <model>"
    echo "Example: bash clean_downloads.bash tas historical NorCPM1"
    exit 1
fi

# Check that the variable provided is valid
# must be either: psl, sfcWind, tas or rsds or tos
if [ "$1" != "psl" ] && [ "$1" != "sfcWind" ] && [ "$1" != "tas" ] && [ "$1" != "rsds" ] && [ "$1" != "tos" ]; then
    echo "Invalid variable provided"
    echo "Usage: bash clean_downloads.bash <variable> <experiment_id> <model>"
    echo "Example: bash clean_downloads.bash tas historical NorCPM1"
    exit 1
fi

# Extract the variable name from the command line argument
# Also extract the experiment_id
variable_id=$1
experiment_id=$2
model=$3

# Find the data directory
data_dir="${canari_dir}/${experiment_id}/${variable_id}/${model}/data"
# Set up the files
files=($(find "${data_dir}" -type f -name "*.nc"))

# Check that the data directory exists
if [ ! -d "${data_dir}" ]; then
    echo "The data directory does not exist: ${data_dir}"
    exit 1
fi

# Function to display elapsed time in a human-readable format
function display_duration() {
    local T=$1
    local D=$((T/60/60/24))
    local H=$((T/60/60%24))
    local M=$((T/60%60))
    local S=$((T%60))
    (( D > 0 )) && printf '%d days ' $D
    (( H > 0 )) && printf '%d hours ' $H
    (( M > 0 )) && printf '%d minutes ' $M
    (( D > 0 || H > 0 || M > 0 )) && printf 'and '
    printf '%d seconds\n' $S
}

# Start a timer
loop_start_time=$(date +%s)
# Loop over the files
for file in "${files[@]}"; do

    # Check if the file is empty
    if [ ! -s "${file}" ]; then
        echo "[INFO] removing empty file: ${file}"
        rm "${file}"
    fi

done
# End the timer
loop_end_time=$(date +%s)

# Calculate the duration
loop_duration=$((loop_end_time-loop_start_time))

# Echo the duration
echo "[INFO] loop duration: $(display_duration ${loop_duration})"

