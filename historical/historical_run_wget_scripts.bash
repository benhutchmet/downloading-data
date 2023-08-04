#!/bin/bash
#
# Runs the wget scripts to download the historical data
#
# Usage: bash historical_run_wget_scripts.bash <variable> <model> <data_node> <experiment_id>
#
# Example: bash historical_run_wget_scripts.bash tas NorCPM1 esgf-data1.llnl.gov historical
#

# Source the dictionaries
source "/home/users/benhutch/downloading-data/dictionaries.bash"

# Check that the correct number of arguments were provided
if [ "$#" -ne 4 ]; then
    echo "Illegal number of arguments provided"
    echo "Usage: bash historical_run_wget_scripts.bash <variable> <model> <data_node> <experiment_id>"
    echo "Example: bash historical_run_wget_scripts.bash tas NorCPM1 esgf-data1.llnl.gov"
    exit 1
fi

# Check that the variable provided is valid
# must be either: psl, sfcWind, tas or rsds
if [ "$1" != "psl" ] && [ "$1" != "sfcWind" ] && [ "$1" != "tas" ] && [ "$1" != "rsds" ] && [ "$1" != "tos" ]; then
    echo "Invalid variable provided"
    echo "Usage: bash historical_run_wget_scripts.bash <variable>"
    echo "Example: bash historical_run_wget_scripts.bash tas"
    exit 1
fi

# Extract the variable name from the command line argument
variable_id=$1
model=$2
data_node=$3

# NorCPM1_esgf-data1.llnl.gov_tas.wget
# Find the wget script
wget_script="${wget_scripts_dir}/${variable_id}/${model}_${data_node}_${variable_id}.wget"

# Set the data directory
data_dir="${data_dir}/${variable_id}/${model}/"
mkdir -p "${data_dir}"

# Check that the wget script exists
if [ ! -f "${wget_script}" ]; then
    echo "The wget script does not exist: ${wget_script}"
    exit 1
fi

# Function to display the elapsed time
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

# Run the wget script
# first echo which wget script we are running
echo "[INFO] running wget script: ${wget_script}"
# then run the wget script
script_start_time=$(date +%s)
cd "${data_dir}" && bash "${wget_script}" -s
script_end_time=$(date +%s)
script_duration=$((script_end_time-script_start_time))
echo "[INFO] script duration: $(display_duration ${script_duration})"

