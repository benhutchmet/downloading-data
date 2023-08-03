#!/bin/bash

# Script for downloading the wget scripts for historical data from the ESGF server
#
# Usage: bash historical_data_download_wget_scripts.bash <variable>
#
# Example: bash historical_data_download_wget_scripts.bash tas
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
if [ "$1" != "psl" ] && [ "$1" != "sfcWind" ] && [ "$1" != "tas" ] && [ "$1" != "rsds" ]; then
    echo "Invalid variable provided"
    echo "Usage: bash historical_data_download_wget_scripts.bash <variable>"
    echo "Example: bash historical_data_download_wget_scripts.bash tas"
    exit 1
fi

# Extract the variable name from the command line argument
variable_id=$1

# Set up the constants for the url to be used
activity_id="CMIP"
latest="true"
project="CMIP6"
experiment_id="historical"
table_id="Amon"

# Echo the string we are looking for
echo "[INFO] models: ${historical_models[@]}"
echo "[INFO] data nodes: ${data_nodes[@]}"
echo "[INFO] variable: ${variable_id}"
echo "[INFO] activity_id: ${activity_id}"
echo "[INFO] latest: ${latest}"
echo "[INFO] project: ${project}"
echo "[INFO] experiment_id: ${experiment_id}"
echo "[INFO] table_id: ${table_id}"