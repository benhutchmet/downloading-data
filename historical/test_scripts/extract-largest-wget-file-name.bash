#!/bin/bash
# test file
# usage: extract-largest-wget-file-name.bash <model> <variable> <experiment_id>
#
# example: extract-largest-wget-file-name.bash BCC-CSM2-MR sfcWind dcppA-hindcast

# extract the model, variable and experiment_id from the command line
model=$1
variable_id=$2
experiment_id=$3

# get the dir
wget_script_dir="/gws/nopw/j04/canari/users/benhutch/${experiment_id}/${variable_id}/${model}"

# get the largest file
largest_file=$(ls -S ${wget_script_dir} | head -1)

# echo the name of the largest file
echo ${largest_file}

# We want to split the file by "_"
#  BCC-CSM2-MR_esgf-data1.llnl.gov_sfcWind.bash
# and extract the 2nd element
# esgf-data1.llnl.gov
# which is the data node
data_node=$(echo ${largest_file} | cut -d'_' -f2)

# echo the data node
echo ${data_node}


