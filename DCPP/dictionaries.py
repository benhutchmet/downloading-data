# Dictionaries for DCPP
search_connection = 'https://esgf-node.llnl.gov/esg-search'

# List of variables
variables = ['tas','psl', 'uas', 'vas', 'rsds']

variables_test = ['rsds']

models = [ "BCC-CSM2-MR", "MPI-ESM1-2-HR", "CanESM5", "CMCC-CM2-SR5", "HadGEM3-GC31-MM", "EC-Earth3", "MPI-ESM1-2-LR", "FGOALS-f3-L", "MIROC6", "IPSL-CM6A-LR", "CESM1-1-CAM5-CMIP5", "NorCPM1" ]

# models without MPI-ESM1-2-LR and CESM1-1-CAM5-CMIP5
models_uas_vas = [ "BCC-CSM2-MR", "MPI-ESM1-2-HR", "CanESM5", "CMCC-CM2-SR5", "HadGEM3-GC31-MM", "EC-Earth3", "FGOALS-f3-L", "MIROC6", "IPSL-CM6A-LR", "NorCPM1" ]

# RSDS not valid for MPI-ESM1-2-LR
models_rsds = [ "BCC-CSM2-MR", "MPI-ESM1-2-HR", "CanESM5", "CMCC-CM2-SR5", "HadGEM3-GC31-MM", "EC-Earth3", "FGOALS-f3-L", "MIROC6", "IPSL-CM6A-LR", "CESM1-1-CAM5-CMIP5", "NorCPM1" ]

# variable models list dictionary
var_models = {
    'tas': models,
    'psl': models,
    'uas': models_uas_vas,
    'vas': models_uas_vas,
    'rsds': models_rsds
}

# var_models test
var_models_test = {
    'rsds': models_rsds
}

# Set up the path for the downloaded data
download_csv_path = "/gws/nopw/j04/canari/users/benhutch/dcppA-hindcast/download_data/"

dcpp_dir_badc = "/badc/cmip6/data/CMIP6/DCPP/"

# Now set up the group work space directory
dcpp_dir_gws = "/gws/nopw/j04/canari/users/benhutch/"