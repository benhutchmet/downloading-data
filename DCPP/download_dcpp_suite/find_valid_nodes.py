#!/usr/bin/env python3

"""
find_valid_nodes.py
Author: Ben Hutchins
=====================

A script which checks that the models exist
on ESGF for a given variable and experiment.
Then creates a list of models and valid nodes
which is used to download the data from ESGF.

Usage:
------

    python find_valid_nodes.py <variable> <experiment>

Parameters:
-----------

    variable: str
        The variable to download. e.g. tas, rsds, pr, etc.
    experiment: str
        The experiment to download. e.g. historical, dcppA-hindcast, etc.

"""

# Local imports
import os
import sys

# Third-party imports
from pyesgf.search import SearchConnection
import pandas as pd
import requests
import tqdm as tqdm

# Set the environment to on
os.environ['ESGF_PYCLIENT_NO_FACETS_STAR_WARNING'] = "on"

# Import the functions
sys.path.append('/home/users/benhutch/downloading-data/DAMIP/')

# Import the functions
from testing_download_functions import find_valid_nodes

# Import the dictionaires
sys.path.append('/home/users/benhutch/downloading-data/DCPP/')

# Import the dictionaries
import dictionaries as dic

# Define a function to loop over the models and check
# that they exist on ESGF
def check_models_exist(variable: str, models_list: list,
                       params: dict, conn: SearchConnection):
    """
    Check that the models exist on ESGF. For a specific
    variable and experiment.
    
    Parameters:
    -----------
    
        variable: str
            The variable to download. e.g. tas, rsds, pr, etc.
        models_list: list
            A list of models to check.
        params: dict
            A dictionary of parameters to pass to the search.
        conn: SearchConnection
            A search connection to ESGF.
            
    Returns:
    --------
    
        valid_models: list
            A list of models which exist on ESGF.
        results_df: pd.DataFrame
            A dataframe of the results.
    """
    
    # Create a dataframe for the results
    results_df = pd.DataFrame(columns=['model', 'valid'])

    # Create an empty list for the valid models
    valid_models = []

    # Loop over the models
    for model in models_list:
        print(f"Checking {model} for {variable} and {params['experiment_id']}")

        # Set up the params
        params['variable_id'] = variable
        params['source_id'] = model

        # Query ESGF
        ctx = conn.new_context(**params)

        # Get the number of results
        try:
            results = ctx.search()
        except:
            print(f"Model {model} not found for {variable} and {params['experiment_id']}")
            results = []

        # If the len results > 0 then the model exists
        if len(results) > 0:
            results_df = pd.concat([results_df, pd.DataFrame([{'model': model, 'valid': True}])], ignore_index=True)

            # Append to the valid models list
            valid_models.append(model)
        else:
            results_df = pd.concat([results_df, pd.DataFrame([{'model': model, 'valid': False}])], ignore_index=True)

    # Return the valid models
    return valid_models, results_df


if __name__ == "__main__":

    # Set up the search connection
    conn = SearchConnection(dic.search_connection, distrib=True)

    try:
        # Get the variable and experiment from the command line
        variable = sys.argv[1]
        experiment = sys.argv[2]
    except:
        raise ValueError("Please provide a variable and experiment")

    # Set up the parameters
    # FIXME: Fix hard coding for suite run
    params = {
        'activity_id': 'DCPP', # Hard coded for now
        'experiment_id': experiment,
        'latest': True,
        'sub_experiment_id': 's1961', # FOR TESTING
        'project': 'CMIP6',
        'table_id': 'Amon',
    }

    # Call the function to get the valid models
    valid_models, results_df = check_models_exist(variable, dic.var_models[variable], params, conn)

    # Print the valid models
    print(f"The valid models for {variable} and {experiment} are:")
    print(valid_models)