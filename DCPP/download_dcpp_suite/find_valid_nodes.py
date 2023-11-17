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

    python find_valid_nodes.py <variable> <experiment> <sub_experiment_id>

Parameters:
-----------

    variable: str
        The variable to download. e.g. tas, rsds, pr, etc.
    experiment: str
        The experiment to download. e.g. historical, dcppA-hindcast, etc.
    sub_experiment_id: str
        The sub_experiment_id to download. e.g. s1960, s1961, etc.

"""

# Local imports
import os
import sys

# Third-party imports
from pyesgf.search import SearchConnection
import pandas as pd
import requests
import tqdm as tqdm
from datetime import datetime

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

# Define a function to find the valid nodes for each model
def find_valid_nodes_model_list(variable: str, models_list: list,
                                params: dict, conn: SearchConnection):
    """
    Function which given a list of valid models finds
    the valid nodes for each model at present on ESGF.
    
    ---warning---
    The valid nodes will change with time.
    
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
    
        valid_nodes: dict
            A dictionary of valid nodes for each model.
    """

    # Create a dictionary for the valid nodes
    valid_nodes = {}

    # Loop over the models
    for model in tqdm.tqdm(models_list):
        print(f"Finding valid nodes for {model} for {variable} and {params['experiment_id']}")

        # Set up the variable_id for the params
        params['variable_id'] = variable
        params['source_id'] = model

        # Append the model to the dictionary
        valid_nodes[model] = []

        # Find the valid nodes
        valid_nodes[model] = find_valid_nodes(params=params,
                                                models_list=[model],
                                                conn=conn)
    # Return the valid nodes
    return valid_nodes

# Define a function which given valid nodes will extract each model dictionary
# into a single row for a .csv file
def extract_valid_nodes_to_csv(valid_nodes: dict):
    """
    Function which given a dictionary of valid nodes
    for each model will extract each model dictionary
    into a single row for a .csv file.

    Parameters:
    -----------

        valid_nodes: dict
            A dictionary of valid nodes for each model.

    Returns:
    --------

        valid_nodes_df: pd.DataFrame
            A dataframe of the valid nodes for each model.
    """

    # Initialise an empty dataframe
    df = pd.DataFrame()

    # Loop through the valid nodes dictionary
    # using tqdm to show progress
    for model, nodes in tqdm.tqdm(valid_nodes.items()):
        # Create a dataframe for the model
        model_row = pd.DataFrame.from_dict(nodes)

        # Concatenate the model row to the dataframe
        df = pd.concat([df, model_row], ignore_index=True)

    # Return the dataframe
    return df


if __name__ == "__main__":

    # Set up the search connection
    conn = SearchConnection(dic.search_connection, distrib=True)

    try:
        # Get the variable and experiment from the command line
        variable = sys.argv[1]
        experiment = sys.argv[2]
        sub_experiment_id = sys.argv[3]
    except:
        raise ValueError("Please provide a variable and experiment")

    # Set up the parameters
    # FIXME: Fix hard coding for suite run
    params = {
        'activity_id': 'DCPP', # Hard coded for now
        'experiment_id': experiment,
        'latest': True,
        'sub_experiment_id': sub_experiment_id,
        'project': 'CMIP6',
        'table_id': 'Amon',
    }

    # Call the function to get the valid models
    valid_models, results_df = check_models_exist(variable, 
                                                    dic.var_models[variable], 
                                                    params, 
                                                    conn)

    # Print the valid models
    print(f"The valid models for {variable} and {experiment} are:")
    print(valid_models)

    # Now we have the valid models we can find the valid nodes for each model
    valid_nodes = find_valid_nodes_model_list(variable, 
                                                valid_models, 
                                                params, 
                                                conn)
    
    # Print the valid nodes
    print(f"The valid nodes for {variable} and {experiment} are:")
    print(valid_nodes)

    # Now once we have the valid nodes we want to extract these into a .csv
    # format to save them for the other scripts
    valid_nodes_df = extract_valid_nodes_to_csv(valid_nodes)

    # Print the dataframe
    print(valid_nodes_df)

    # Set up the directory to save the .csv file
    save_dir = "/gws/nopw/j04/canari/users/benhutch/dcppA-hindcast/download_data/"

    # If the directory doesn't exist then create it
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    # Set up the current date
    current_date = datetime.now().strftime("%Y%m%d")

    # Create a filename for the .csv file
    filename = f"{variable}_{experiment}_{sub_experiment_id}_valid_nodes_{current_date}.csv"

    # Form the full path
    full_path = os.path.join(save_dir, filename)

    # Save the dataframe without the index
    valid_nodes_df.to_csv(full_path, index=False)