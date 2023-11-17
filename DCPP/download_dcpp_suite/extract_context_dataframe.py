#!/usr/bin/env python3

"""
extract_context_dataframe.py
Author: Ben Hutchins
=====================

A script which:

* For each model and data node pair (loaded from a csv file) extracts
    the results of the search from ESGF.

* Extracts the file context for each model and data node pair.

* Creates a dataframe containing all of the variable, filename and url
    context for each model and data node pair.

* Checks whether these files exist on JASMIN (using the url context).
    Either in /badc/ or /gws/.

* Adds whether the file exists on JASMIN to the dataframe.

* Formats and saves the dataframe as a csv file.

Usage:
------

    python extract_context_download.py <variable> <experiment> <sub_experiment_id>

Parameters
----------

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
from datetime import timedelta

# Set the environment to on
os.environ['ESGF_PYCLIENT_NO_FACETS_STAR_WARNING'] = "on"

# Import the functions
sys.path.append('/home/users/benhutch/downloading-data/DAMIP/')

# Python
from testing_download_functions import find_valid_nodes, \
                                       check_file_exists_jasmin, \
                                       extract_file_context, \
                                       create_results_list

# Import the dictionaires
sys.path.append('/home/users/benhutch/downloading-data/DCPP/')

# Import the dictionaries
import dictionaries as dic

# Define a function to call the function 'extract_file_context'
# to extract the file context for each result (for each model and
# data node pair)
def extract_file_context_for_results_list(results_list: list):
    """
    Extract the file context for each result in a results list.
    
    Parameters:
    -----------
    
        results_list: list
            A list of results from a search.
        connection: SearchConnection
            A search connection to ESGF.
    
    Returns:
    --------
    
        results_list: list
            A list of results from a search, with the file context
            added.
    """
    
    # Initialise the results list
    file_context_list = []

    # Initialise the failed results list
    failed_results_list = []

    # Loop over the results list
    for result in tqdm.tqdm(results_list):
        
        # Extract the file context
        file_context, failed_results = extract_file_context(result)
        
        # Append to the file context list
        file_context_list.append(file_context)

        # Append to the failed results list
        failed_results_list.append(failed_results)

    # Return the results list
    return file_context_list, failed_results_list

# Define a function which creates the dataframe
# which we append the file context to
def create_dataframe(file_context_list: list,
                     failed_results_list: list):
    """
    Create a dataframe from the file context list.

    Parameters:
    -----------
    
        file_context_list: list
            A list of file contexts.
        failed_results_list: list
            A list of failed results.

    Returns:
    --------
    
        df: pd.DataFrame
            A dataframe of the file context.
    """

    # Create an empty dataframe
    df = pd.DataFrame()

    # TODO: Sort out failed results list
    # Assert that all of the lists within the failed results list
    # are empty
    assert all([len(failed_results) == 0 for failed_results in failed_results_list]), \
        "There are failed results in the failed results list."

    # Loop over the file context list
    for file_context in tqdm.tqdm(file_context_list):

        # create a dataframe from the file context dictionary
        file_context_df = pd.DataFrame.from_dict(file_context)

        # Concatenate the dataframe to the main dataframe
        df = pd.concat([df, file_context_df], ignore_index=True)

    # Return the dataframe
    return df

# Set up the main function
if __name__ == "__main__":
    
    # Set up the search connection
    conn = SearchConnection(dic.search_connection, distrib=True)

    # Try to get the variable and experiment from the command line
    try:
        variable = sys.argv[1]
        experiment = sys.argv[2]
        sub_experiment_id = sys.argv[3]
    except IndexError:
        raise IndexError("Please provide a variable and experiment and sub experiment to download.")

    # Find the csv containing the model and data node pairs
    # form the filename
    current_date = datetime.now().strftime("%Y%m%d")
    
    # FIXME: Minus 1 from the day for testing
    current_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    # Set up the filename
    filename = f"{variable}_{experiment}_valid_nodes_{current_date}.csv"

    # Set up the path
    path = os.path.join(dic.download_csv_path, filename)

    # Read in the csv as a dataframe
    df = pd.read_csv(path)

    # Print the dataframe
    print(df)

    # Set up the parameters for the search
        # Set up the parameters
    # FIXME: Fix hard coding for suite run
    params = {
        'activity_id': 'DCPP', # Hard coded for now
        'experiment_id': experiment,
        'latest': True,
        'sub_experiment_id': sub_experiment_id,
        'project': 'CMIP6',
        'table_id': 'Amon',
        'variable_id': variable
    }

    # Create the results list
    results_list = create_results_list(params=params,
                                        max_results_list=df,
                                        connection=conn)

    # Print the results list
    print(results_list)

    # Now we want to extract the file context for each result
    file_context_results, \
    failed_results_list = extract_file_context_for_results_list(
                                                results_list=results_list)
    
    # Print the failed results list
    print(failed_results_list)

    # Print the file context results
    print(file_context_results)

    # Now we want to add the file context to the dataframe
    file_context_df = create_dataframe(file_context_list=file_context_results,
                                        failed_results_list=failed_results_list)
    
    # # Print the file context dataframe
    # print(file_context_df)

    # Now we want to check whether the files exist on JASMIN
    # in both the /badc/ and /gws/ directories
    # using the check_file_exists_jasmin function
    # first for the /badc/ directory
    file_context_df = check_file_exists_jasmin(df=file_context_df,
                                                directory=dic.dcpp_dir_badc)
    
    # # Print the file context dataframe
    # print(file_context_df)

    #FIXME: Fix issue with overwriting whether the file exists on JASMIN
    # for example, the file may exist on JASMIN in the /badc/ directory
    # but not in the /gws/ directory, but the /gws/ directory will overwrite
    # the /badc/ directory, giving false for file exists on JASMIN
    # Now for the /gws/ directory
    file_context_df = check_file_exists_jasmin(df=file_context_df,
                                                directory=dic.dcpp_dir_gws)
    
    # # Print the file context dataframe
    # print(file_context_df)

    # Now we have the dataframe we want to save it as a csv file
    # Set up the save_dir
    save_dir = dic.download_csv_path

    # If the directory doesn't exist then create it
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    # Set up the current date
    current_date = datetime.now().strftime("%Y%m%d")

    # Create a filename for the .csv file
    filename = f"{variable}_{experiment}_file_context_{current_date}.csv"

    # Form the full path
    path = os.path.join(save_dir, filename)

    # Save the dataframe as a csv file
    file_context_df.to_csv(path, index=False)