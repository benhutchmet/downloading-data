# Import the relevant libraries
from pyesgf.search import SearchConnection
import os
import pandas as pd
import requests
from tqdm import tqdm
from typing import List, Dict, Union
from pyesgf.search import ResultSet

# Set the environment variables
os.environ["ESGF_PYCLIENT_NO_FACETS_STAR_WARNING"] = "on"


def query_data_esgf(connection: SearchConnection,
                    source_id: str,
                    experiment_id: str,
                    variable_id: str,
                    table_id: str = 'Amon',
                    project: str = 'CMIP6',
                    member_id: str = None,
                    data_node: str = None,
                    latest: bool = True) -> ResultSet:
    """
    Query the ESGF database for a given dataset.

    Parameters
    ----------
    connection : SearchConnection
        Connection to the ESGF database.
    source_id : str
        Model name. E.g. 'CanESM5'.
    experiment_id : str
        Experiment name. E.g. 'historical'.
    variable_id : str
        Variable name. E.g. 'tas'.
    table_id : str, optional
        Table name. E.g. 'Amon'. The default is 'Amon'.
    project : str, optional
        Project name. E.g. 'CMIP6'. The default is 'CMIP6'.
    member_id : str, optional
        Member name. E.g. 'r1i1p1f1'. The default is None.
    data_node : str, optional
        Data node name. E.g. 'crd-esgf-drc.ec.gc.ca'. The default is None.
    latest : bool, optional
        Whether to download the latest version of the dataset. The default is True.
        
    Returns
    -------
    results : ResultSet
        List of dictionaries containing the dataset information.
    """

    # Define common parameters
    params = {
        "latest": latest,
        "project": project,
        "source_id": source_id,
        "experiment_id": experiment_id,
        "variable_id": variable_id,
        "table_id": table_id
    }

    # Add optional parameters if they are not None
    if member_id is not None:
        params["member_id"] = member_id
    if data_node is not None:
        params["data_node"] = data_node

    # Query the database
    query = connection.new_context(**params)

    # Get the results
    results = query.search()

    return results

# Write a function which will extract the file context from the results
# and return a list of dictionaries containing the file name and download 
# URL
def extract_file_context(results: ResultSet) -> list[dict]:
    """
    Extract the file context from the results.
    Save the file name and download URL in a list of dictionaries.
    
    Parameters
    ----------
    results : ResultSet
        List of dictionaries containing the dataset information.
        
    Returns
    -------
    files_list : list[dict]
        List of dictionaries containing the file name and download URL.
    """

    # Initialise an empty list to store the results
    hit_list = []

    print("Extracting file context for " + str(len(results)) + " datasets...")
    # Loop over the results to extract the file context
    for i in range(len(results)):
        try:
            # Extract the file context
            hit = results[i].file_context().search()

            # Append the results to the list
            hit_list.append(hit)
        except:
            print(f"Error: {results[i]}")
            continue
    
    # Use map to extract the file name and download URL from the list of dictionaries
    files_list = list(
        map(
            lambda f: {
                "file_name": f.filename,
                "download_url": f.download_url
            }, 
            hit_list
        )
    )

    return files_list