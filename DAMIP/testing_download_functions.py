# Import the relevant libraries
from pyesgf.search import SearchConnection
import os
import pandas as pd
import requests
from tqdm import tqdm
from typing import List, Dict, Union
from pyesgf.search import ResultSet
from concurrent.futures import ThreadPoolExecutor

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
    files_list = []

    print("Extracting file context for " + str(len(results)) + " datasets...")
    # Loop over the results to extract the file context
    for i in range(len(results)):
        try:
            # Extract the file context
            hit = results[i].file_context().search()

            files = map(lambda f: {'filename': f.filename, 'url': f.download_url}, hit)

            files_list.extend(files)           
        except:
            print(f"Error: {results[i]}")
            continue
    
    return files_list

# Multi thread version of the above function
def extract_file_context_multithread(results: ResultSet) -> list[dict]:
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
    files_list = []

    print("Extracting file context for " + str(len(results)) + " datasets...")

    # Define a function to extract the file context for a single result
    def extract_single_file_context(result):
        try:
            # Extract the file context
            hit = result.file_context().search()

            files = list(map(lambda f: {'filename': f.filename, 'url': f.download_url}, hit))

            return files
        except:
            print(f"Error: {result}")
            return []

    # Use a thread pool to extract the file context for all results
    with ThreadPoolExecutor() as executor:
        files_list = list(executor.map(extract_single_file_context, results))

    # Flatten the list of lists into a single list
    files_list = [file for sublist in files_list for file in sublist]

    return files_list

# Define a function which will download single files
def download_file(url: str, 
                  filename: str,
                  directory: str):
    """
    Downloads a single NetCDF file from ESGF for a given URL
    and filename.
    
    Parameters
    ----------
    url : str
        URL to download the file from.
    filename : str
        Filename to save the file as.
        
    Returns
    -------
    None
    """

    # If the directory does not exist, create it
    if not os.path.isdir(directory):
        os.makedirs(directory)

    # Log the filename and download URL
    print("Downloading " + filename + " from " + url)
    print("Saving to " + directory + filename)

    # Set up the request
    r = requests.get(url, stream=True)

    # Set up the total size
    total_size = int(r.headers.get('content-length', 0))
    
    # Set up the block size
    block_size = 1024

    # Set up the filepath
    filepath = os.path.join(directory, filename)

    # Download the file
    with open(filepath, 'wb') as f:
        for data in tqdm(r.iter_content(block_size),
                            total=total_size//block_size,
                            unit='KiB',
                            unit_scale=True):
                f.write(data)

        # If the total size is not zero
        # and the file size is not equal to the total size
        if total_size != 0:
            print("Downloaded size does not match expected size!\n",
                "FYI, the status code was ", r.status_code)
            

