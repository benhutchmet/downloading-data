#!/usr/bin/env python3

"""
download_missing_files.py
Author: Ben Hutchins
=====================

Loads up a csv file containing the results of the search (filename and url) and
whether the file exists on JASMIN. Then downloads the files which do not exist
on JASMIN and appends the results to the dataframe.

Script which:

* Loads up a csv file containing the results of the search (filename and url)
    and whether the file exists on JASMIN.

* Downloads the files which do not exist on JASMIN and appends the results to
    the dataframe.

* Formats and saves the dataframe as a csv file. This is a central file which
    contains the filenames, urls, file_exists and directories on JASMIN of the 
    files on ESGF. This is updated each time the script is run. One of these 
    exists for each variable and experiment combination.

Usage:
------

    python download_missing_files.py <variable> <experiment> <current_date>
                                        <sub_experiment_id>

Parameters:
-----------

    variable: str
        The variable to download. e.g. tas, rsds, pr, etc.
    experiment: str
        The experiment to download. e.g. historical, dcppA-hindcast, etc.
    current_date: str
        The current date. format: YYYYMMDD, e.g. 20231117
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
from requests.models import Response
from requests.exceptions import HTTPError

# For retrying the request
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Set the environment to on
os.environ['ESGF_PYCLIENT_NO_FACETS_STAR_WARNING'] = "on"

# Import the functions
sys.path.append('/home/users/benhutch/downloading-data/DAMIP/')

# Specific imports for the functions
from testing_download_functions import *

# Import the dictionaires
sys.path.append('/home/users/benhutch/downloading-data/DCPP/')

# Import the dictionaries
import dictionaries as dic

# Function to find the csv file
def find_load_csv(variable: str,
                experiment: str,
                sub_experiment_id: str,
                current_date: str,
                save_dir: str):
    """
    Find the csv file containing the results of the search.

    Parameters:
    -----------

        variable: str
            The variable to download. e.g. tas, rsds, pr, etc.
        experiment: str
            The experiment to download. e.g. historical, dcppA-hindcast, etc.
        sub_experiment_id: str
            The sub_experiment_id to download. e.g. s1960, s1961, etc.
        current_date: str
            The current date. format: YYYYMMDD, e.g. 20231117
        save_dir: str
            The directory in which the csv file is saved.

    Returns:
    --------

        df: pd.DataFrame
            A dataframe for the extracted csv file.
    """

    # Set up the filename
    filename = f"{variable}_{experiment}_{sub_experiment_id}_file_context_{current_date}.csv"

    # Set up the path
    path = os.path.join(save_dir, filename)

    # Load up the csv file
    df = pd.read_csv(path)

    # Return the dataframe
    return df

# Define a function for downloading the files
# Which don't already exist on JASMIN
def download_files(df: pd.DataFrame,
                   download_dir: str,
                   conn: SearchConnection):
    """
    Download the files which do not already exist on JASMIN.

    Parameters:
    -----------

        df: pd.DataFrame
            A dataframe containing the results of the search.
        download_dir: str
            The directory in which to save the downloaded files.
        conn: SearchConnection
            A search connection to ESGF.

    Returns:
    --------

        df: pd.DataFrame
            A dataframe containing the filenames, urls and updated
            directories of the downloaded files.
    """

    # If the file_exists column only contains True
    if all(df['file_exists'] == True):
        print("All files already exist on JASMIN")
        return df

    # Create a copy of the dataframe
    df_copy = df.copy()

    # Keep only the files which exist on JASMIN
    df_existing_files = df_copy[df_copy['file_exists'] == True]

    # Constrain df to only the files which do not exist on JASMIN
    df_to_download = df_copy[df_copy['file_exists'] == False]

    # Reset the index
    df_to_download.reset_index(drop=True, inplace=True)

    # Loop through the dataframe
    for i in tqdm(range(len(df_to_download))):

        # Extract the file url
        url = df_to_download.loc[i, 'url']

        # Extract the filename
        filename = df_to_download.loc[i, 'filename']

        # Split the filename into its components
        filename_split = filename.split('_')

        # Extract the variable
        variable = filename_split[0]

        # Extract the model
        model = filename_split[2]

        # Extract the experiment
        experiment = filename_split[3]

        # Set up the download path
        # Should download to CANARI pre-exisiting directory
        download_path = os.path.join(download_dir, experiment, "data",
                                        variable, model)
        
        # Check if the directory exists
        if not os.path.exists(download_path):
            os.makedirs(download_path)

        # Set up the full path
        full_path = os.path.join(download_path, filename)

        # Assert that the file doesn't already exist
        # assert not os.path.exists(full_path), "File {} already exists".format(full_path)

        # If the file already exists
        if os.path.exists(full_path):
            # Replace the current filepath with the new filepath
            df_to_download.loc[i, 'filepath'] = full_path

            # Replace the file exists column with true
            df_to_download.loc[i, 'file_exists'] = True

            # Continue to the next iteration
            continue

        # Replace the current filepath with the new filepath
        df_to_download.loc[i, 'filepath'] = full_path

        # Replace the file exists column with true
        df_to_download.loc[i, 'file_exists'] = True

        # EXample url:
        # https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/DCPP/CMCC/CMCC-CM2-SR5/dcppA-hindcast/s1963-r11i1p1f1/Amon/rsds/gn/v20230130/rsds_Amon_CMCC-CM2-SR5_dcppA-hindcast_s1963-r11i1p1f1_gn_196311-197312.nc

        # Create a Session object
        s = requests.Session()

        # Set up the retry factors
        retry = Retry(connect=3, backoff_factor=0.5)

        # Set up the adapter
        adapter = HTTPAdapter(max_retries=retry)

        # Mount the adapter
        s.mount('http://', adapter)
        s.mount('https://', adapter)

        backup_url = False
        backup_backup_url = False
        r_n = None

        try:
            print("Downloading file {}".format(full_path))
            print("First attempt")
            # Set up the request with a timeout of 30 seconds
            r = s.get(url, stream=True, timeout=90, verify=False)
        except requests.exceptions.ConnectionError:
            print("Connection error")
            print("Trying again")
            print("Second attempt")
            print("Using backup url")
            # split the url
            # into its components
            # Find the other valid urls
            # TODO: Find other URLs which are valid
            print("Current url: {}".format(url))
            url_split = url.split('/')
            
            # Within url split, find the index of CMIP6
            project_index = url_split.index('CMIP6')

            # Example url:
            # https://esgf-data1.llnl.gov/thredds/fileServer/css03_data/CMIP6/DCPP/EC-Earth-Consortium/EC-Earth3/dcppA-hindcast/s1962-r5i1p1f1/Amon/vas/gr/v20201216/vas_Amon_EC-Earth3_dcppA-hindcast_s1962-r5i1p1f1_gr_197211-197310.nc

            # Set up a list for the valid nodes
            valid_nodes = []

            # Nodes tried list
            nodes_tried = []

            # Extract and print the current data node
            current_data_node = url_split[2]
            print("Current data node: {}".format(current_data_node))

            # Append the current data node to the nodes tried list
            nodes_tried.append(current_data_node)

            # Set up the sub_experiment_id
            sub_experiment_id_full = url_split[project_index + 5]

            # Split the sub_experiment_id by '-'
            sub_experiment_id_split = sub_experiment_id_full.split('-')

            # Set up the new sub_experiment_id
            sub_experiment_id =  sub_experiment_id_split[0] 

            # Set up the variant label
            variant_label = sub_experiment_id_split[1]

            # Set up the parameters
            params = {
                'activity_id': url_split[project_index + 1],
                'experiment_id': url_split[project_index + 4],
                'latest': True,
                'sub_experiment_id': sub_experiment_id,
                'project': 'CMIP6', # Hardcoded
                'table_id': url_split[project_index + 6],
                'variable_id': variable,
                'source_id': url_split[project_index + 3],
                'variant_label': variant_label,
            }

            # Example url:
            #http://esgf.bsc.es/thredds/fileServer/esg_dataroot/a1ua-DCPP-r3-full/CMIP6/DCPP/EC-Earth-Consortium/EC-Earth3/dcppA-hindcast/s1960-r3i1p1f1/Amon/vas/gr/v20201215/vas_Amon_EC-Earth3_dcppA-hindcast_s1960-r3i1p1f1_gr_196911-197010.nc
            # https://noresg.nird.sigma2.no/thredds/fileServer/css03_data/CMIP6/DCPP/NCC/NorCPM1/dcppA-hindcast/s1964-r3i1p1f1/Amon/vas/gn/v20190914/vas_Amon_NorCPM1_dcppA-hindcast_s1964-r3i1p1f1_gn_196410-197412.nc


            # Query the database
            query = conn.new_context(**params)

            # Try to get the results
            try:
                print("searching for other valid nodes")
                # Get the results
                results = query.search()
            except HTTPError as http_err:
                print(f"HTTPError raised for {url}")
                print(f"HTTP error occurred: {http_err}")
                print("Unlucky")
                print("Trying again")
                print("Second attempt")
                results = query.search()

            # Print the length of the results
            print("Number of results: {}".format(len(results)))

            # Identify the unique nodes which are valid
            data_node_set = set([result.json['data_node'] for result in results])

            # Print the data node set
            print("Data node set: {}".format(data_node_set))

            # Convert to a list and print
            data_node_list = list(data_node_set)
            print("Data node list: {}".format(data_node_list))

            # if the current data node is in the list
            if current_data_node in data_node_list:
                # Remove the current data node from the list
                data_node_list.remove(current_data_node)
            else:
                print("Current data node not in list")

            # If the list is empty
            if not data_node_list:
                # Raise an error
                raise ValueError("No valid data nodes found")
            elif len(data_node_list) == 1:
                # Set the new data node
                new_data_node = data_node_list[0]

                # Replace the current data node with the new data node
                url_split[2] = new_data_node

                # Join the url back together
                url = '/'.join(url_split)
            elif len(data_node_list) > 1:
                # Set the new data node
                new_data_node = data_node_list[0]

                # Replace the current data node with the new data node
                url_split[2] = new_data_node

                # Join the url back together
                url = '/'.join(url_split)

            # Append the new data node to the valid nodes list
            nodes_tried.append(new_data_node)

            # Print the new url
            print("New url: {}".format(url))

            backup_url = True

            try:
                # Set up the request with a timeout of 90 seconds
                r_n = s.get(url, stream=True, timeout=90, verify=False)
            except requests.exceptions.ConnectionError:
                print("Third time lucky")
                # Remove the tried nodes from the list
                data_node_list.remove(node for node in nodes_tried)

                # If the list is empty
                if not data_node_list:
                    raise ValueError("No valid data nodes found")
                elif len(data_node_list) == 1:
                    # Set the new data node
                    new_data_node = data_node_list[0]

                    # Replace the current data node with the new data node
                    url_split[2] = new_data_node

                    # Join the url back together
                    url = '/'.join(url_split)

                    # Print the new url
                    print("New url: {}".format(url))

                    # Set the backup url to True
                    backup_backup_url = True

                    # Set up the request with a timeout of 90 seconds
                    try:
                        r_n = s.get(url, stream=True, timeout=90, verify=False)
                    except requests.exceptions.ConnectionError:
                        raise ValueError("No valid data nodes found - better luck next time")


        # If backup_url is False
        if not backup_url:
            # Assert that r is an instance of Response
            assert isinstance(r, Response), "r is not an instance of Response"
            # Print the type of r
            print("Type of r: {}".format(type(r)))

            # Set up the total size
            total_size = int(r.headers.get('content-length', 0))

        # If backup_url is True
        elif backup_url:
            # Assert that r_n is an instance of Response
            assert isinstance(r_n, Response), "r_n is not an instance of Response"
            # Print the type of r_n
            print("Type of r_n: {}".format(type(r_n)))

            # Set up the total size
            total_size = int(r_n.headers.get('content-length', 0))

        # Set up the chunk size
        block_size = 1024

        # if backup_url is False:
        if not backup_url:
            # Download the file
            with open(full_path, 'wb') as f:
                for data in tqdm(r.iter_content(block_size),
                                total=total_size//block_size, 
                                unit='KiB', 
                                unit_scale=True):
                    f.write(data)

                # If the total size is not equal to zero
                if total_size != 0:
                    print("File {} downloaded successfully".format(full_path))
                    print("File is not empty")
        #elif backup_url is True:
        elif backup_url:
            # Download the file
            with open(full_path, 'wb') as f:
                for data in tqdm(r_n.iter_content(block_size),
                                total=total_size//block_size, 
                                unit='KiB', 
                                unit_scale=True):
                    f.write(data)

                # If the total size is not equal to zero
                if total_size != 0:
                    print("File {} downloaded successfully".format(full_path))
                    print("File is not empty")

    # Assert that all rows in file_exists are True
    assert all(df_to_download['file_exists'] == True), "Not all files downloaded"

    # Assert that the filepath column does not contain any NaNs
    assert not any(df_to_download['filepath'].isna()), "Filepath column contains NaNs"

    # Concatenate the dataframes
    new_df = pd.concat([df_existing_files, df_to_download], ignore_index=True)

    # Assert that all rows in file_exists are True
    assert all(new_df['file_exists'] == True), "Not all files downloaded in full dataframe"

    # Assert that the filepath column does not contain any NaNs
    assert not any(new_df['filepath'].isna()), "Filepath column contains NaNs in full dataframe"

    # Return the dataframe
    return new_df

# If name is main
if __name__ == "__main__":

    # Set up the search connection
    conn = SearchConnection(dic.search_connection, distrib=True)

    # Try to get the variable, experiment and current date from the command line
    try:
        variable = sys.argv[1]
        experiment = sys.argv[2]
        current_date = sys.argv[3]
        sub_experiment_id = sys.argv[4]
    except IndexError:
        raise IndexError("Please provide the variable, experiment and current date.")
    
    # Modify the sub_experiment_id
    sub_experiment_id = f"s{sub_experiment_id}"

    # Load the csv into a dataframe
    df = find_load_csv(variable, experiment,
                        sub_experiment_id, current_date, dic.download_csv_path)

    # # Print the dataframe
    print(df)

    # Download the files
    df = download_files(df, dic.dcpp_dir_gws, conn)

    # Print the dataframe
    print(df)

    # Now we want to save the dataframe
    save_dir = dic.download_csv_path

    # If the directory doesn't exist
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    # Set up the filename
    filename = f"{variable}_{experiment}_file_context_complete.csv"

    # Set up the path
    path = os.path.join(save_dir, filename)

    # If the file does not exist
    if not os.path.exists(path):
        # Save the dataframe
        df.to_csv(path, index=False)
    else:
        # Read in the dataframe
        df_existing = pd.read_csv(path)

        # Concatenate the dataframes
        df = pd.concat([df_existing, df], ignore_index=True)

        # Drop the duplicates in the filename column
        df = df.drop_duplicates(subset=['filename'])

        # Save the dataframe
        df.to_csv(path, index=False)
