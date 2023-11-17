# Import the relevant libraries
from pyesgf.search import SearchConnection
import os
import glob
import pandas as pd
import requests
from tqdm import tqdm
from typing import List, Dict, Union
from pyesgf.search import ResultSet
from concurrent.futures import ThreadPoolExecutor

# Set the environment variables
os.environ["ESGF_PYCLIENT_NO_FACETS_STAR_WARNING"] = "on"


# Function to check which models are available on ESGF
def query_models_esgf(experiment_id: str, variable_id: str, table_id: str, activity_id: str, connection: SearchConnection, latest: bool = True, project: str = 'CMIP6',
                      sub_experiment_id: str = None) -> set:
    """
    Query the ESGF database for models based on the given parameters.

    Parameters:
    experiment_id (str): The experiment id to search for.
    variable_id (str): The variable id to search for.
    table_id (str): The table id to search for.
    activity_id (str): The activity id to search for.
    connection (SearchConnection): The ESGF database connection.
    latest (bool, optional): Whether to search for the latest version. Defaults to True.
    project (str, optional): The project to search in. Defaults to 'CMIP6'.
    sub_experiment_id (str, optional): The sub experiment id to search for. Defaults to None.

    Returns:
    set: A set of unique 'source_id' values from the search results.
    """

    # Set up the params for the query
    params = {
        "latest": latest,
        "project": project,
        "experiment_id": experiment_id,
        "variable_id": variable_id,
        "activity_id": activity_id,
        "table_id": table_id
    }

    # Add the sub_experiment_id if it is not None
    if sub_experiment_id is not None:
        params["sub_experiment_id"] = sub_experiment_id

    # Query the database
    query = connection.new_context(**params)

    # Get the results
    results = query.search()

    print("Found " + str(len(results)) + " results.")

    # Form a set of the unique 'source_id' values from the results
    model_list = set(id for result in results for id in result.json['source_id'])

    return model_list


def query_data_esgf(connection: SearchConnection,
                    source_id: str,
                    experiment_id: str,
                    variable_id: str,
                    table_id: str = 'Amon',
                    project: str = 'CMIP6',
                    activity_id: str = None,
                    member_id: str = None,
                    data_node: str = None,
                    sub_experiment_id: str = None,
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
    activity_id : str, optional
        Activity name. E.g. 'CMIP'. The default is None.
    member_id : str, optional
        Member name. E.g. 'r1i1p1f1'. The default is None.
    data_node : str, optional
        Data node name. E.g. 'crd-esgf-drc.ec.gc.ca'. The default is None.
    sub_experiment_id : str, optional
        Sub experiment name. E.g. 's1960'. The default is None.
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
    if activity_id is not None:
        params["activity_id"] = activity_id
    if sub_experiment_id is not None:
        params["sub_experiment_id"] = sub_experiment_id

    # Query the database
    query = connection.new_context(**params)

    # Get the results
    results = query.search()

    return results

# Write a function which will extract the file context from the results
# and return a list of dictionaries containing the file name and download 
# URL
def extract_file_context(results: ResultSet) -> 'list[dict]':
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

    total_results = len(results)
    print(f"Extracting file context for {total_results} datasets...")

    # Keep a list of the results which fail
    failed_results = []

    # Loop over the results to extract the file context
    for i in tqdm(range(total_results)):
        try:
            # Extract the file context
            hit = results[i].file_context().search()

            files = map(lambda f: {'filename': f.filename, 'url': f.download_url}, hit)

            files_list.extend(files)

            # Log the progress
            print(f"Processed {i+1} out of {total_results} results.")           
        except:
            print(f"Error: {results[i]}")

            # Append the result to the failed results list
            failed_results.append(results[i])
            
            continue
    
    return files_list, failed_results

# Multi thread version of the above function
def extract_file_context_multithread(results: ResultSet) -> 'list[dict]':
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
            

# Write a function which given a dataframe containing the file name and download URL
# Will check whether the file exists on JASMIN
def check_file_exists_jasmin(df: pd.DataFrame,
                            directory: str) -> pd.DataFrame:
    """
    Given a dataframe containing the file name and download URL,
    check whether the file exists on JASMIN.
    
    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing the file name and download URL.
    directory : str
        Directory to check for the files.
        E.g. '/badc/cmip6/data/CMIP6/CMIP' for CMIP experiments.
        
    Returns
    -------
    df : pd.DataFrame
        Dataframe containing the file name and download URL.
    """

    # Create a new column in the dataframe to store whether the file exists
    if 'file_exists' not in df.columns:
        df['file_exists'] = False

    # Create a new column in the dataframe to store the file path
    if 'filepath' not in df.columns:
        df['filepath'] = None

    # Loop over the dataframe
    for i in range(len(df)):
        # Get the filename
        filename = df['filename'][i]

        # Split the filename by _
        filename_split = filename.split('_')

        # Extract the variable name
        variable_name = filename_split[0]

        # Extract the time window
        time_window = filename_split[1]

        # Extract the model name
        model_name = filename_split[2]

        # Split the url by /
        # and extract the last 7th element
        # which is the directory name
        url_split = df['url'][i].split('/')

        # Identify the idex for the model name
        model_index = url_split.index(model_name)

        # Extract the element before the model name
        # which is the model group
        model_group = url_split[model_index - 1]

        print(model_group)

        # Extract the experiment name
        experiment_name = filename_split[3]

        # Extract the ensemble member name
        ensemble_member_name = filename_split[4]

        # Extract the grid name
        grid_name = filename_split[5]

        # split the directory by /
        # and extract the first element
        directory_split = directory.split('/')
        
        directory_first = directory_split[1]

        print(directory_first)

        # if the first directory of the directory is badc
        if directory_first == 'badc':
            # Form the pattern
            pattern = os.path.join(directory, model_group, model_name, experiment_name,
                                    ensemble_member_name, time_window, variable_name,
                                    grid_name, "files", "d*", filename)
        elif directory_first == 'gws':
            # Form the pattern
            pattern = os.path.join(directory, experiment_name, "data",
                                    variable_name, model_name, filename)

        # Get a list of all paths that match the pattern
        filepaths = glob.glob(pattern)

        # If filepath is greater than 0, the file exists
        if len(filepaths) > 0:
            print("File exists for " + filename)
            
            if df.loc[i, 'file_exists'] == False:
                df.loc[i, 'file_exists'] = True
                df.loc[i, 'filepath'] = filepaths[0]
            else:
                print("File already exists for " + filename)

        elif len(filepaths) == 0:
            print("File does not exist for " + filename)

            # If the file does not already exist
            # then set file_exists to False
            if df.loc[i, 'file_exists'] == False:
                df.loc[i, 'file_exists'] = False
        elif len(filepaths) > 1:
            print("More than one file found for " + filename)
            AssertionError("More than one file found for " + filename)
        else:
            print("Something went wrong with " + filename)
            AssertionError("Something went wrong with " + filename)

    return df

# Set up a function to find the valid nodes for each model
def find_valid_nodes(params: dict, models_list: set, conn) -> list:
    """
    Find valid nodes for each model in the models_list.

    Parameters:
    params (dict): The parameters for the database query.
    models_list (set): The set of models to search for.
    conn: The database connection.

    Returns:
    list: A list of dictionaries, each containing the 'source_id', 'data_node', and 'num_results' for the model with the most results.
    """
    # Initialize an empty dictionary to store the results
    max_results = {'source_id': None, 'data_node': None, 'num_results': 0}

    # Create a list for the max_results dictionaries
    max_results_list = []

    # Set up the max results per source dictionary
    max_results_per_source = {}

    # Loop through the models_list and query which nodes have data for each model
    for source_id in models_list:
        print("trying to find valid nodes for model: {}".format(source_id))
        # Set the source_id constraint
        params['source_id'] = source_id
        print(params)
        # Query the database
        model_query = conn.new_context(**params)
        # Get the results
        model_results = model_query.search()
        print(len(model_results))

        # Identify the unique nodes (data_node) which have data for the model
        data_node_set = set(result.json['data_node'] for result in model_results)
        print(data_node_set)

        # Initialize max_results for this source_id
        max_results = {'source_id': None, 'data_node': None, 'num_results': 0}

        # Loop through the data_node_set and query how many files are available for each node
        for data_node in data_node_set:
            print("trying to find valid files for node: {}".format(data_node))

            # Set up the params for the query
            params_node = params.copy()
            params_node['data_node'] = data_node
            # Query the database
            node_query = conn.new_context(**params_node)
            # Get the results
            node_results = node_query.search()
            print(len(node_results))

            # If this data_node has more results, update max_results
            if len(node_results) > max_results['num_results']:
                max_results = {'source_id': source_id, 'data_node': data_node, 'num_results': len(node_results)}

        # After checking all data_nodes for this source_id, append max_results to max_results_list
        max_results_list.append(max_results)


    # Assert that there are no duplicate source_id entries in max_results_list
    assert len(max_results_list) == len(set(result['source_id'] for result in max_results_list)), \
    "There are duplicate source_id entries in max_results_list!"

    # Assert that all of the source_id's in models_list are in max_results_list
    assert all(result['source_id'] in models_list for result in max_results_list), \
    "Not all of the source_id's in models_list are in max_results_list!"

    print(max_results_list)
    return max_results_list

# Set up a function to create a list of the matching results for each model
# and node
def create_results_list(params: dict, max_results_list: list,
                        connection: SearchConnection) -> list:
    """
    Create a list of the matching results for each model and node.
    By querying the ESGF database for each model and node and the
    given parameters.
    
    
    Parameters
    ----------
    params : dict
        The parameters for the database query.
    max_results_list : dict
        The list of dictionaries containing the 'source_id', 'data_node', and 'num_results' for the model with the most results.
    connection : SearchConnection
        The database connection.
        
    Returns
    -------
    results_list : list
        A list of dictionaries containing the matching results for each model and node.
    """

    # Assert that the max_results_list is not empty and is a list
    # assert max_results_list != [], "max_results_list is empty!"

    # # Assert that the max_results_list is a list
    # assert isinstance(max_results_list, list), "max_results_list is not a list!"

    # Initialize an empty list to store the results
    results_list = []

    # If the max_results_list is a dictionary, convert it to a list
    if isinstance(max_results_list, dict):
        # Convert the max_results_list to a dataframe
        max_results_df = pd.DataFrame.from_dict(max_results_list)
    elif isinstance(max_results_list, list):
        # Convert the max_results_list to a dataframe
        max_results_df = pd.DataFrame.from_dict(max_results_list)
    elif isinstance(max_results_list, pd.DataFrame):
        max_results_df = max_results_list
    else:
        raise TypeError("max_results_list is not a dictionary or dataframe!")

    # Loop through the max_results_list and query the database for each model and node
    for i in tqdm(range(len(max_results_df))):
        
        # Get the source_id and data_node
        source_id = max_results_df.loc[i, 'source_id']
        data_node = max_results_df.loc[i, 'data_node']

        # Print the source_id and data_node
        print("Querying for source_id: {} and data_node: {}".format(source_id, data_node))

        # Query the database
        result = query_data_esgf(connection=connection,
                                source_id=source_id,
                                experiment_id=params['experiment_id'],
                                variable_id=params['variable_id'],
                                table_id=params['table_id'],
                                project=params['project'],
                                activity_id=params['activity_id'],
                                data_node=data_node,
                                sub_experiment_id=params['sub_experiment_id'],
                                latest=params['latest'])
        
        # Print the number of results
        print("Found {} results.".format(len(result)))

        # Append the results to the results_list
        results_list.append(result)

    return results_list

# Define a function for extracting the file context for each result
def extract_file_context_list(results_list: list) -> list:
    """
    Extract the file context for each result.
    
    Parameters
    ----------
    results_list : list
        A list of dictionaries containing the matching results for each model and node.
        
    Returns
    -------
    files_list : list
        A list of dictionaries containing the file name and download URL.
    """

    # Initialize an empty list to store the results
    files_list = []

    # Loop through the results_list and extract the file context for each result
    for i in range(len(results_list)):
        print("Extracting file context for result {} out of {}.".format(i+1, len(results_list)))
        # Extract the file context
        files = extract_file_context(results_list[i])
        # Append the files to the files_list
        files_list.append(files)

    return files_list

# Define a function for extracting the file context for each result
# and appending it using pandas.concat to a dataframe
def extract_file_context_df(files_list: list) -> pd.DataFrame:
    """
    Extract the file context for each result.
    Append the results to a dataframe.
    
    Parameters
    ----------
    files_list : list
        A list of dictionaries containing the file context for each of the
        model node results.
        
    Returns
    -------
    df : pd.DataFrame
        A dataframe containing the file name and download URL.
    """

    # Initialize an empty dataframe to store the results
    df = pd.DataFrame()

    # Loop through the files_list and extract the file context for each result
    for i in range(len(files_list)):
        print("Extracting file context for result {} out of {}.".format(i+1, len(files_list)))
        
        # Extract the file context
        files = files_list[i]

        # Convert the files to a dataframe
        files_df = pd.DataFrame.from_dict(files)

        # Append the files_df to the df
        df = pd.concat([df, files_df], ignore_index=True)

    return df

# Function to download files that don't exist in the specified
# directory
def download_files(download_dir: str, 
                   df: pd.DataFrame) -> pd.DataFrame:
    """
    Download files that don't exist in the specified directory.

    Parameters:
    download_dir (str): The directory to download the files to.
    df (pd.DataFrame): The DataFrame containing the file information.

    Returns:
    pd.DataFrame: The updated DataFrame with the new filepaths.
    """
    # Constrain the dataframe to only the files which don't already exist on JASMIN
    files_df = df[df['file_exists'] == False]

    # Reset the index
    files_df.reset_index(drop=True, inplace=True)

    # Loop through the files_df and download the files
    for i in tqdm(range(len(files_df))):
        # Get the file_url
        file_url = files_df.loc[i, 'url']

        # Get the filename
        filename = files_df.loc[i, 'filename']

        # Split the filename and extract the variable name
        variable = filename.split('_')[0]

        # Split the filename to get the experiment name
        experiment = filename.split('_')[3]

        # Set up the model
        model = filename.split('_')[2]

        # Set up the download directory
        download_dir_loop = os.path.join(download_dir, experiment, variable, model)

        # If the download directory doesn't exist, make it
        if not os.path.exists(download_dir_loop):
            os.makedirs(download_dir_loop)

        # Set up the download path
        download_path = os.path.join(download_dir_loop, filename)

        # In the filepath column of the dataframe
        # replace the current file path with the download path
        files_df.loc[i, 'filepath'] = download_path

        # Set up the request
        r = requests.get(file_url, stream=True)

        # Set up the total size
        total_size = int(r.headers.get('content-length', 0))

        # Set up the block size
        block_size = 1024

        # Download the file
        with open(download_path, 'wb') as f:
            for data in tqdm(r.iter_content(block_size), 
                            total = total_size//block_size, 
                            unit = 'KiB', 
                            unit_scale = True):
                f.write(data)

            # If the total size is no 0
            if total_size != 0:
                print("File is not empty")
                print("Download complete - file saved to {}".format(download_path))

    return files_df