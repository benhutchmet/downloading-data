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
def query_models_esgf(experiment_id: str, variable_id: str, table_id: str, activity_id: str, connection: SearchConnection, latest: bool = True, project: str = 'CMIP6') -> set:
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

    # Loop over the results to extract the file context
    for i in range(total_results):
        try:
            # Extract the file context
            hit = results[i].file_context().search()

            files = map(lambda f: {'filename': f.filename, 'url': f.download_url}, hit)

            files_list.extend(files)

            # Log the progress
            print(f"Processed {i+1} out of {total_results} results.")           
        except:
            print(f"Error: {results[i]}")
            continue
    
    return files_list

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
        Directory to check for the file.
        
    Returns
    -------
    df : pd.DataFrame
        Dataframe containing the file name and download URL.
    """

    # Create a new column in the dataframe to store whether the file exists
    df['file_exists'] = False

    # Create a new column in the dataframe to store the file path
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

        # Form the pattern
        pattern = os.path.join(directory, model_group, model_name, experiment_name,
                                ensemble_member_name, time_window, variable_name,
                                grid_name, "files", "d*", filename)

        # Get a list of all paths that match the pattern
        filepaths = glob.glob(pattern)

        # If filepath is greater than 0, the file exists
        if len(filepaths) > 0:
            print("File exists for " + filename)
            df.loc[i, 'file_exists'] = True
            df.loc[i, 'filepath'] = filepaths[0]
        elif len(filepaths) == 0:
            print("File does not exist for " + filename)
            df.loc[i, 'file_exists'] = False
        elif len(filepaths) > 1:
            print("More than one file found for " + filename)
            AssertionError("More than one file found for " + filename)
        else:
            print("Something went wrong with " + filename)
            AssertionError("Something went wrong with " + filename)

    return df



# # # Constrain the source_id_set to the first 1 model
# # source_id_set = list(source_id_set)[0:1]

# # # Print the set
# # print(source_id_set)

# # Initialize an empty dictionary to store the results
# max_results = {'source_id': None, 'data_node': None, 'num_results': 0}

# # Create a list for the max_results dictionaries
# max_results_list = []

# # Set up the max results per source dictionary
# max_results_per_source = {}

# # Loop through the source_id_set and query which nodes have data for each model
# for source_id in source_id_set:
#     print("trying to find valid nodes for model: {}".format(source_id))
#     # Set the source_id constraint
#     params['source_id'] = source_id
#     print(params)
#     # Query the database
#     model_query = conn.new_context(**params)
#     # Get the results
#     model_results = model_query.search()
#     # Print the number of results
#     print(len(model_results))

#     # if the len of the model results is not 0
#     if len(model_results) != 0:
#         # Print the first result
#         print(model_results[0].json['id'])

#     # Identify the unique nodes (data_node) which have data for the model
#     data_node_set = set(result.json['data_node'] for result in model_results)

#     # Print the set
#     print(data_node_set)

#     # Loop through the data_node_set and query how many files are available for each 
#     # node
#     for data_node in data_node_set:
#         print("trying to find valid files for node: {}".format(data_node))
        
#         # Set up the params for the query
#         params_node = params.copy()
        
#         # Set the data_node constraint
#         params_node['data_node'] = data_node
#         # Query the database
#         node_query = conn.new_context(**params_node)
#         # Get the results
#         node_results = node_query.search()
#         # Print the number of results
#         print(len(node_results))

#         # If this source_id is not in max_results_per_source or this data_node has more results, update the dictionary
#         if source_id not in max_results_per_source or len(node_results) > max_results_per_source[source_id]:
#             max_results = {'source_id': source_id, 'data_node': data_node, 'num_results': len(node_results)}
#             max_results_per_source[source_id] = len(node_results)

#             # Append the max_results dictionary to the list
#             max_results_list.append(max_results)
#         else:
#             print("this data_node has less results than the previous one")
#             continue

# # Print the dictionary
# print(max_results_list)

# # Clean the max_results_list to remove duplicate source_id entries
# # Keep the entry with the highest number of results (num_results)
# # Initialize an empty list to store the unique source_id entries
# unique_source_id_list = []

# # Loop through the max_results_list and append the unique source_id entries
# for result in max_results_list:
#     if result['source_id'] not in unique_source_id_list:
#         unique_source_id_list.append(result['source_id'])

# # Print the list
# print(unique_source_id_list)

# # Initialize an empty list to store the unique max_results_list entries
# unique_max_results_list = []

# # Loop through the unique_source_id_list and only
# # Append the max_results_list entries which match the source_id and have the highest num_results
# for source_id in unique_source_id_list:
#     print("source_id: {}".format(source_id))
#     # Initialize an empty list to store the num_results
#     num_results_list = []
#     # Loop through the max_results_list and append the num_results to the list
#     for result in max_results_list:
#         if result['source_id'] == source_id:
#             num_results_list.append(result['num_results'])
#     # Get the max num_results
#     max_num_results = max(num_results_list)
#     # Loop through the max_results_list and append the entries which match the source_id and max_num_results
#     for result in max_results_list:
#         if result['source_id'] == source_id and result['num_results'] == max_num_results:
#             unique_max_results_list.append(result)



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