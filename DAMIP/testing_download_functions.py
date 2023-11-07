# Import the relevant libraries
from pyesgf.search import SearchConnection
import os
import pandas as pd
import requests
from tqdm import tqdm
from typing import List, Dict
os.environ["ESGF_PYCLIENT_NO_FACETS_STAR_WARNING"] = "on"


def query_data_esgf(connection: SearchConnection,
                    source_id: str,
                    experiment_id: str,
                    variable_id: str,
                    table_id: str = 'Amon',
                    project: str = 'CMIP6',
                    member_id: str = None,
                    latest: bool = True)-> List[Dict]:
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
    project : str, optiSonal
        Project name. E.g. 'CMIP6'. The default is 'CMIP6'.
    member_id : str, optional
        Member name. E.g. 'r1i1p1f1'. The default is None.
    latest : bool, optional
        Whether to download the latest version of the dataset. The default is True.
        
    Returns
    -------
    results : list
        List of dictionaries containing the dataset information.
    """

    if member_id is None:
        # Query the database
        query = connection.new_context(
            latest=latest,
            project=project,
            source_id=source_id,
            experiment_id=experiment_id,
            variable_id=variable_id,
            table_id=table_id
        )
    else:
        # Query the database
        query = connection.new_context(
            latest=latest,
            project=project,
            source_id=source_id,
            experiment_id=experiment_id,
            variable_id=variable_id,
            table_id=table_id,
            member_id=member_id
        )

    # Get the results
    results = query.search()

    return results