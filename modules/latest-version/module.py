import pandas as pd
import io, requests

def get_latest_version(dataset, edition):
    '''
    Pulls the latest v4 from CMD for a given dataset and edition
    '''
    editions_url = f"https://api.beta.ons.gov.uk/v1/datasets/{dataset}/editions/{edition}/versions"
    items = requests.get(f"{editions_url}?limit=1000").json()['items']
    
    # get latest version number
    latest_version_number = items[0]['version']
    assert latest_version_number == len(items), f"Get_Latest_Version for /{dataset}/editions/{edition} - number of versions ({len(items)}) does not match latest version number ({latest_version_number})"
    # get latest version URL
    url = f"{editions_url}/{str(latest_version_number)}"
    # get latest version data
    latest_version = requests.get(url).json()
    # decode data frame
    file_location = requests.get(latest_version['downloads']['csv']['href'])
    file_object = io.StringIO(file_location.content.decode('utf-8'))
    df = pd.read_csv(file_object, dtype=str)
    return df
