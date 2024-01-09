import pandas as pd
import io, requests, sys

def get_latest_version(dataset, edition, **kwargs):
    '''
    Pulls the latest v4 from CMD for a given dataset and edition
    '''
    if sys.platform.lower().startswith('win'):
        verify = False
        requests.packages.urllib3.disable_warnings()
    else:
        verify = True
    
    editions_url = f"https://api.beta.ons.gov.uk/v1/datasets/{dataset}/editions/{edition}/versions"

    if 'version_number' in kwargs.keys():
        latest_version_number = kwargs['version_number']

    else:
        items = requests.get(f"{editions_url}?limit=1000", verify=verify).json()['items']
        # get latest version number
        latest_version_number = items[0]['version']
        assert latest_version_number == len(items), f"Get_Latest_Version for /{dataset}/editions/{edition} - number of versions ({len(items)}) does not match latest version number ({latest_version_number})"
    
    # get latest version URL
    url = f"{editions_url}/{str(latest_version_number)}"
    # get latest version data
    latest_version = requests.get(url, verify=verify).json()
    # check download option exists
    check_download_available(url)
    # decode data frame
    file_location = requests.get(latest_version['downloads']['csv']['href'], verify=verify)
    file_object = io.StringIO(file_location.content.decode('utf-8'))
    df = pd.read_csv(file_object, dtype=str)
    return df

def check_download_available(latest_version_url):
    # checks if the csv download is available for the latest version 
    # mainly used for trade
    page_dict = requests.get(latest_version_url, verify=verify).json()
    
    assert 'downloads' in page_dict.keys(), f"No download option available for {latest_version_url}"
    assert 'csv' in page_dict['downloads'].keys(), f"No csv download available for {latest_version_url}"
    print(f"Download options available for {latest_version_url}")
    return
