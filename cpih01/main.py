import pandas as pd
import requests, io

def transform(files):
    
    assert type(files) == list, f"transform takes in a list, not {type(files)}"
    assert len(files) == 1, f"transform only takes in 1 source file, not {len(files)}"
    file = files[0]
    output_file = "/tmp/v4-cpih01.csv"
    
    df = pd.read_csv(file, dtype=str)
    
    # check on source data to make sure obs are only to 1 dp
    df['v4_0'] = df['v4_0'].apply(Check_Source_Data)

    df['Time'] = df['time']

    df['uk-only'] = 'K02000001'
    df['Geography'] = 'United Kingdom'

    df = df.rename(
        columns = {
            'time':'mmm-yy',
            'Code':'cpih1dim1aggid',
            'Code desc':'Aggregate'
            }
    )

    df = df[[
        'v4_0', 'mmm-yy', 'Time', 'uk-only', 'Geography', 'cpih1dim1aggid', 'Aggregate'
        ]]

    """
    # Left out until lambda has public internet access
    previous_df = Get_Latest_Version('cpih01', 'time-series')
    df = pd.concat([df, previous_df])
    """
    
    df.to_csv(output_file, index=False)

    return output_file

def Check_Source_Data(value):
    # checks source data to make sure obs are only to 1 dp 
    number = str(value)
    if '.' not in number:
        return value
    
    decimal = number.split('.')[-1]
    number_of_dp = len(decimal)
    assert number_of_dp == 1, f"{value} is not to 1 decimal place"
    return value

def Get_Latest_Version(dataset, edition):
    '''
    Pulls the latest v4 from CMD for a given dataset and edition
    '''
    editions_url = 'https://api.beta.ons.gov.uk/v1/datasets/{}/editions/{}/versions'.format(dataset, edition)
    items = requests.get(editions_url + '?limit=1000').json()['items']

    # get latest version number
    latest_version_number = items[0]['version']
    assert latest_version_number == len(items), 'Get_Latest_Version for /{}/editions/{} - number of versions does not match latest version number'.format(dataset, edition)
    # get latest version URL
    url = editions_url + "/" + str(latest_version_number)
    # get latest version data
    latest_version = requests.get(url).json()
    # decode data frame
    file_location = requests.get(latest_version['downloads']['csv']['href'])
    file_object = io.StringIO(file_location.content.decode('utf-8'))
    df = pd.read_csv(file_object, dtype=str)
    return df

