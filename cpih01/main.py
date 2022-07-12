import pandas as pd
from latest_version import get_latest_version

def transform(files, **kwargs):
    if 'location' in kwargs.keys():
        location = kwargs['location']
        if location == '':
            pass
        elif not location.endswith('/'):
            location += '/'
    else:
        location = '' 

    assert type(files) == list, f"transform takes in a list, not {type(files)}"
    assert len(files) == 1, f"transform only takes in 1 source file, not {len(files)} /n {files}"
    file = files[0]

    dataset_id = "cpih01"
    output_file = f"{location}v4-{dataset_id}.csv"
    
    df = pd.read_csv(file, dtype=str)
    
    # check on source data to make sure obs are only to 1 dp
    df['v4_0'] = df['v4_0'].apply(check_source_data)

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

    previous_df = Get_Latest_Version('cpih01', 'time-series')
    df = pd.concat([df, previous_df])
    df = df.drop_duplicates()
    
    df.to_csv(output_file, index=False)

    return {dataset_id: output_file}

def check_source_data(value):
    # checks source data to make sure obs are only to 1 dp 
    number = str(value)
    if '.' not in number:
        return value
    
    decimal = number.split('.')[-1]
    number_of_dp = len(decimal)
    assert number_of_dp == 1, f"{value} is not to 1 decimal place"
    return value

