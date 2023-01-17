
import pandas as pd

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
    
    dataset_id = "mid-year-pop-est"
    output_file = f"{location}v4-{dataset_id}.csv"

    source = pd.read_excel(file, sheet_name='2021_geog', dtype=str)
    
    df_list = []
    for col in source.columns:
        if 'pop' not in col:
            continue
        
        year = col.split('_')[-1]
        
        df_loop = pd.DataFrame()
        df_loop['v4_0'] = source[col]
        df_loop['calendar-years'] = year
        df_loop['Time'] = year
        df_loop['administrative-geography'] = source['area']
        df_loop['Geography'] = source['name']
        df_loop['Sex'] = source['sex'].apply(sexLabels)
        df_loop['sex'] = df_loop['Sex'].apply(lambda x: x.lower())
        df_loop['single-year-of-age'] = source['age'].apply(lambda x: x.strip().lower().replace('90', '90+'))
        df_loop['Age'] = df_loop['single-year-of-age'].apply(ageLabels)
        
        df_list.append(df_loop)
        
    df = pd.concat(df_list)
    df = df[['v4_0', 'calendar-years', 'Time', 'administrative-geography',
           'Geography', 'sex', 'Sex', 'single-year-of-age', 'Age']]
    
    df.to_csv(output_file, index=False)
    
    return {dataset_id: output_file}

def sexLabels(value):
    lookup = {'0':'All', '1':'Male', '2':'Female'}
    return lookup[value]

def ageLabels(value):
    if value == 'total':
        return 'Total'
    else:
        return value