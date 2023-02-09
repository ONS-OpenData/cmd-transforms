import pandas as pd
from sparsity_functions import SparsityFiller
from code_list import get_codes_from_codelist

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
    assert len(files) == 1, f"transform takes in 1 source file1, not {len(files)} /n {files}"
    file = files[0]
    
    dataset_id_quarterly = "regional-gdp-by-quarter"
    dataset_id_yearly = "regional-gdp-by-year"

    output_file_quarterly = f"{location}v4-{dataset_id_quarterly}.csv"
    output_file_yearly = f"{location}v4-{dataset_id_yearly}.csv"
    
    output = {
        dataset_id_quarterly: output_file_quarterly,
        dataset_id_yearly: output_file_yearly
    }

    # getting labels from API
    global nuts_dict, sic_dict, growth_rate_dict
    nuts_dict = get_codes_from_codelist("nuts") 
    sic_dict = get_codes_from_codelist("sic-unofficial")  
    growth_rate_dict = get_codes_from_codelist("quarterly-index-and-growth-rate") 
    
    df = pd.read_csv(file, dtype=str)

    df['Region'] = df['Region'].apply(lambda x: x.replace('EN', 'UK0'))
    df['geography'] = df['Region'].apply(geog_labels)

    df['Industry'] = df['Industry'].apply(lambda x: x.replace('_', '--'))
    df['Industry desc'] = df['Industry'].apply(sic_labels)

    df['Price'] = df['Price'].apply(lambda x: x.lower())

    df['Measure'] = df['Measure'].apply(lambda x: x.lower())

    df = df.rename(columns={
            'time': 'Time',
            'Region': 'nuts',
            'geography': 'Geography',
            'Industry': 'sic-unofficial',
            'Industry desc': 'UnofficialStandardIndustrialClassification',
            'Price': 'type-of-prices',
            'Price desc': 'Prices',
            'Measure': 'quarterly-index-and-growth-rate',
            'Measure desc': 'GrowthRate'
            }
    )

    df = df[['v4_0', 'time type', 'Time', 'nuts', 'Geography', 
            'sic-unofficial', 'UnofficialStandardIndustrialClassification', 'type-of-prices', 'Prices',
            'quarterly-index-and-growth-rate', 'GrowthRate']]

    df_yearly = df[df['time type'] == 'Year'].reset_index(drop=True)
    df_yearly['time type'] = df_yearly['Time']
    df_yearly['quarterly-index-and-growth-rate'] = df_yearly['quarterly-index-and-growth-rate'].apply(lambda x: x.replace('ix', 'aix'))
    df_yearly['GrowthRate'] = df_yearly['quarterly-index-and-growth-rate'].apply(growth_rate_labels)

    df_yearly = df_yearly.rename(columns={
            'time type': 'calendar-years'
            }
    )

    df_quarterly = df[df['time type'] == 'Quarter'].reset_index(drop=True)
    df_quarterly['time type'] = df_quarterly['Time'].apply(lambda x: x.replace(' ', '-').lower())
    df_quarterly['quarterly-index-and-growth-rate'] = df_quarterly['quarterly-index-and-growth-rate'].apply(lambda x: x.replace('ix', 'qix'))
    df_quarterly['GrowthRate'] = df_quarterly['quarterly-index-and-growth-rate'].apply(growth_rate_labels)

    df_quarterly = df_quarterly.rename(columns={
            'time type': 'yyyy-qq'
            }
    )

    df_yearly.to_csv(output_file_yearly, index=False)
    SparsityFiller(output_file_yearly)

    df_quarterly.to_csv(output_file_quarterly, index=False)
    SparsityFiller(output_file_quarterly)

    return output

def geog_labels(value):
    return nuts_dict[value]

def sic_labels(value):
    return sic_dict[value]

def growth_rate_labels(value):
    return growth_rate_dict[value]

