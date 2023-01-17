import pandas as pd
from sparsity_functions import SparsityFiller

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
    assert len(files) == 3, f"transform takes in 3 source files, not {len(files)} /n {files}"
    
    file_rolling_quarterly = [file for file in files if 'LFS_rolling_quarterly' in file][0]
    file_quarterly = [file for file in files if 'LFS_quarterly' in file][0]
    file_monthly = [file for file in files if 'LFS_monthly' in file][0]
    
    dataset_id = "labour-market-quarters-months"
    output_file = f"{location}v4-{dataset_id}.csv"
    
    dataset_id1 = "labour-market-rolling-quarters"
    output_file1 = f"{location}v4-{dataset_id1}.csv"
    
    dataset_id2 = "labour-market-monthly"
    output_file2 = f"{location}v4-{dataset_id2}.csv"
    
    dataset_id3 = "labour-market-quarterly"
    output_file3 = f"{location}v4-{dataset_id3}.csv"

    source1 = pd.read_csv(file_rolling_quarterly, dtype=str)
    
    df_list = []
    for col in source1.columns:
        if col in ('Unnamed: 0', 'Datetime', 'Rolling Quarter'):
            continue
        
        df_loop = pd.DataFrame()
        df_loop['v4_1'] = source1[col]
        df_loop['Data Marking'] = ''
        
        df_loop['mmm-mmm-yyyy'] = source1['Rolling Quarter'].apply(slugize)
        df_loop['Time'] = source1['Rolling Quarter']
        
        df_loop['uk-only'] = 'K02000001'
        df_loop['Geography'] = 'United Kingdom'
        
        df_loop['cdid'] = slugize(col)
        df_loop['CDID'] = col
        
        df_list.append(df_loop)
        
    df1 = pd.concat(df_list)
    df1 = df1.drop_duplicates()
    
    # data markings
    df1.loc[pd.isnull(df1['v4_1']), 'Data Marking'] = '.'
    
    df1.loc[df1['v4_1'] == '..', 'Data Marking'] = '..'
    df1.loc[df1['v4_1'] == '..', 'v4_1'] = ''
    
    df1.loc[df1['v4_1'] == '*', 'Data Marking'] = '*'
    df1.loc[df1['v4_1'] == '*', 'v4_1'] = ''
    
    df1.to_csv(output_file1, index=False)
    del source1
    
    source2 = pd.read_csv(file_monthly, dtype=str)
    
    df_list = []
    for col in source2.columns:
        if col in ('Unnamed: 0', 'Datetime', 'Year', 'Month'):
            continue
        
        df_loop = pd.DataFrame()
        df_loop['v4_1'] = source2[col]
        df_loop['Data Marking'] = ''
        
        df_loop['mmm-yy'] = source2['Month'] + '-' + source2['Year'].apply(lambda x: x[2:])
        df_loop['Time'] = df_loop['mmm-yy']
        
        df_loop['uk-only'] = 'K02000001'
        df_loop['Geography'] = 'United Kingdom'
        
        df_loop['cdid'] = slugize(col)
        df_loop['CDID'] = col
        
        df_list.append(df_loop)
        
    df2 = pd.concat(df_list)
    df2 = df2.drop_duplicates()
    
    # data markings
    df2.loc[pd.isnull(df2['v4_1']), 'Data Marking'] = '.'
    
    df2.to_csv(output_file2, index=False)
    del source2
    
    source3 = pd.read_csv(file_quarterly, dtype=str)
    
    df_list = []
    for col in source3.columns:
        if col in ('Unnamed: 0', 'Datetime', 'Year', 'Month', 'Quarter'):
            continue
        
        df_loop = pd.DataFrame()
        df_loop['v4_1'] = source3[col]
        df_loop['Data Marking'] = ''
        
        df_loop['mmm-mmm-yyyy'] = source3['Month'].apply(timeQuarterLabels) + ' ' + source3['Year']
        df_loop['Time'] = df_loop['mmm-mmm-yyyy']
        df_loop['mmm-mmm-yyyy'] = df_loop['Time'].apply(slugize)
        
        df_loop['uk-only'] = 'K02000001'
        df_loop['Geography'] = 'United Kingdom'
        
        df_loop['cdid'] = slugize(col)
        df_loop['CDID'] = col
        
        df_list.append(df_loop)
        
    df3 = pd.concat(df_list)
    df3 = df3.drop_duplicates()
    
    # data markings
    df3.loc[pd.isnull(df3['v4_1']), 'Data Marking'] = '.'
    
    df3.to_csv(output_file3, index=False)
    del source3
    
    # used for all file
    source2 = pd.read_csv(file_monthly, dtype=str)
    
    df_list = []
    for col in source2.columns:
        if col in ('Unnamed: 0', 'Datetime', 'Year', 'Month'):
            continue
        
        df_loop = pd.DataFrame()
        df_loop['v4_1'] = source2[col]
        df_loop['Data Marking'] = ''
        
        df_loop['mmm-yy'] = source2['Month'] + '-' + source2['Year']
        df_loop['Time'] = df_loop['mmm-yy']
        
        df_loop['uk-only'] = 'K02000001'
        df_loop['Geography'] = 'United Kingdom'
        
        df_loop['cdid'] = slugize(col)
        df_loop['CDID'] = col
        
        df_list.append(df_loop)
        
    df2 = pd.concat(df_list)
    df2 = df2.drop_duplicates()
    
    # data markings
    df2.loc[pd.isnull(df2['v4_1']), 'Data Marking'] = '.'

    del source2
    
    df1 = df1.rename(columns={'mmm-mmm-yyyy': 'quarters-months'})
    df2 = df2.rename(columns={'mmm-yy': 'quarters-months'})
    df2['quarters-months'] = df2['quarters-months'].apply(slugize)
    df3 = df3.rename(columns={'mmm-mmm-yyyy': 'quarters-months'})
    
    df = pd.concat([df1, df2, df3])
    df.loc[df['v4_1'] == '[x]', 'Data Marking'] = '[x]'
    df.loc[df['v4_1'] == '[x]', 'v4_1'] = ''

    df.to_csv(output_file, index=False)
    SparsityFiller(output_file, '.')
    
    # output used for upload
    output = {
            dataset_id: output_file,
            dataset_id1: output_file1,
            dataset_id2: output_file2,
            dataset_id3: output_file3,
            }
    
    return output

def slugize(value):
    return value.lower().replace(" ", "-")

def timeQuarterLabels(value):
    if value == 'Mar':
        return 'Jan-Mar'
    elif value == 'Jun':
        return 'Apr-Jun'
    elif value == 'Sep':
        return 'Jul-Sep'
    elif value == 'Dec':
        return 'Oct-Dec'
    else:
        raise Exception(f'Unknown quarter type - {value}')
