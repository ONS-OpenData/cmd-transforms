from databaker.framework import *
import pandas as pd
from datetime import datetime
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
    assert len(files) == 1, f"transform only takes in 1 source file, not {len(files)} /n {files}"
    file = files[0]
    
    dataset_id = "online-job-advert-estimates"
    output_file = f"{location}v4-{dataset_id}.csv"

    wanted_sheet = 'Adverts by category Feb 2020' # will change with each edition
    
    data_marker = '..' # used for future weeks
    global imputed_data_marker
    imputed_data_marker = 'x' # used for imputed values
    
    tabs = loadxlstabs(file, [wanted_sheet])
    
    # get row where data starts
    start_point = tabs[0].excel_ref('A').filter(contains_string('Date'))
    start_point_number = start_point.y
        
    source = pd.read_excel(file, sheet_name=wanted_sheet, skiprows=start_point_number, dtype=str)
    del tabs
    
    # check to make sure data starts at 07/02/18
    if source['Date'].iloc[0] != '2018-02-07 00:00:00':
        raise Exception(f'''
    First row of data should be '2018-02-07 00:00:00' not {source['Date'].iloc[0]}
    Week numbers will be out of sync
    ''')
    
    df_list = []
    week_number_start = 6 # data starts 07/02/18 -> equivalent to week 6
    
    for row in source.index:
            
        df_loop = pd.DataFrame()
        df_loop['v4_1'] = source[source.columns[1:-1]].iloc[row]
        df_loop['Data Marking'] = ''
        
        df_loop['calendar-years'] = source['Date'].iloc[row][:4] # extracting year
        df_loop['Time'] = df_loop['calendar-years']
        
        df_loop['uk-only'] = 'K02000001'
        df_loop['Geography'] = 'United Kingdom'
        
        df_loop['week-number'] = week_number_start
        df_loop['Week'] = week_number_start
        
        df_loop['adzuna-jobs-category'] = df_loop.index
        df_loop['adzuna-jobs-category'] = df_loop['adzuna-jobs-category'].apply(Slugize)
        df_loop['AdzunaJobsCategory'] = df_loop.index
        
        df_loop['indicator'] = source['Notes'].iloc[row]
        df_loop['indicator'] = df_loop['indicator'].apply(GetImputedValues)
        
        df_list.append(df_loop)
            
        week_number_start += 1
        
    df = pd.concat(df_list).reset_index(drop=True)
    
    # Adding imputed data markings
    # abit hacky but works unless different type of imputed value appears
    assert df['indicator'].unique().size == 3, "a new type of imputed value needs to be wrangled.."
    df1 = df[df['indicator'] == 'All industries,Education']
    df = df[df['indicator'] != 'All industries,Education']
        
    df.loc[df['indicator'] == imputed_data_marker, 'Data Marking'] = imputed_data_marker
    
    df1.loc[df1['AdzunaJobsCategory'] == 'All industries', 'Data Marking'] = imputed_data_marker
    df1.loc[df1['AdzunaJobsCategory'] == 'Education', 'Data Marking'] = imputed_data_marker
    
    df = pd.concat([df, df1])
    df = df.drop(['indicator'], axis=1)
    
    # create new df for each year to correct week number
    df_list= []
    
    for year in df['Time'].unique():
        
        df_loop = df[df['Time'] == year].reset_index(drop=True)
        
        if year in ('2018', '2019'):
             df_loop['week-number'] = df_loop['week-number'].apply(WeekNumber)
        
        elif int(year)%4 == 0: # has an extra week
            
            df_loop['week-number'] = df_loop['week-number'].apply(WeekNumberLeapYear)
            
        else: # week numbers are now skewed
            df_loop['week-number'] = df_loop['week-number'].apply(lambda x: x-1)
            df_loop['week-number'] = df_loop['week-number'].apply(WeekNumber)
            
        df_loop['Week'] = df_loop['week-number'].apply(WeekNumberLabel)
        
        df_list.append(df_loop)
    
    df = pd.concat(df_list)    
    
    if '2025' in df['calendar-years'].unique():
        raise Exception("aborting, 2025 is in data, check if week numbers are skewed")
    
    df.to_csv(output_file, index=False)
    SparsityFiller(output_file, data_marker)
    
    return {dataset_id: output_file}


def GetImputedValues(value):
    if value == 'nan':
        return ''
    
    elif pd.isnull(value):
        return ''
    
    else:
        indicator_range = value.split(' ')[-1].strip(']')
        
        if ':' in indicator_range:
            start_value = indicator_range.split(':')[0]
            end_value = indicator_range.split(':')[-1]
            
            if start_value.startswith('B') and end_value.startswith('AE'):
                return imputed_data_marker
            
            else:
                raise TypeError("A new type of imputed data range that isn't accounted for")
        
        elif ',' in indicator_range:
            indicator_values = indicator_range.split(',')
            lookup = {'B': 'All industries', 'K': 'Education'} 
            new_indicator_values = []
            for item in indicator_values:
                new_indicator_values.append(lookup[item[0]])
            return ','.join(new_indicator_values)
        
        else:
            raise TypeError("A new format of imputed data marking in data not accounted for")
               
def Slugize(value):
    new_value = value.replace(' / ', '-').replace('&', 'and').replace(' ', '-').lower()
    return new_value

def WeekNumber(value):
    number = value % 52
    if number == 0:
        number = 52
    return 'week-' + str(number)

def WeekNumberLeapYear(value):
    '''same as above but for leap years'''
    number = (value+2) % 53
    if number == 0:
        number = 53
    return f'week-{str(number)}'

def WeekNumberLabel(value):
    number = int(value.split('-')[-1])
    return f'Week {str(number)}'
    