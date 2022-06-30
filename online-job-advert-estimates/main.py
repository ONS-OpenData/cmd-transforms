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
    
    for tab in tabs:
        # row number of start point to skip rows for read_excel
        #start_point = tab.excel_ref('A').filter(contains_string('Date'))
        start_point = tab.excel_ref('B').filter(contains_string('Date'))
        start_point_number = start_point.y
        number_of_indicators = len(start_point.fill(DOWN).is_not_blank().is_not_whitespace())
        
        # pretty hacky..
        # if notes at bottom of spreadsheet then rows will be removed
        end_of_wanted_data = tab.excel_ref('B').filter(contains_string('Imputed'))
        end_of_wanted_data = end_of_wanted_data.y
        
        start_of_unwanted_data = tab.excel_ref('B').filter(contains_string('Note'))
        start_of_unwanted_data = start_of_unwanted_data.y
        
        if start_of_unwanted_data > end_of_wanted_data:
            # find number of rows that are not needed
            lines_to_ignore = tab.excel_ref('B').filter(contains_string('Note'))
            lines_to_ignore = len(lines_to_ignore.expand(DOWN).is_not_blank().is_not_whitespace())
            # lines to ignore plus the number of spaces between end of data and start of notes
            lines_to_ignore += start_of_unwanted_data - end_of_wanted_data - 1
            # number of indicators needs modifying
            number_of_indicators -= lines_to_ignore - 1
            
        else:
            lines_to_ignore = 0
        
    source = pd.read_excel(file, sheet_name=wanted_sheet, skiprows=start_point_number, 
                           skipfooter=lines_to_ignore, dtype=str)
    
    source = source.drop(['Unnamed: 0'], axis=1)
    
    # check to make sure data starts at 07/02/18
    if source.columns[1] != datetime(2018, 2, 7, 0, 0):
        raise Exception(f'''
    First column of data starting at "{datetime.strftime(source.columns[1], '%d-%m-%Y')}" rather than "07/02/18"
    Week numbers will be out of sync
    ''')
    
    df_list = []
    week_number_start = 6 # data starts 07/02/18 -> equivalent to week 6
    for col in source.columns:
        if col == 'Date':
            continue
        
        df_loop = pd.DataFrame()
        df_loop['v4_1'] = source[col]
        df_loop['date'] = ConvertDateTime(col)
        df_loop['week-number'] = week_number_start
        df_loop['indicator'] = source['Date']
        df_loop['Data Marking'] = source[col].iloc[number_of_indicators-1]
        df_list.append(df_loop)
        
        week_number_start += 1
        
    df = pd.concat(df_list).reset_index(drop=True)
    
    print(f"List of imputed values are {df['Data Marking'].unique()}")
    
    df['Data Marking'] = df['Data Marking'].apply(lambda x: x.replace(' only', ''))
    
    df.loc[df['indicator'] == df['Data Marking'], 'Data Marking'] = imputed_data_marker
    df['Data Marking'] = df['Data Marking'].apply(DataMarker)
    
    df = df[df['indicator'] != 'Imputed values']
    
    df['calendar-years'] = df['date'].apply(lambda x: x.split('-')[-1])
    df['time'] = df['calendar-years']
    
    df['uk-only'] = 'K02000001'
    df['geography'] = 'United Kingdom'
    
    df['adzuna-jobs-category'] = df['indicator'].apply(Slugize)
    
    # create new df for each year to correct week number
    df_list= []
    
    for year in df['time'].unique():
        
        df_loop = df[df['time'] == year].reset_index(drop=True)
        
        if year in ('2018', '2019'):
             df_loop['week-number'] = df_loop['week-number'].apply(WeekNumber)
        
        elif int(year)%4 == 0: # has an extra week
            
            df_loop['week-number'] = df_loop['week-number'].apply(WeekNumberLeapYear)
            
        else: # week numbers are now skewed
            df_loop['week-number'] = df_loop['week-number'].apply(lambda x: x-1)
            df_loop['week-number'] = df_loop['week-number'].apply(WeekNumber)
            
        df_loop['week'] = df_loop['week-number'].apply(WeekNumberLabel)
        
        df_list.append(df_loop)
    
    df = pd.concat(df_list)
        
    df = df.rename(columns={
            'indicator':'AdzunaJobsCategory',
            'time':'Time',
            'geography':'Geography',
            'week':'Week'
            }
        )
    
    df = df[[
            'v4_1', 'Data Marking', 'calendar-years', 'Time', 'uk-only', 'Geography',
            'adzuna-jobs-category', 'AdzunaJobsCategory', 'week-number', 'Week'
            ]]
    
    df.to_csv(output_file, index=False)
    SparsityFiller(output_file, data_marker)
    return {dataset_id: output_file}

def ConvertDateTime(value):
    return datetime.strftime(value, '%d-%m-%Y')

def DataMarker(value):
    if value == 'All':
        return imputed_data_marker
    elif value == imputed_data_marker:
        return value
    else:
        return ''
    
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
    return 'week-' + str(number)

def WeekNumberLabel(value):
    number = int(value.split('-')[-1])
    return 'Week ' + str(number)
    
def OutputName(tab_name):
    # returns the correct output file name from the tab name
    if 'feb 2020' in tab_name.lower():
        return 'v4-job-advert-estimates-feb-2020-index-by-category.csv'
    elif '2019 index' in tab_name.lower():
        return 'v4-job-advert-estimates-2019-index-by-category.csv'
    else:
        raise Exception('{} is not the correct tab of data'.format(tab_name))
        
