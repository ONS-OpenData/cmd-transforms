from databaker.framework import *
import pandas as pd
from sparsity_functions import SparsityFiller
from datetime import datetime, timedelta

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
    
    dataset_id = "faster-indicators-shipping-data"
    output_file = f"{location}v4-{dataset_id}.csv"
    
    tabs = loadxlstabs(file)
    tabs = [tab for tab in tabs if 'weekly' in tab.name.lower()]
    
    data_marker_missing_weeks = '..'
    data_marker_missing_data = '..'
    
    '''Data Baking'''
    conversionsegments = []
    for tab in tabs:
        junk = tab.excel_ref('A').filter(contains_string('x:')).expand(RIGHT)#.expand(DOWN)
        
        week_number = tab.excel_ref('A8').expand(DOWN).is_not_blank().is_not_whitespace()
        week_number -= junk
        
        week_commencing = week_number.shift(1, 0)
        
        port = tab.excel_ref('C3').expand(RIGHT).is_not_blank().is_not_whitespace()

        visit = tab.name
        
        geography = 'K02000001'
        
        obs = week_number.waffle(port)
        
        dimensions = [
                HDim(week_number, 'week_number', DIRECTLY, LEFT),
                HDim(week_commencing, 'week_commencing', DIRECTLY, LEFT),
                HDim(port, 'port', DIRECTLY, ABOVE),
                HDimConst(GEOG, geography),
                HDimConst('visit', visit)
                ]
        
        for cell in dimensions[1].hbagset:
            dimensions[1].AddCellValueOverride(cell, str(cell.value))
        
        conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
        conversionsegments.append(conversionsegment)
    
    df = pd.concat(conversionsegments).reset_index(drop=True)

    df['TIME'] = df['week_commencing'].apply(lambda x: str(x).split('-')[0])
    df['month'] = df['week_commencing'].apply(lambda x: str(x).split('-')[1])
    # if week 1 starts in december then the year will be incorrect -> add 1 to it
    df.loc[(df['week_number'] == '1.0') & (df['month'] == '12'), 'TIME'] = df['TIME'].apply(AddToYear)
    # if week 52/53 starts in january then the year will be incorrect -> take away 1 from it
    df.loc[(df['week_number'] == '52.0') & (df['month'] == '01'), 'TIME'] = df['TIME'].apply(MinusAYear)
    df.loc[(df['week_number'] == '53.0') & (df['month'] == '01'), 'TIME'] = df['TIME'].apply(MinusAYear)
    df['week_number'] = df['week_number'].apply(WeekCorrector)
    df = df.drop(['month'], axis=1)
    df['TIMEUNIT'] = df['TIME']
    
    '''Post Processing'''
    df['OBS'] = df['OBS'].apply(V4Integers)
    
    df['Geography'] = 'United Kingdom'
    
    df['week_number_codelist'] = 'week-' + df['week_number'].apply(lambda x: str(int(float(x))))
    df['week_number'] = df['week_number_codelist'].apply(WeekNumberLabels)
    
    df['port_codelist'] = df['port'].apply(Slugize)
    
    df['visit'] = df['visit'].apply(VisitType)
    df['visit_codelist'] = df['visit'].apply(Slugize)
    
    df = df.rename(columns={
            'OBS':'v4_1',
            'DATAMARKER':'Data Marking',
            'TIMEUNIT':'calendar-years',
            'TIME':'Time',
            'GEOG':'uk-only',
            'week_number_codelist':'week-number',
            'week_number':'Week',
            'visit_codelist':'ship-and-visit-type',
            'visit':'ShipAndVisitType',
            'port_codelist':'shipping-port',
            'port':'Port'
            }
        )
    
    df = df[[
            'v4_1', 'Data Marking', 'calendar-years', 'Time', 'uk-only', 'Geography',
            'week-number', 'Week', 'shipping-port', 'Port', 'ship-and-visit-type', 'ShipAndVisitType'
            ]]
    
    # filling in data marker where there is no data marker
    df.loc[(df['v4_1'] == '') & pd.isnull(df['Data Marking']), 'Data Marking'] = data_marker_missing_data
    
    df.to_csv(output_file, index=False)
    SparsityFiller(output_file, data_marker_missing_weeks)
    
    return {dataset_id: output_file}



def WeekNumberLabels(value):
    number = value.split('-')[-1]
    return 'Week ' + number
    
def Slugize(value):
    new_value = value.replace(' & ', '-').replace('&', '').replace(' ', '-').lower()
    return new_value

def VisitType(value):
    lookup = {
            'Weekly all visits':'All visits', 
            'Weekly all unique ships':'All unique ship visits',
            'Weekly C&T visits':'Cargo and tanker visits', 
            'Weekly C&T unique ships':'Cargo and tanker unique ship visits',
            'Weekly Passenger visits':'Passenger ship visits'
            }
    return lookup[value]

def YearCalculator(value):
    '''
    year is pulled from week commencing
    so some week 1's will be from previous year
    function adds 6 days to week commencing to find year
    '''
    as_datetime = datetime.strptime(value, '%Y-%m-%d %H:%M:%S') # convert to datetime format
    new_time = as_datetime + timedelta(days=6)
    return datetime.strftime(new_time, '%Y-%m-%d %H:%M:%S')

def AddToYear(value):
    '''adds one to the year'''
    new_value = str(int(value) + 1)
    return new_value

def MinusAYear(value):
    '''takes away one from the year'''
    new_value = str(int(value) - 1)
    return new_value

def WeekCorrector(value):
    '''to correct week numbers > 53'''
    week_number = float(value)
    if week_number > 53:
        new_week_number = float(week_number % 53)
        return str(new_week_number)
    else:
        return value
    
def V4Integers(value):
    '''
    treats all values in v4 column as strings
    returns integers instead of floats for numbers ending in '.0'
    '''
    new_value = str(value)
    if new_value.endswith('.0'):
        new_value = new_value[:-2]
    return new_value



