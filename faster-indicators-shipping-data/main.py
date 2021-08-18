
from databaker.framework import *
from databakerUtils.writers import v4Writer
import pandas as pd
import glob
from databakerUtils.sparsityFunctions import SparsityFiller
from databakerUtils.v4Functions import v4Integers
from datetime import datetime, timedelta


def transform(location):
    if location == '':
        pass
    elif not location.endswith('/'):
        location += '/'
    
    # input file is an xlsx
    files = glob.glob(location + '*.xlsx')
    # takes 1 input file
    assert len(files) == 1, f"transform only takes 1 input file, {len(files)} were found"
    file = files[0]
    
    # output file saved in same location as input files
    output_file = location + 'v4-shipping-data.csv'
    
    tabs = loadxlstabs(file)
    tabs = [tab for tab in tabs if 'weekly' in tab.name.lower()]

    data_marker_missing_weeks = '..'
    data_marker_missing_data = '..'
    
    '''DataBaking'''
    conversionsegments = []
    for tab in tabs:
        junk = tab.excel_ref('A').filter(contains_string('x:')).expand(RIGHT)

        week_number = tab.excel_ref('A7').expand(DOWN).is_not_blank().is_not_whitespace()
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
        
        # changing datetime format to str
        for cell in dimensions[1].hbagset:
            dimensions[1].AddCellValueOverride(cell, str(cell.value))

        conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
        conversionsegments.append(conversionsegment)

    data = pd.concat(conversionsegments).reset_index(drop=True)

    data['TIME'] = data['week_commencing'].apply(lambda x: str(x).split('-')[0])
    data['month'] = data['week_commencing'].apply(lambda x: str(x).split('-')[1])
    # if week 1 starts in december then the year will be incorrect -> add 1 to it
    data.loc[(data['week_number'] == '1.0') & (data['month'] == '12'), 'TIME'] = data['TIME'].apply(AddToYear) 
    # if week 52/53 starts in january then the year will be incorrect -> take away 1 from it
    data.loc[(data['week_number'] == '52.0') & (data['month'] == '01'), 'TIME'] = data['TIME'].apply(MinusAYear) 
    data.loc[(data['week_number'] == '53.0') & (data['month'] == '01'), 'TIME'] = data['TIME'].apply(MinusAYear) 
    data['week_number'] = data['week_number'].apply(WeekCorrector)

    data = data.drop(['month'], axis=1)
    data['TIMEUNIT'] = data['TIME']
    df = v4Writer('file-path', data, asFrame=True)

    '''Post Processing'''
    df['V4_1'] = df['V4_1'].apply(v4Integers)

    df['Geography'] = 'United Kingdom'

    df['week_number_codelist'] = 'week-' + df['week_number'].apply(lambda x: str(int(float(x))))
    df['week_number'] = df['week_number_codelist'].apply(WeekNumberLabels)
    df = df.drop(['week_commencing', 'week_commencing_codelist'], axis=1)

    df['port_codelist'] = df['port'].apply(Slugize)

    df['visit'] = df['visit'].apply(VisitType)
    df['visit_codelist'] = df['visit'].apply(Slugize)

    df = df.rename(columns={
            'Time_codelist':'calendar-years',
            'Time':'Time',
            'Geography_codelist':'uk-only',
            'Geography':'Geography',
            'week_number_codelist':'week-number',
            'week_number':'Week',
            'visit_codelist':'ship-and-visit-type',
            'visit':'ShipAndVisitType',
            'port_codelist':'shipping-port',
            'port':'Port'
            }
        )

    # filling in data marker where there is no data marker
    df.loc[(df['V4_1'] == '') & pd.isnull(df['Data Marking']), 'Data Marking'] = data_marker_missing_data

    df.to_csv(output_file, index=False)
    SparsityFiller(output_file, data_marker_missing_weeks)
    
    return output_file
        
def WeekNumberLabels(value):
    number = value.split('-')[-1]
    as_int = int(number)
    if as_int < 10:
        new_value = 'Week 0' + number
        return new_value
    else:
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

