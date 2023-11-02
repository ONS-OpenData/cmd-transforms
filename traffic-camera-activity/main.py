from databaker.framework import *
import pandas as pd
from sparsity_functions import SparsityFiller
import datetime


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

    dataset_id = "traffic-camera-activity"
    output_file = f"{location}v4-{dataset_id}.csv"

    tabs = loadxlstabs(file)
    tabs = [tab for tab in tabs if 'note' not in tab.name.lower()] # remove note tab
    tabs = [tab for tab in tabs if 'content' not in tab.name.lower()]

    '''DataBaking'''
    conversionsegments = []
    for tab in tabs:
        junk = tab.filter(contains_string('Note:')).expand(DOWN).expand(RIGHT) # unwated notes

        date = tab.excel_ref('A').expand(DOWN).is_not_blank().is_not_whitespace()
        date -= junk
        
        area = tab.excel_ref('B1').expand(RIGHT).is_not_blank().is_not_whitespace()
        
        vehicle = tab.excel_ref('B2').expand(RIGHT).is_not_blank().is_not_whitespace()
        
        adjustment = tab.name
        
        obs = area.waffle(date)
        
        dimensions = [
                HDimConst(TIME, 'year'), # year to be filled in later
                HDimConst(GEOG, 'K02000001'),
                HDim(date, 'date', DIRECTLY, LEFT),
                HDim(area, 'area', DIRECTLY, ABOVE),
                HDim(vehicle, 'vehicle', DIRECTLY, ABOVE),
                HDimConst('adjustment', adjustment)
                ]
        
        for cell in dimensions[2].hbagset:
            dimensions[2].AddCellValueOverride(cell, str(cell.value))
        
        conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
        conversionsegments.append(conversionsegment)
        
    df = pd.concat(conversionsegments)

    '''Post Processing'''
    df['OBS'] = df['OBS'].apply(v4Integers)

    df['calendar-years'] = df['date'].apply(DateToYear)
    df['Time'] = df['calendar-years']

    df['Geography'] = 'United Kingdom'

    df['dd-mm'] = df['date'].apply(DateToDDMMM)
    df['date'] = df['dd-mm']
    df['dd-mm'] = df['dd-mm'].apply(lambda x: x.lower())

    df['traffic-camera-area'] = df['area'].apply(Slugize)

    df['vehicle'] = df['vehicle'].apply(lambda x: x.capitalize())
    df['pedestrians-and-vehicles'] = df['vehicle'].apply(Slugize)

    df['adjustment'] = df['adjustment'].apply(SeasonalAdjustment)
    df['seasonal-adjustment'] = df['adjustment'].apply(Slugize)

    df = df.rename(columns={
        'OBS':'v4_1',
        'DATAMARKER':'Data Marking',
        'GEOG':'uk-only',
        'date':'DayMonth',
        'area':'TrafficCameraArea',
        'vehicle':'PedestriansAndVehicles',
        'adjustment':'SeasonalAdjustment'
        }
    )

    df = df[['v4_1', 'Data Marking', 'calendar-years', 'Time', 'uk-only', 'Geography',
                'dd-mm', 'DayMonth', 'traffic-camera-area', 'TrafficCameraArea', 
                'pedestrians-and-vehicles', 'PedestriansAndVehicles', 'seasonal-adjustment', 'SeasonalAdjustment']]

    df.to_csv(output_file, index=False)
    SparsityFiller(output_file, '*')

    return {dataset_id: output_file}


def DateToYear(value):
    # pulls the year from the date -> date should have form 'yyyy-mm-dd 00:00:00'
    as_datetime = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    new_value = datetime.datetime.strftime(as_datetime, '%Y')
    return new_value

def DateToDDMMM(value):
    # converts date to just day and month -> dd-mmm
    as_datetime = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    new_value = datetime.datetime.strftime(as_datetime, '%d-%m')
    return new_value

def Slugize(value):
    new_value = value.replace('&', 'and').replace(' ', '-').lower()
    return new_value

def SeasonalAdjustment(value):
    if value == 'Trend':
        return value
    lookup = {
            'Non seasonally adjusted':'Non Seasonal Adjustment',
            'Seasonally adjusted':'Seasonal Adjustment',
            'Non-adjusted':'Non Seasonal Adjustment',
            'Weekday adjusted':'Seasonal Adjustment'
            }
    return lookup[value]

def v4Integers(value):
    # returns integers instead of floats for numbers ending in '.0'
    newValue = str(value)
    if newValue.endswith('.0'):
        newValue = newValue[:-2]
    return newValue

