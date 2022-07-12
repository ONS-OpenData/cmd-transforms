from databaker.framework import *
import pandas as pd
import datetime
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

    dataset_id = "uk-spending-on-cards"
    output_file = f"{location}v4-{dataset_id}.csv"

    wanted_tabs = 'Daily CHAPS indices'
    tabs = loadxlstabs(file, wanted_tabs)

    ''' DataBaking '''
    conversionsegments = []
    for tab in tabs:
        junk = tab.excel_ref('A').filter(contains_string('Note')).expand(RIGHT).expand(DOWN)
        
        category = tab.excel_ref('A').filter(contains_string('Category')).fill(DOWN).is_not_blank()
        category -= junk
        
        date_time = tab.excel_ref('A').filter(contains_string('Category')).fill(RIGHT).is_not_blank()
        
        obs = date_time.waffle(category)
        
        dimensions = [
                HDim(category, 'category', DIRECTLY, LEFT),
                HDim(date_time, 'datetime', DIRECTLY, ABOVE)
                ]
        
        for cell in dimensions[1].hbagset:
            dimensions[1].AddCellValueOverride(cell, str(cell.value))
        
        conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
        conversionsegments.append(conversionsegment)
        
    df = pd.concat(conversionsegments)

    ''' Post Processing '''
    df['calendar-years'] = df['datetime'].apply(datetime_to_years)
    df['Time'] = df['calendar-years']

    df['uk-only'] = 'K02000001'
    df['Geography'] = 'United Kingdom'

    df['dd-mm'] = df['datetime'].apply(datetime_to_dd_mm)
    df['DayMonth'] = df['dd-mm']

    df['spend-category'] = df['category'].apply(lambda x: x.replace(' ', '-').lower())

    df = df.rename(columns={
            'OBS':'v4_0',
            'category':'Category'
            }
        )

    df = df[['v4_0', 'calendar-years', 'Time', 'uk-only', 'Geography',
            'dd-mm', 'DayMonth', 'spend-category', 'Category']]

    df.to_csv(output_file, index=False)
    SparsityFiller(output_file, '.')

    return {dataset_id: output_file}

def datetime_to_years(value):
    # pulls the year from a datetime
    as_datetime = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S') 
    year = datetime.datetime.strftime(as_datetime, '%Y')
    return year

def datetime_to_dd_mm(value):
    # pulls the date and month from datetime into dd-mm format
    as_datetime = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S') 
    daymonth = datetime.datetime.strftime(as_datetime, '%d-%m')
    return daymonth

