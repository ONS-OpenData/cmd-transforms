from databaker.framework import *
import pandas as pd
from sparsity_functions import SparsityFiller
from latest_version import get_latest_version
import datetime

def transform(files, **kwargs):
    if 'location' in kwargs.keys():
        location = kwargs['location']
    else:
        location = ''
        
    assert type(files) == list, f"transform takes in a list, not {type(files)}"
    assert len(files) == 2, f"transform only takes in 2 source files, not {len(files)} /n {files}"
    
    # output used for upload
    output = {
            'weekly-deaths-health-board': '',
            'weekly-deaths-age-sex': '',
            'weekly-deaths-region': '',
            'weekly-deaths-local-authority': ''
            }
    
    # file for region and age&sex
    published_file = [file for file in files if 'published' in file.lower()][0]
    tabs = loadxlstabs(published_file)
    
    output_file = weekly_deaths_by_region(tabs)
    output['weekly-deaths-region'] = output_file
    
    
    
    
    
    return output



def weekly_deaths_by_region(source_tabs, **kwargs):
    if 'location' in kwargs.keys():
        location = kwargs['location']
        if location == '':
            pass
        elif not location.endswith('/'):
            location += '/'
    else:
        location = ''
        
    dataset_id = "weekly-deaths-region"
    output_file = f"{location}v4-weekly-deaths-region.csv"
    
    tabs = source_tabs
    tabs = [tab for tab in tabs if '12' in tab.name]
    
    conversionsegments = []
    for tab in tabs:
        if tab.name == '12a':
            # starting points for tables 12a1 & 12a2
            table_12a1 = tab.excel_ref('A').filter(contains_string('Table 12a1'))
            table_12a2 = tab.excel_ref('A').filter(contains_string('Table 12a2'))
            
            # 12a1 first
            geography_12a1 = table_12a1.shift(2, 1).expand(RIGHT).is_not_blank().is_not_whitespace()
            week_number_12a1 = table_12a1.shift(0, 2).expand(DOWN).is_not_blank().is_not_whitespace()
            week_number_12a1 -= table_12a2.expand(DOWN)
            date_12a1 = week_number_12a1.shift(1, 0)
            
            obs_12a1 = geography_12a1.waffle(week_number_12a1)
            
            dimensions_12a1 = [
                    HDim(date_12a1, TIME, DIRECTLY, LEFT),                    
                    HDimConst(GEOG, ''),  
                    HDim(geography_12a1, 'geography_labels', DIRECTLY, ABOVE),
                    HDim(week_number_12a1, 'week_number', DIRECTLY, LEFT),
                    HDimConst('death_type', 'Total registered deaths')
                    ]
            
            for cell in dimensions_12a1[0].hbagset:
                dimensions_12a1[0].AddCellValueOverride(cell, str(cell.value))
            
            conversionsegment_12a1 = ConversionSegment(tab, dimensions_12a1, obs_12a1).topandas()
            conversionsegments.append(conversionsegment_12a1)
            
            # 12a2
            geography_12a2 = table_12a2.shift(2, 1).expand(RIGHT).is_not_blank().is_not_whitespace()
            week_number_12a2 = table_12a2.shift(0, 2).expand(DOWN).is_not_blank().is_not_whitespace()
            date_12a2 = week_number_12a2.shift(1, 0)
            
            obs_12a2 = geography_12a2.waffle(week_number_12a2)
            
            dimensions_12a2 = [
                    HDim(date_12a2, TIME, DIRECTLY, LEFT),                     
                    HDimConst(GEOG, ''),  
                    HDim(geography_12a2, 'geography_labels', DIRECTLY, ABOVE),
                    HDim(week_number_12a2, 'week_number', DIRECTLY, LEFT),
                    HDimConst('death_type', 'Deaths involving COVID-19: registrations')
                    ]
            
            for cell in dimensions_12a2[0].hbagset:
                dimensions_12a2[0].AddCellValueOverride(cell, str(cell.value))
            
            conversionsegment_12a2 = ConversionSegment(tab, dimensions_12a2, obs_12a2).topandas()
            conversionsegments.append(conversionsegment_12a2)
            
        elif tab.name == '12b':
            # quick check on data
            assert tab.excel_ref('A6').value == 'Week number', f"data seems to have moved, A6 should be 'Week number', not '{tab.excel_ref('A5').value}'"
            
            geography = tab.excel_ref('C6').expand(RIGHT).is_not_blank().is_not_whitespace()
            week_number = tab.excel_ref('A7').expand(DOWN).is_not_blank().is_not_whitespace()
            date = week_number.shift(1, 0)
            
            obs = geography.waffle(week_number)
            
            dimensions = [
                    HDim(date, TIME, DIRECTLY, LEFT),                    
                    HDimConst(GEOG, ''),  
                    HDim(geography, 'geography_labels', DIRECTLY, ABOVE),
                    HDim(week_number, 'week_number', DIRECTLY, LEFT),
                    HDimConst('death_type', 'Deaths involving COVID-19: occurrences')
                    ]
            
            for cell in dimensions[0].hbagset:
                dimensions[0].AddCellValueOverride(cell, str(cell.value))
            
            conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
            conversionsegments.append(conversionsegment)
            
    df = pd.concat(conversionsegments)
    df1 = v4Writer('file-path', df, asFrame=True)
    
    ''' Post processing '''
    df['OBS'] = df['OBS'].apply(v4Integers)
    
    df['Time'] = df['TIME'].apply(YearExtractor)
    df['Time_codelist'] = df['Time']
    
    df['Geography_codelist'] = df['geography_labels'].apply(GeographyCodesFromLabels)
    df['Geography'] = df['geography_labels']
    
    df['week_number'] = df['week_number'].apply(lambda x: str(int(float(x))))
    df['week_number'] = 'Week ' + df['week_number']
    df['week_number_codelist'] = df['week_number'].apply(Slugize)
    
    df['death_type_codelist'] = df['death_type'].apply(Slugize)
    
    
    df = df.rename(columns={
            'OBS':'v4_0',
            'Time_codelist':'calendar-years',
            'Geography_codelist':'administrative-geography',
            'week_number_codelist':'week-number',
            'week_number':'Week',
            'death_type_codelist':'recorded-deaths',
            'death_type':'Deaths'
            }
    )
    
    df = df[[
            'v4_0', 'calendar-years', 'Time', 'administrative-geography', 'Geography',
            'week-number', 'Week', 'recorded-deaths', 'Deaths'
             ]]
    
    
    latest_df = get_latest_version(dataset_id, 'covid-19')
    
    # removed pre filled sparsity
    latest_df = latest_df[latest_df['Data Marking'] != 'x']
    latest_df = latest_df.rename(columns={'v4_1':'v4_0'}).drop(['Data Marking'], axis=1)
    latest_df = latest_df.reset_index(drop=True)
    
    # combine latest version with new version
    new_df = pd.concat([latest_df, df])
    #assert len(new_df) == len(new_df.drop_duplicates()), 'Weekly deaths by region has some duplicate data which it shouldnt'
    
    # removing duplicates
    # dataframe without obs to find any duplicates
    temp_df = new_df.drop(['v4_0'], axis=1).reset_index(drop=True)
    temp_df = temp_df.drop_duplicates()
    # index of rows to keep
    index_to_keep = temp_df.index
    new_df = new_df.iloc[index_to_keep]
    
    
    V4Checker(new_df, 'region')
    new_df.to_csv(output_file, index=False)
    SparsityFiller(output_file, 'x')
    
    return {dataset_id: output_file}