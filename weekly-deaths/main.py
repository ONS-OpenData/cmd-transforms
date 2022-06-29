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
    published_file = [file for file in files if 'publication' in file.lower()][0]
    tabs = loadxlstabs(published_file)
    
    output_file = weekly_deaths_by_region(tabs, location=location)
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
    
    ''' Post processing '''
    df['OBS'] = df['OBS'].apply(V4Integers)
    
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
    latest_df = latest_df.rename(columns={'v4_1':'v4_0', 'V4_1':'v4_0'}).drop(['Data Marking'], axis=1)
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
    
    return output_file

















def V4Checker(v4, dataset):
    '''
    Checks the dimensions of the v4 to make sure no irregularities
    '''
    df = v4.copy()
    # obs and data marking column not needed
    df = df[[col for col in df.columns if '4' not in col and 'Data Marking' not in col]]
    
    if dataset == 'region':
        for col in df.columns:
            if col not in ('calendar-years', 'Time', 'administrative-geography', 'Geography',
                           'week-number', 'Week', 'recorded-deaths', 'Deaths'):
                raise Exception('V4Checker on {} data - "{}" is not a correct column'.format(dataset, col))
        
        # year check
        for code in df['Time'].unique():
            try: 
                int(code)
            except:
                raise Exception('V4Checker on {} data - "{}" is not a year'.format(dataset, code))
            
            if int(code) < 2020:
                raise Exception('V4Checker on {} data - "{}" is outside of year range, data started in 2020'.format(dataset, code))
        
        # geography check
        for code in df['administrative-geography'].unique():
            if code not in ('E12000004', 'E12000007', 'W92000004', 'E12000009', 'E12000003',
                            'E12000008', 'K04000001', 'E12000002', 'E12000005', 'E12000006',
                            'E12000001'):
                raise Exception('V4Checker on {} data - "{}" should not be in geography codes'.format(dataset, code))
                
        # week number check
        for code in df['week-number'].unique():
            week_number = int(code.split('-')[-1])
            if week_number > 53:
                raise Exception('V4Checker on {} data - week "{}" is out of range'.format(dataset, code))
                
        # recorded death check
        for code in df['recorded-deaths'].unique():
            if code not in ('deaths-involving-covid-19-registrations',
                            'deaths-involving-covid-19-occurrences', 
                            'total-registered-deaths'):
                raise Exception('V4Checker on {} data - "{}" should not be in recorded deaths'.format(dataset, code))
                
    
    elif dataset == 'age&sex':
        for col in df.columns:
            if col not in ('calendar-years', 'Time', 'administrative-geography', 'Geography', 
                           'week-number', 'Week', 'sex', 'Sex', 'age-groups', 'AgeGroups', 
                           'recorded-deaths', 'Deaths'):
                raise Exception('V4Checker on {} data - "{}" is not a correct column'.format(dataset, col))
        
        # year check
        for code in df['Time'].unique():
            try: 
                int(code)
            except:
                raise Exception('V4Checker on {} data - "{}" is not a year'.format(dataset, code))
            
            if int(code) < 2020:
                raise Exception('V4Checker on {} data - "{}" is outside of year range, data started in 2020'.format(dataset, code))
        
        # geography check
        for code in df['administrative-geography'].unique():
            if code not in ('K04000001'):
                raise Exception('V4Checker on {} data - "{}" should not be in geography codes'.format(dataset, code))
              
        # week number check
        for code in df['week-number'].unique():
            week_number = int(code.split('-')[-1])
            if week_number > 53:
                raise Exception('V4Checker on {} data - week "{}" is out of range'.format(dataset, code))
                
        # recorded death check
        for code in df['recorded-deaths'].unique():
            if code not in ('deaths-involving-covid-19-registrations',
                            'deaths-involving-covid-19-occurrences', 
                            'total-registered-deaths'):
                raise Exception('V4Checker on {} data - "{}" should not be in recorded deaths'.format(dataset, code))
                
        # sex codes check
        for code in df['sex'].unique():
            if code not in ('male', 'female', 'all'):
                raise Exception('V4Checker on {} data - "{}" should not be in sex codes'.format(dataset, code))
                
        # age groups check
        for code in df['age-groups'].unique():
            if code not in ('90+', '20-24', '15-19', '85-89', '1-4', '0-1', 'all-ages',
                           '45-49', '40-44', '30-34', '65-69', '70-74', '75-79', '5-9',
                           '25-29', '50-54', '55-59', '80-84', '60-64', '10-14', '35-39'):
                raise Exception('V4Checker on {} data - "{}" should not be in age groups'.format(dataset, code))
    
    
    elif dataset.lower().replace('-', ' ') in ('la', 'local authority'):
        for col in df.columns:
            if col not in ('calendar-years', 'Time', 'administrative-geography', 'Geography',
                           'week-number', 'Week', 'cause-of-death', 'CauseOfDeath',
                           'place-of-death', 'PlaceOfDeath', 'registration-or-occurrence',
                           'RegistrationOrOccurrence'):
                raise Exception('V4Checker on {} data - "{}" is not a correct column'.format(dataset, col))
        
        # year checker - only one year of data per edition
        assert df['Time'].unique().size == 1, 'V4Checker on {} data - should only have one option for time but has {}'.format(dataset, df['Time'].unique().size)
        
        # geography check - dont want to call api and too many codes to create a list
        # quick check by counting codes - a change would mean sparsity anyway
        #assert df['administrative-geography'].unique().size == 336, 'V4Checker on {} data - been a change to the number of geographies, should be 336 but there is {}'.format(dataset, df['administrative-geography'].unique().size)
        
        # week number check
        for code in df['week-number'].unique():
            week_number = int(code.split('-')[-1])
            if week_number > 53:
                raise Exception('V4Checker on {} data - week "{}" is out of range'.format(dataset, code))
         
        # cause of death check
        for code in df['cause-of-death'].unique():
            if code not in ('all-causes', 'covid-19'):
                raise Exception('V4Checker on {} data - "{}" should not be in cause of death'.format(dataset, code))
                
        # place of death check
        for code in df['place-of-death'].unique():
            if code not in ('care-home', 'elsewhere', 'home', 'hospice', 'hospital',
                            'other-communal-establishment'):
                raise Exception('V4Checker on {} data - "{}" should not be in place of death'.format(dataset, code))
                
        # registration or occurrence check - hard coded in transform
        # so just a quick check that they have the same number
        if len(df[df['registration-or-occurrence'] == 'registrations']) != len(df[df['registration-or-occurrence'] == 'occurrences']):
            raise Exception('V4Checker on {} data - there are a different number of registrations and occurences'.format(dataset))
        
        
    elif dataset.lower().replace('-', ' ') in ('hb', 'health board'):
        for col in df.columns:
            if col not in ('calendar-years', 'Time', 'local-health-board', 'Geography',
                           'week-number', 'Week', 'cause-of-death', 'CauseOfDeath',
                           'place-of-death', 'PlaceOfDeath', 'registration-or-occurrence',
                           'RegistrationOrOccurrence'):
                raise Exception('V4Checker on {} data - "{}" is not a correct column'.format(dataset, col))
        
        # year checker - only one year of data per edition
        assert df['Time'].unique().size == 1, 'V4Checker on {} data - should only have one option for time but has {}'.format(dataset, df['Time'].unique().size)
        
        # geography check 
        for code in df['local-health-board'].unique():
            if code not in ('W11000023', 'W11000024', 'W11000025', 'W11000028', 'W11000029',
                            'W11000030', 'W11000031'):
                raise Exception('V4Checker on {} data - "{}" should not be in local health board'.format(dataset, code))
        
        # week number check
        for code in df['week-number'].unique():
            week_number = int(code.split('-')[-1])
            if week_number > 53:
                raise Exception('V4Checker on {} data - week "{}" is out of range'.format(dataset, code))
         
        # cause of death check
        for code in df['cause-of-death'].unique():
            if code not in ('all-causes', 'covid-19'):
                raise Exception('V4Checker on {} data - "{}" should not be in cause of death'.format(dataset, code))
                
        # place of death check
        for code in df['place-of-death'].unique():
            if code not in ('care-home', 'elsewhere', 'home', 'hospice', 'hospital',
                            'other-communal-establishment'):
                raise Exception('V4Checker on {} data - "{}" should not be in place of death'.format(dataset, code))
                
        # registration or occurrence check - hard coded in transform
        # so just a quick check that they have the same number
        if len(df[df['registration-or-occurrence'] == 'registrations']) != len(df[df['registration-or-occurrence'] == 'occurrences']):
            raise Exception('V4Checker on {} data - there are a different number of registrations and occurences'.format(dataset))
        
    print('{} is ok'.format(dataset))
    

def Slugize(value):
    new_value = value.replace(' ', '-').replace(':', '').lower()
    return new_value

def V4Integers(value):
    new_value = str(value)
    if new_value.endswith('.0'):
        new_value = new_value[:-2]
    return new_value

def YearExtractor(value):
    # extracts the year from datetime
    as_datetime = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    year = datetime.datetime.strftime(as_datetime, '%Y')
    return year

def MonthExtractor(value):
    # extracts the month from datetime
    as_datetime = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    month = datetime.datetime.strftime(as_datetime, '%b')
    return month

def SexLabels(value):
    if value == None:
        return 'All'
    elif 'Person' in value:
        return 'All'
    elif 'People' in value:
        return 'All'
    elif 'Female' in value:
        return 'Female'
    elif 'Male' in value:
        return 'Male'

def AgeLabels(value):
    if 'Under 1' in value or '<1' in value:
        return '0-1'
    #elif value == '1-4':
        #return '1-4'
    #elif value == '5-9':
        #return '5-9'
    elif 'all ages' in value.lower():
        return 'All ages'
    else:
        return value

def AgeCodes(value):
    lookup = {
            '00-01':'0-1',
            '01-04':'1-4',
            '05-09':'5-9',
            'All ages':'all-ages'
            }
    return lookup.get(value, value)

def AgeLabelsCorrector(value):
    # removes leading zeros
    if 'to' in value:
        first_number = str(int(value.split(' ')[0]))
        second_number = str(int(value.split(' ')[-1]))
        return f"{first_number}-{second_number}"
    else:
        return value
        

def AgeCorrector(value):
    # any old labels get fixed
    lookup = {
            '00-01':'0-1',
            '01-04':'1-4',
            '05-09':'5-9'
            }
    return lookup.get(value, value)

def WeekNumberLabels(value):
    value = str(value)
    as_int = int(value)
    if as_int < 10:
        new_value = str(as_int)
        return new_value
    else:
        return value

def DeathType(value):
    if 'registrations' in value.lower():
        return 'Deaths involving COVID-19: registrations'
    elif 'occurrences' in value.lower():
        return 'Deaths involving COVID-19: occurrences'
    else:
        return 'Total registered deaths'

def TotalGeog(value):
    # Returns england & wales code for total
    if value.startswith('E'):
        return False
    elif value.startswith('W'):
        return False
    else:
        return True
    
def GeogLabelsCorrector(value):
    if value == 'East':
        return 'East of England'
    else:
        return value
    
def GeographyCodesFromLabels(value):
    lookup = {
            'North East':'E12000001',
            'North West':'E12000002',
            'Yorkshire and The Humber':'E12000003',
        	   'East Midlands':'E12000004',
        	   'West Midlands':'E12000005',
        	   'East of England':'E12000006',
        	   'London':'E12000007',
        	   'South East':'E12000008',
        	   'South West':'E12000009',
        	   'Wales':'W92000004'
            }
    return lookup[value]

