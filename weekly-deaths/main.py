from databaker.framework import *
import pandas as pd
import math
from sparsity_functions import SparsityFiller

def transform(files, **kwargs):        
    assert type(files) == list, f"transform takes in a list, not {type(files)}"
    assert len(files) == 1, f"transform only takes in 1 source file, not {len(files)} /n {files}"
    file = files[0]
    
    # output used for upload
    output = {
            'weekly-deaths-age-sex': '',
            'weekly-deaths-region': ''
            }
    
    source_tabs = loadxlstabs(file)
    
    # weekly deaths region
    output_file = transform_weekly_deaths_by_region(source_tabs)
    output['weekly-deaths-region'] = output_file

    # weekly deaths age sex
    output_file = transform_weekly_deaths_by_age_and_sex(source_tabs)
    output['weekly-deaths-age-sex'] = output_file
    
    return output

def transform_weekly_deaths_by_region(source_tabs, **kwargs):        
    dataset_id = "weekly-deaths-region"
    output_file = f"v4-{dataset_id}.csv"

    tabs = source_tabs
    tabs = [tab for tab in tabs if '3' in tab.name]
    assert len(tabs) == 1, f"{len(tabs)} tabs found but should have only found 1"

    # iterating the databaking process
    max_length = []
    for tab in tabs:
        tab_max = len(tab.excel_ref('A'))
        max_length.append(tab_max)
    max_length = max(max_length)
    batch_number = 84    # iterates over this many rows at a time
    number_of_iterations = math.ceil(max_length/batch_number)    # will iterate this many times
       

    css = []
    for tab in tabs:
        # check to make sure data has not moved
        start_point = tab.excel_ref('A6')
        assert start_point.value.startswith('Week'), f"Data appears to have moved, cell A6 (Table_3) should be 'Week number' not {start_point.value}"
        
        for i in range(0, number_of_iterations):
            
            Min = start_point.y + 2 + batch_number * i  # data starts on below start_point
            Max = Min + batch_number - 1
        
            week_number = tab.excel_ref(f"A{Min}:A{Max}").is_not_blank().is_not_whitespace()
            date = week_number.shift(1, 0)
            area = week_number.shift(2, 0)
            cause_of_death = week_number.shift(3, 0)
            
            obs = week_number.shift(4, 0)
    
            dimensions = [
                HDim(week_number, 'week_number', DIRECTLY, LEFT),
                HDim(date, TIME, DIRECTLY, LEFT),
                HDim(area, GEOG, DIRECTLY, LEFT),
                HDim(cause_of_death, 'cause_of_death', DIRECTLY, LEFT),
            ]
            
            if len(obs) != 0:
                cs = ConversionSegment(tab, dimensions, obs).topandas()
                css.append(cs)

    df = pd.concat(css)

    df['OBS'] = df['OBS'].apply(lambda x: str(int(x)))

    df['TIME'] = df['TIME'].apply(lambda x: x[-4:])
    df['calendar-years'] = df['TIME']

    # remove England, Wales and non-residents
    df = df[df['GEOG'] != "England, Wales and non-residents"]
    df['administrative-geography'] = df['GEOG'].apply(geography_codes_from_labels)

    df['Week'] = df['week_number'].apply(week_number_labels)
    df['week-number'] = df['Week'].apply(slugize)

    df['cause-of-death'] = df['cause_of_death'].apply(cause_of_death_codes)
    df['cause-of-death'] = df['cause-of-death'].apply(slugize)


    df = df.rename(columns={
                'OBS': 'v4_0',
                'TIME': 'Time',
                'GEOG': 'Geography',
                'cause_of_death': 'CauseOfDeath',
            }
        )

    df = df[[
            'v4_0', 'calendar-years', 'Time', 'administrative-geography', 'Geography',
            'week-number', 'Week', 'cause-of-death', 'CauseOfDeath'
        ]]
    
    assert len(df["Time"].unique()) == 1, "found more than one calendar year in weekly deaths by region"
    
    df.to_csv(output_file, index=False)
    SparsityFiller(output_file, 'x')

    return output_file

def transform_weekly_deaths_by_age_and_sex(source_tabs, **kwargs):
    dataset_id = "weekly-deaths-age-sex"
    output_file = f"v4-{dataset_id}.csv"

    tabs = source_tabs
    tabs = [tab for tab in tabs if tab.name.lower() in ('table_1', 'table_2')]
    assert len(tabs) == 2, f"{len(tabs)} tabs found but should have only found 2"
    
    # iterating the databaking process - file length is going to get long
    max_length = []
    for tab in tabs:
        tab_max = len(tab.excel_ref('A'))
        max_length.append(tab_max)
    max_length = max(max_length)
    batch_number = 20    # iterates over this many rows at a time
    number_of_iterations = math.ceil(max_length/batch_number)    # will iterate this many times
    
    css = []
    for tab in tabs:
        
        # get start point
        if tab.name.lower() == 'table_1':
            start_column = 'A'
            start_point = tab.excel_ref(start_column).filter(contains_string('Week number'))
        
        elif tab.name.lower() == 'table_2':
            start_column = 'B'
            start_point = tab.excel_ref(start_column).filter(contains_string('Week number'))

        assert len(start_point) != 0, f"Data appears to have moved, could not find cell containing 'Week number' in column '{start_column}' in tab '{tab.name}'"
        assert start_point.value.startswith('Week'), f"Data appears to have moved, cell {start_point} in tab {tab.name} should be 'Week number' not {start_point.value}"
        
        for i in range(0, number_of_iterations):
            
            Min = start_point.y + 2 + batch_number * i  # data starts below start_point
            Max = Min + batch_number - 1
            
            week_number = tab.excel_ref(f"{start_column}{Min}:{start_column}{Max}").is_not_blank().is_not_whitespace()
            date = week_number.shift(1, 0)
            area = week_number.shift(2, 0)
            sex = week_number.shift(3, 0)
            age = week_number.shift(4, 0)
            place = week_number.shift(5, 0)
            
            obs = week_number.shift(6, 0)
            
            dimensions = [
                    HDim(week_number, 'week_number', DIRECTLY, LEFT),
                    HDim(date, TIME, DIRECTLY, LEFT),
                    HDim(area, GEOG, DIRECTLY, LEFT),
                    HDim(sex, 'sex', DIRECTLY, LEFT),
                    HDim(age, 'age', DIRECTLY, LEFT),
                    HDimConst('tab_name', tab.name),
                    HDim(place, 'place', DIRECTLY, LEFT),
                ]
            
            if len(obs) != 0:
                cs = ConversionSegment(tab, dimensions, obs).topandas()
                css.append(cs)

    df = pd.concat(css)
    df = df.reset_index(drop=True)

    df['OBS'] = df['OBS'].apply(lambda x: str(int(x)))
    
    df['TIME'] = df['TIME'].apply(lambda x: x[-4:])
    df['calendar-years'] = df['TIME']

    # remove England, Wales and non-residents
    df = df[df['GEOG'] != "England, Wales and non-residents"]
    df['administrative-geography'] = df['GEOG'].apply(geography_codes_from_labels)
        
    df['Week'] = df['week_number'].apply(week_number_labels)
    df['week-number'] = df['Week'].apply(slugize)

    df['Sex'] = df['sex'].apply(lambda x: 'All' if x == 'All people' else x)
    df['sex'] = df['Sex'].apply(slugize)

    df['AgeGroups'] = df['age'].apply(age_groups_labels)
    df['age'] = df['AgeGroups'].apply(slugize)

    df['RegistrationOrOccurrence'] = df['tab_name'].apply(registration_or_occurrence_from_tab_name)
    df['registration-or-occurrence'] = df['RegistrationOrOccurrence'].apply(slugize)

    df = df[df['place'] == 'All places']
    assert len(df) != 0, "Length of df is 0 - something gone wrong"

    df = df.rename(columns={
            'OBS': 'v4_0',
            'TIME': 'Time',
            'GEOG': 'Geography',
            'age': 'age-groups',
            }
        )

    df = df[[
        'v4_0', 'calendar-years', 'Time', 'administrative-geography', 'Geography', 'week-number', 'Week',
        'sex', 'Sex', 'age-groups', 'AgeGroups', 'registration-or-occurrence', 'RegistrationOrOccurrence'
        ]]
    
    # check to see if registrations data has 2023 in it
    t = df[df["registration-or-occurrence"] == "registrations"]
    t = t[t["Time"] == "2023"]
    if len(t) > 0:
        raise Exception("Registrations data found for 2023 - will need to include this")
    
    df = df[df["Time"] != "2023"]
    assert len(df["Time"].unique()) == 1, "found more than one calendar year in weekly deaths by age & sex"

    df.to_csv(output_file, index=False)
    SparsityFiller(output_file, 'x')
    
    return output_file

def slugize(value):
    return value.lower().replace(' ', '-')

def geography_codes_from_labels(value):
    if "non-residents" in value:
        raise Exception("non-residents area has not been correctly removed from geography dimension")
    lookup = {
        'England and Wales': 'K04000001',
        'England': 'E92000001',
        'North East': 'E12000001',
        'North West': 'E12000002',
        'Yorkshire and The Humber': 'E12000003',
	    'East Midlands': 'E12000004',
	    'West Midlands': 'E12000005',
	    'East of England': 'E12000006',
	    'London': 'E12000007',
	    'South East': 'E12000008',
	    'South West': 'E12000009',
	    'Wales': 'W92000004'
            }
    return lookup[value]

def week_number_labels(value):
    week_number = int(float(value))
    return f"Week {week_number}"

def cause_of_death_codes(value):
    new_value = value.split('(')[0].strip()
    return new_value

def age_groups_labels(value):
    lookup = {
            'All ages': 'All ages',
            'Under 1': '0-1',
            '1 to 4': '1-4',
            '5 to 9': '5-9',
            '10 to 14': '10-14',
            '15 to 19': '15-19',
            '20 to 24': '20-24',
            '25 to 29': '25-29',
            '20 to 24': '20-24',
            '25 to 29': '25-29',
            '30 to 34': '30-34',
            '35 to 39': '35-39',
            '40 to 44': '40-44',
            '45 to 49': '45-49',
            '50 to 54': '50-54',
            '55 to 59': '55-59',
            '60 to 64': '60-64',
            '65 to 69': '65-69',
            '70 to 74': '70-74',
            '75 to 79': '75-79',
            '80 to 84': '80-84',
            '85 to 89': '85-89',
            '90 and over': '90+'
        }
    return lookup[value]

def registration_or_occurrence_from_tab_name(value):
    lookup = {'table_1': 'Registrations', 'table_2': 'Occurrences'}
    return lookup[value.lower()]

