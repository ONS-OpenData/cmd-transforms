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
    assert len(files) == 1, f"transform only takes in 1 source file, not {len(files)} /n {files}"
    
    # output used for upload
    output = {
            'weekly-deaths-health-board': '',
            'weekly-deaths-age-sex': '',
            'weekly-deaths-region': '',
            'weekly-deaths-local-authority': ''
            }
    
    # file for region and age&sex
    #published_file = [file for file in files if 'lahb' not in file.lower()]
    #assert len(published_file) == 1, f"found {len(published_file)} files for 'published_file', should be 1"
    #published_file = published_file[0]
    #tabs = loadxlstabs(published_file)
    
    # region data
    #output_file = weekly_deaths_by_region(tabs, location=location)
    #output['weekly-deaths-region'] = output_file

    # age&sex data
    #output_file = weekly_deaths_by_age_sex(tabs, location=location)
    #output['weekly-deaths-age-sex'] = output_file
    
    # file for health board and local authority
    year_of_data = '2023' # changes with each edition
    lahb_file = [file for file in files if 'lahb' in file.lower()][0]
        
    # health board and local authority data
    #reg_data = pd.read_excel(lahb_file, sheet_name='Registrations - All data', skiprows=3)
    #occ_data = pd.read_excel(lahb_file, sheet_name='Occurrences - All data', skiprows=3)
    reg_data = pd.read_excel(lahb_file, sheet_name='Table 1', skiprows=5)
    occ_data = pd.read_excel(lahb_file, sheet_name='Table 2', skiprows=5)

    output_file_hb, output_file_la = weekly_deaths_by_la_hb(reg_data, occ_data, year_of_data, location=location)
    output['weekly-deaths-health-board'] = output_file_hb
    output['weekly-deaths-local-authority'] = output_file_la
    
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


def weekly_deaths_by_age_sex(source_tabs, **kwargs):
    if 'location' in kwargs.keys():
        location = kwargs['location']
        if location == '':
            pass
        elif not location.endswith('/'):
            location += '/'
    else:
        location = ''
        
    dataset_id = "weekly-deaths-age-sex"
    output_file = f"{location}v4-{dataset_id}.csv"
    
    
    tabs = source_tabs
    tabs = [tab for tab in tabs if tab.name in ('2', '4', '5')]
    
    conversionsegments = []
    for tab in tabs:
        geography = 'K04000001'
        
        if tab.name == '2':
            # check to make sure data has not moved
            assert tab.excel_ref('A7').value == 'Week number', f"data seems to have moved, A7 should be 'Week number', not {tab.excel_ref('A6').value}"
            
            # table start points
            table_a = tab.excel_ref('A').filter(contains_string('Table 2a'))
            table_b = tab.excel_ref('A').filter(contains_string('Table 2b'))
            table_c = tab.excel_ref('A').filter(contains_string('Table 2c'))
            
            # table 2a
            week_number_a = table_a.shift(0, 2).expand(DOWN).is_not_blank().is_not_whitespace()
            week_number_a -= table_b.expand(DOWN)
            
            date_a = week_number_a.shift(1, 0)
            
            age_groups_a = table_a.shift(2, 1).expand(RIGHT).is_not_blank().is_not_whitespace()
            
            obs_a = week_number_a.waffle(age_groups_a)
            
            dimensions_a = [
                    HDim(date_a, TIME, DIRECTLY, LEFT),
                    HDimConst(GEOG, geography),
                    HDim(week_number_a, 'week_number', DIRECTLY, LEFT),
                    HDimConst('sex', 'All'),
                    HDim(age_groups_a, 'age', DIRECTLY, ABOVE),
                    HDimConst('death_type', 'Total registered deaths')
                    ]
            
            for cell in dimensions_a[0].hbagset:
                dimensions_a[0].AddCellValueOverride(cell, str(cell.value))
            
            conversionsegment = ConversionSegment(tab, dimensions_a, obs_a).topandas()
            conversionsegments.append(conversionsegment)
            
            # table 2b
            week_number_b = table_b.shift(0, 2).expand(DOWN).is_not_blank().is_not_whitespace()
            week_number_b -= table_c.expand(DOWN)
            
            date_b = week_number_b.shift(1, 0)
            
            age_groups_b = table_b.shift(2, 1).expand(RIGHT).is_not_blank().is_not_whitespace()
            
            obs_b = week_number_b.waffle(age_groups_b)
            
            dimensions_b = [
                    HDim(date_b, TIME, DIRECTLY, LEFT),
                    HDimConst(GEOG, geography),
                    HDim(week_number_b, 'week_number', DIRECTLY, LEFT),
                    HDimConst('sex', 'Male'),
                    HDim(age_groups_b, 'age', DIRECTLY, ABOVE),
                    HDimConst('death_type', 'Total registered deaths')
                    ]
            
            for cell in dimensions_b[0].hbagset:
                dimensions_b[0].AddCellValueOverride(cell, str(cell.value))
            
            conversionsegment = ConversionSegment(tab, dimensions_b, obs_b).topandas()
            conversionsegments.append(conversionsegment)
            
            # table 2c
            week_number_c = table_c.shift(0, 2).expand(DOWN).is_not_blank().is_not_whitespace()
            
            date_c = week_number_c.shift(1, 0)
            
            age_groups_c = table_c.shift(2, 1).expand(RIGHT).is_not_blank().is_not_whitespace()
            
            obs_c = week_number_c.waffle(age_groups_c)
            
            dimensions_c = [
                    HDim(date_c, TIME, DIRECTLY, LEFT),
                    HDimConst(GEOG, geography),
                    HDim(week_number_c, 'week_number', DIRECTLY, LEFT),
                    HDimConst('sex', 'Female'),
                    HDim(age_groups_c, 'age', DIRECTLY, ABOVE),
                    HDimConst('death_type', 'Total registered deaths')
                    ]
            
            for cell in dimensions_c[0].hbagset:
                dimensions_c[0].AddCellValueOverride(cell, str(cell.value))
            
            conversionsegment = ConversionSegment(tab, dimensions_c, obs_c).topandas()
            conversionsegments.append(conversionsegment)
            
        elif tab.name == '4':
            # check to make sure data has not moved
            assert tab.excel_ref('A7').value == 'Week number', f"data seems to have moved, A7 should be 'Week number', not {tab.excel_ref('A6').value}"
            
            # table start points
            table_a = tab.excel_ref('A').filter(contains_string('Table 4a'))
            table_b = tab.excel_ref('A').filter(contains_string('Table 4b'))
            table_c = tab.excel_ref('A').filter(contains_string('Table 4c'))
            
            # table 4a
            week_number_a = table_a.shift(0, 2).expand(DOWN).is_not_blank().is_not_whitespace()
            week_number_a -= table_b.expand(DOWN)
            
            date_a = week_number_a.shift(1, 0)
            
            age_groups_a = table_a.shift(2, 1).expand(RIGHT).is_not_blank().is_not_whitespace()
            
            obs_a = week_number_a.waffle(age_groups_a)
            
            dimensions_a = [
                    HDim(date_a, TIME, DIRECTLY, LEFT),
                    HDimConst(GEOG, geography),
                    HDim(week_number_a, 'week_number', DIRECTLY, LEFT),
                    HDimConst('sex', 'All'),
                    HDim(age_groups_a, 'age', DIRECTLY, ABOVE),
                    HDimConst('death_type', 'Deaths involving COVID-19: registrations')
                    ]
            
            for cell in dimensions_a[0].hbagset:
                dimensions_a[0].AddCellValueOverride(cell, str(cell.value))
            
            conversionsegment = ConversionSegment(tab, dimensions_a, obs_a).topandas()
            conversionsegments.append(conversionsegment)
            
            # table 4b
            week_number_b = table_b.shift(0, 2).expand(DOWN).is_not_blank().is_not_whitespace()
            week_number_b -= table_c.expand(DOWN)
            
            date_b = week_number_b.shift(1, 0)
            
            age_groups_b = table_b.shift(2, 1).expand(RIGHT).is_not_blank().is_not_whitespace()
            
            obs_b = week_number_b.waffle(age_groups_b)
            
            dimensions_b = [
                    HDim(date_b, TIME, DIRECTLY, LEFT),
                    HDimConst(GEOG, geography),
                    HDim(week_number_b, 'week_number', DIRECTLY, LEFT),
                    HDimConst('sex', 'Male'),
                    HDim(age_groups_b, 'age', DIRECTLY, ABOVE),
                    HDimConst('death_type', 'Deaths involving COVID-19: registrations')
                    ]
            
            for cell in dimensions_b[0].hbagset:
                dimensions_b[0].AddCellValueOverride(cell, str(cell.value))
            
            conversionsegment = ConversionSegment(tab, dimensions_b, obs_b).topandas()
            conversionsegments.append(conversionsegment)
            
            # table 4c
            week_number_c = table_c.shift(0, 2).expand(DOWN).is_not_blank().is_not_whitespace()
            
            date_c = week_number_c.shift(1, 0)
            
            age_groups_c = table_c.shift(2, 1).expand(RIGHT).is_not_blank().is_not_whitespace()
            
            obs_c = week_number_c.waffle(age_groups_c)
            
            dimensions_c = [
                    HDim(date_c, TIME, DIRECTLY, LEFT),
                    HDimConst(GEOG, geography),
                    HDim(week_number_c, 'week_number', DIRECTLY, LEFT),
                    HDimConst('sex', 'Female'),
                    HDim(age_groups_c, 'age', DIRECTLY, ABOVE),
                    HDimConst('death_type', 'Deaths involving COVID-19: registrations')
                    ]
            
            for cell in dimensions_c[0].hbagset:
                dimensions_c[0].AddCellValueOverride(cell, str(cell.value))
            
            conversionsegment = ConversionSegment(tab, dimensions_c, obs_c).topandas()
            conversionsegments.append(conversionsegment)
            
        elif tab.name == '5':
            # check to make sure data has not moved
            assert tab.excel_ref('A7').value == 'Week number', f"data seems to have moved, A7 should be 'Week number', not {tab.excel_ref('A6').value}"
            
            # table start points
            table_a = tab.excel_ref('A').filter(contains_string('Table 5a'))
            table_b = tab.excel_ref('A').filter(contains_string('Table 5b'))
            table_c = tab.excel_ref('A').filter(contains_string('Table 5c'))
            
            # table 5a
            week_number_a = table_a.shift(0, 2).expand(DOWN).is_not_blank().is_not_whitespace()
            week_number_a -= table_b.expand(DOWN)
            
            date_a = week_number_a.shift(1, 0)
            
            age_groups_a = table_a.shift(2, 1).expand(RIGHT).is_not_blank().is_not_whitespace()
            
            obs_a = week_number_a.waffle(age_groups_a)
            
            dimensions_a = [
                    HDim(date_a, TIME, DIRECTLY, LEFT),
                    HDimConst(GEOG, geography),
                    HDim(week_number_a, 'week_number', DIRECTLY, LEFT),
                    HDimConst('sex', 'All'),
                    HDim(age_groups_a, 'age', DIRECTLY, ABOVE),
                    HDimConst('death_type', 'Deaths involving COVID-19: occurrences')
                    ]
            
            for cell in dimensions_a[0].hbagset:
                dimensions_a[0].AddCellValueOverride(cell, str(cell.value))
            
            conversionsegment = ConversionSegment(tab, dimensions_a, obs_a).topandas()
            conversionsegments.append(conversionsegment)
            
            # table 5b
            week_number_b = table_b.shift(0, 2).expand(DOWN).is_not_blank().is_not_whitespace()
            week_number_b -= table_c.expand(DOWN)
            
            date_b = week_number_b.shift(1, 0)
            
            age_groups_b = table_b.shift(2, 1).expand(RIGHT).is_not_blank().is_not_whitespace()
            
            obs_b = week_number_b.waffle(age_groups_b)
            
            dimensions_b = [
                    HDim(date_b, TIME, DIRECTLY, LEFT),
                    HDimConst(GEOG, geography),
                    HDim(week_number_b, 'week_number', DIRECTLY, LEFT),
                    HDimConst('sex', 'Male'),
                    HDim(age_groups_b, 'age', DIRECTLY, ABOVE),
                    HDimConst('death_type', 'Deaths involving COVID-19: occurrences')
                    ]
            
            for cell in dimensions_b[0].hbagset:
                dimensions_b[0].AddCellValueOverride(cell, str(cell.value))
            
            conversionsegment = ConversionSegment(tab, dimensions_b, obs_b).topandas()
            conversionsegments.append(conversionsegment)
            
            # table 5c
            week_number_c = table_c.shift(0, 2).expand(DOWN).is_not_blank().is_not_whitespace()
            
            date_c = week_number_c.shift(1, 0)
            
            age_groups_c = table_c.shift(2, 1).expand(RIGHT).is_not_blank().is_not_whitespace()
            
            obs_c = week_number_c.waffle(age_groups_c)
            
            dimensions_c = [
                    HDim(date_c, TIME, DIRECTLY, LEFT),
                    HDimConst(GEOG, geography),
                    HDim(week_number_c, 'week_number', DIRECTLY, LEFT),
                    HDimConst('sex', 'Female'),
                    HDim(age_groups_c, 'age', DIRECTLY, ABOVE),
                    HDimConst('death_type', 'Deaths involving COVID-19: occurrences')
                    ]
            
            for cell in dimensions_c[0].hbagset:
                dimensions_c[0].AddCellValueOverride(cell, str(cell.value))
            
            conversionsegment = ConversionSegment(tab, dimensions_c, obs_c).topandas()
            conversionsegments.append(conversionsegment)
            
    df = pd.concat(conversionsegments)
    
    ''' Post processing '''
    df['OBS'] = df['OBS'].apply(V4Integers)
    
    df['Time'] = df['TIME'].apply(YearExtractor)
    df['Time_codelist'] = df['Time']
    
    df['Geography'] = 'England and Wales'
    
    df['week_number'] = df['week_number'].apply(lambda x: str(int(float(x))))
    df['week_number'] = 'Week ' + df['week_number']
    df['week_number_codelist'] = df['week_number'].apply(Slugize)
    
    df['sex_codelist'] = df['sex'].apply(Slugize)
    
    df['age'] = df['age'].apply(AgeLabels)
    df['age'] = df['age'].apply(AgeLabelsCorrector)
    df['age_codelist'] = df['age'].apply(AgeCodes)
    
    df['death_type_codelist'] = df['death_type'].apply(Slugize)
    
    df = df.rename(columns={
            'OBS':'v4_0',
            'Time_codelist':'calendar-years',
            'GEOG':'administrative-geography',
            'week_number_codelist':'week-number',
            'week_number':'Week',
            'sex':'Sex',
            'sex_codelist':'sex',
            'age_codelist':'age-groups',
            'age':'AgeGroups',
            'death_type_codelist':'recorded-deaths',
            'death_type':'Deaths'
            }
        )
    
    df = df[[
            'v4_0', 'calendar-years', 'Time', 'administrative-geography', 'Geography',
            'week-number', 'Week', 'sex', 'Sex', 'age-groups', 'AgeGroups', 'recorded-deaths', 'Deaths'
            ]]
    
    # pull latest v4 from CMD
    latest_df = get_latest_version('weekly-deaths-age-sex', 'covid-19')
    
    # removed pre filled sparsity
    latest_df = latest_df[latest_df['Data Marking'] != 'x']
    latest_df = latest_df.rename(columns={'V4_1':'v4_0', 'v4_1':'v4_0'}).drop(['Data Marking'], axis=1)
    latest_df = latest_df.reset_index(drop=True)
    
    # fix any incorrect age labels
    latest_df['AgeGroups'] = latest_df['AgeGroups'].apply(AgeCorrector)
    
    # combine latest version with new version
    new_df = pd.concat([df, latest_df])
    
    # removing duplicates
    # dataframe without obs to find any duplicates
    temp_df = new_df.drop(['v4_0'], axis=1).reset_index(drop=True)
    temp_df = temp_df.drop_duplicates()
    # index of rows to keep
    index_to_keep = temp_df.index
    new_df = new_df.iloc[index_to_keep]
    
    V4Checker(new_df, 'age-sex')
    new_df.to_csv(output_file, index=False)
    SparsityFiller(output_file, 'x')
    
    return output_file


def weekly_deaths_by_la_hb(registration_tabs, occurrence_tabs, year, **kwargs):
    if 'location' in kwargs.keys():
        location = kwargs['location']
        if location == '':
            pass
        elif not location.endswith('/'):
            location += '/'
    else:
        location = ''
        
    dataset_id_la = "weekly-deaths-local-authority"
    output_file_la = f"{location}v4-{dataset_id_la}.csv"
    dataset_id_hb = "weekly-deaths-health-board"
    output_file_hb = f"{location}v4-{dataset_id_hb}.csv"
    
    year_of_data = year
    
    reg_data = registration_tabs.rename(columns=lambda x: x.strip().lower())
    occ_data = occurrence_tabs.rename(columns=lambda x: x.strip().lower())
    
    #add registration or occurrence
    reg_data['registrationoroccurrence'] = 'Registrations'
    occ_data['registrationoroccurrence'] = 'Occurrences'
    
    df = pd.concat([reg_data, occ_data])
    
    df['calendar-years'] = year_of_data
    df['time'] = df['calendar-years']
    
    df['cause-of-death'] = df['cause of death'].apply(Slugize)
    df['place-of-death'] = df['place of death'].apply(Slugize)
    df['registration-or-occurrence'] = df['registrationoroccurrence'].apply(Slugize)
    
    df['week-number'] = df['week number'].apply(lambda x: 'week-' + str(x))
    df['week number'] = 'Week ' + df['week number'].apply(WeekNumberLabels)
    
    df = df.rename(columns={
            'number of deaths':'v4_0',
            'deaths':'v4_0',
            'time':'Time',
            'cause of death':'CauseOfDeath',
            'place of death':'PlaceOfDeath',
            'week number':'Week',
            'area name':'Geography',
            'registrationoroccurrence':'RegistrationOrOccurrence'
            }
        )

    df = df[[
            'v4_0', 'calendar-years', 'Time', 'area code', 'Geography', 'geography type', 
            'week-number', 'Week', 'cause-of-death', 'CauseOfDeath', 'place-of-death', 'PlaceOfDeath',
            'registration-or-occurrence', 'RegistrationOrOccurrence'
            ]]
    
    df_hb = df[df['geography type'] != 'Local Authority'].drop(['geography type'], axis=1).rename(columns={
            'area code':'local-health-board'
            }
    )
    df_la = df[df['geography type'] == 'Local Authority'].drop(['geography type'], axis=1).rename(columns={
            'area code':'administrative-geography'
            }
    )
    
    V4Checker(df_hb, 'health-board')
    df_hb.to_csv(output_file_hb, index=False)
    SparsityFiller(output_file_hb)
    
    V4Checker(df_la, 'local-authority')
    df_la.to_csv(output_file_la, index=False)
    SparsityFiller(output_file_la)

    return output_file_hb, output_file_la
    













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

