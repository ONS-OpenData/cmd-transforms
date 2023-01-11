import pandas as pd
from databaker.framework import *
from code_list import get_codes_from_codelist

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
    assert len(files) == 2, f"transform only takes in 2 source file, not {len(files)} /n {files}"
    
    input_file = [f"{location}{file}" for file in files if not 'quality' in file][0]
    cv_file = [f"{location}{file}" for file in files if 'quality' in file][0]

    dataset_id = "wellbeing-local-authority"
    output_file = f"{location}v4-{dataset_id}.csv"
    
    tabs = loadxlstabs(input_file)
    tabs_cv = loadxlstabs(cv_file)
    
    tabs_thresholds = [tab for tab in tabs if 'thresholds' in tab.name.lower()]
    tabs_means = [tab for tab in tabs if 'mean' in tab.name.lower()]
    
    tabs_thresholds_cv = [tab for tab in tabs_cv if 'thresholds' in tab.name.lower()]
    tabs_means_cv = [tab for tab in tabs_cv if 'mean' in tab.name.lower()]

    # thresholds data
    conversionsegments, conversionsegments_data_accuracy = [], []
    for tab in tabs_thresholds:
    
        data_accuracy_indicators = tab.excel_ref('C14').expand(RIGHT).filter(contains_string('Data accuracy'))
        indicators = tab.excel_ref('C14').expand(RIGHT) - data_accuracy_indicators
        
        geography_labels = tab.excel_ref('A15').expand(DOWN).is_not_blank().is_not_whitespace()
        geography_codes = geography_labels.shift(1, 0)
        
        measures = tab.name
        
        observations = geography_codes.waffle(indicators)
        
        dimensions = [
                HDim(indicators, 'indicators', DIRECTLY, ABOVE),
                HDim(geography_codes, GEOG, DIRECTLY, LEFT),
                HDim(geography_labels, 'geography_labels', DIRECTLY, LEFT),
                HDimConst('measures', measures)
                ]
        
        conversionsegment = ConversionSegment(tab, dimensions, observations).topandas()
        conversionsegments.append(conversionsegment)
        
    thresholds_data = pd.concat(conversionsegments)
    
    # splitting time and indicators apart
    thresholds_data['Time'] = thresholds_data['indicators'].apply(timeFromIndicators)
    thresholds_data['Estimate'] = thresholds_data['indicators'].apply(wellbeingEstimateFromIndicators)

    thresholds_data = thresholds_data[[
            'OBS', 'DATAMARKER', 'Time', 'GEOG', 'geography_labels', 'measures', 'Estimate' 
            ]]
    
    # means data
    conversionsegments, conversionsegments_data_accuracy = [], []
    for tab in tabs_means:
        
        data_accuracy_time = tab.excel_ref('C14').expand(RIGHT).filter(contains_string('Data accuracy'))
        time = tab.excel_ref('C14').expand(RIGHT) - data_accuracy_time
        
        geography_labels = tab.excel_ref('A15').expand(DOWN).is_not_blank().is_not_whitespace()
        geography_codes = geography_labels.shift(1, 0)
        
        measures = tab.name
        
        indicators = 'Average (mean)'
        
        observations = geography_codes.waffle(time)
        
        dimensions = [
                HDim(time, 'Time', DIRECTLY, ABOVE),
                HDim(geography_codes, GEOG, DIRECTLY, LEFT),
                HDim(geography_labels, 'geography_labels', DIRECTLY, LEFT),
                HDimConst('measures', measures),
                HDimConst('Estimate', indicators)
                ]
        
        conversionsegment = ConversionSegment(tab, dimensions, observations).topandas()
        conversionsegments.append(conversionsegment)

    means_data = pd.concat(conversionsegments)

    means_data['Time'] = means_data['Time'].apply(timeFormatter)
    means_data = means_data[[
            'OBS', 'DATAMARKER', 'Time', 'GEOG', 'geography_labels', 'measures', 'Estimate' 
            ]]
    
    df = pd.concat([thresholds_data, means_data]).reset_index(drop=True)
    
    del thresholds_data, means_data, conversionsegments 

    # CV data for thresholds 
    conversionsegments_lcl, conversionsegments_ucl = [], []
    for tab in tabs_thresholds_cv:
    
        lcl = tab.excel_ref('C11').expand(RIGHT).filter(contains_string('LCL'))
        ucl = tab.excel_ref('C11').expand(RIGHT).filter(contains_string('UCL'))
        
        geography_labels = tab.excel_ref('A12').expand(DOWN).is_not_blank().is_not_whitespace()
        geography_codes = geography_labels.shift(1, 0)
        
        measures = tab.name
        
        obs_lcl = geography_codes.waffle(lcl)
        obs_ucl = geography_codes.waffle(ucl)
        
        dimensions_lcl = [
                HDim(lcl, 'CL', DIRECTLY, ABOVE),
                HDim(geography_codes, GEOG, DIRECTLY, LEFT),
                HDim(geography_labels, 'geography_labels', DIRECTLY, LEFT),
                HDimConst('measures', measures)
                ]
        
        dimensions_ucl = [
                HDim(ucl, 'CL', DIRECTLY, ABOVE),
                HDim(geography_codes, GEOG, DIRECTLY, LEFT),
                HDim(geography_labels, 'geography_labels', DIRECTLY, LEFT),
                HDimConst('measures', measures)
                ]
        
        conversionsegment = ConversionSegment(tab, dimensions_lcl, obs_lcl).topandas()
        conversionsegments_lcl.append(conversionsegment)
        
        conversionsegment = ConversionSegment(tab, dimensions_ucl, obs_ucl).topandas()
        conversionsegments_ucl.append(conversionsegment)
        
    lcl_thresholds_data = pd.concat(conversionsegments_lcl).reset_index(drop=True)
    ucl_thresholds_data = pd.concat(conversionsegments_ucl).reset_index(drop=True)
    
    lcl_thresholds_data.loc[lcl_thresholds_data['OBS'] == '', 'OBS'] = lcl_thresholds_data['DATAMARKER']
    ucl_thresholds_data.loc[ucl_thresholds_data['OBS'] == '', 'OBS'] = ucl_thresholds_data['DATAMARKER']
    
    # CV data for means 
    conversionsegments_lcl, conversionsegments_ucl = [], []
    for tab in tabs_means_cv:
        
        lcl = tab.excel_ref('C11').expand(RIGHT).filter(contains_string('LCL'))
        ucl = tab.excel_ref('C11').expand(RIGHT).filter(contains_string('UCL'))
        
        geography_labels = tab.excel_ref('A12').expand(DOWN).is_not_blank().is_not_whitespace()
        geography_codes = geography_labels.shift(1, 0)
        
        measures = tab.name
        
        indicators = 'Average (mean)'
        
        obs_lcl = geography_codes.waffle(lcl)
        obs_ucl = geography_codes.waffle(ucl)
        
        dimensions_lcl = [
                HDim(lcl, 'CL', DIRECTLY, ABOVE),
                HDim(geography_codes, GEOG, DIRECTLY, LEFT),
                HDim(geography_labels, 'geography_labels', DIRECTLY, LEFT),
                HDimConst('measures', measures),
                HDimConst('Estimate', indicators)
                ]
        
        dimensions_ucl = [
                HDim(ucl, 'CL', DIRECTLY, ABOVE),
                HDim(geography_codes, GEOG, DIRECTLY, LEFT),
                HDim(geography_labels, 'geography_labels', DIRECTLY, LEFT),
                HDimConst('measures', measures),
                HDimConst('Estimate', indicators)
                ]
        
        conversionsegment = ConversionSegment(tab, dimensions_lcl, obs_lcl).topandas()
        conversionsegments_lcl.append(conversionsegment)
        
        conversionsegment = ConversionSegment(tab, dimensions_ucl, obs_ucl).topandas()
        conversionsegments_ucl.append(conversionsegment)
        
    lcl_means_data = pd.concat(conversionsegments_lcl).reset_index(drop=True)
    ucl_means_data = pd.concat(conversionsegments_ucl).reset_index(drop=True)
    
    lcl_means_data.loc[lcl_means_data['OBS'] == '', 'OBS'] = lcl_means_data['DATAMARKER']
    ucl_means_data.loc[ucl_means_data['OBS'] == '', 'OBS'] = ucl_means_data['DATAMARKER']
    
    lcl_data = pd.concat([lcl_thresholds_data, lcl_means_data], sort=False).reset_index(drop=True)
    ucl_data = pd.concat([ucl_thresholds_data, ucl_means_data], sort=False).reset_index(drop=True)
    
    df['Lower limit'] = lcl_data['OBS']
    df['Upper limit'] = ucl_data['OBS']
    
    del conversionsegments_lcl, conversionsegments_ucl
    del lcl_means_data, lcl_thresholds_data, lcl_data, ucl_means_data, ucl_thresholds_data, ucl_data
    
    '''Post processing'''
    df['yyyy-yy'] = df['Time']
    
    df['Geography'] = df['GEOG'].apply(admin_labels)
    
    df['measures'] = df['measures'].apply(measureOfWellbeing)
    df['measure-of-wellbeing'] = df['measures'].apply(slugize)
    
    df['Estimate'] = df['Estimate'].apply(estimateLookup)
    df['wellbeing-estimate'] = df['Estimate'].apply(slugize)
    
    df = df.rename(columns={
            'OBS': 'v4_3',
            'DATAMARKER': 'Data marking',
            'GEOG': 'administrative-geography',
            'measures': 'MeasureOfWellbeing'
            }
    )
    
    df = df[[
            'v4_3', 'Data marking', 'Lower limit', 'Upper limit', 'yyyy-yy', 'Time',
            'administrative-geography', 'Geography', 'measure-of-wellbeing', 'MeasureOfWellbeing',
            'wellbeing-estimate', 'Estimate'
            ]]
    
    df.to_csv(output_file, index=False)
    print('Transform complete!')

    return {dataset_id: output_file}

'''Functions'''

def slugize(value):
    return value.replace('(', '').replace(')', '').replace(' ', '-').lower()

def estimateLookup(value):
    value = value.strip()
    
    lookup = {
            '(score 9 to 10)':'Very good',
            '(score 0 to 1)':'Very good',
            '(score 7 to 8)':'Good',
            '(score 2 to 3)':'Good',
            '(score 5 to 6)':'Fair',
            '(score 4 to 5)':'Fair',
            '(score 0 to 4)':'Poor',
            '(score 6 to 10)':'Poor',
            'Average (mean)':'Average (mean)'
            }
    return lookup[value]

def timeFromIndicators(value):
    # pulls out time from indicators in the threshold tabs
    time = value.split('\n')[0]
    assert time.startswith('April'), f"time not correctly extracted from indicators - {time}"
    start_year = time.split(' ')[1]
    end_year = time.split(' ')[-1]
    new_value = f"{start_year}-{end_year[-2:]}"
    return new_value

def wellbeingEstimateFromIndicators(value):
    # pulls out wellbeing estimate from indicators in the threshold tabs
    estimate = value.split('\n')[2]
    return estimate
    
def timeFormatter(value):
    # formats time correctly from means tabs
    start_year = value.split(' ')[1]
    end_year = value.split(' ')[-1]
    new_value = f"{start_year}-{end_year[-2:]}"
    return new_value

def measureOfWellbeing(value):
    if 'life satisfaction' in value.lower():
        return 'Life satisfaction'
    elif 'happiness' in value.lower():
        return 'Happiness'
    elif 'anxiety' in value.lower():
        return 'Anxiety'
    elif 'worthwhile' in value.lower():
        return 'Worthwhile'
    else:
        raise Exception(f"Unexpected measure of wellbeing - {value}")

admin_dict = get_codes_from_codelist("administrative-geography")

def admin_labels(value):
    return admin_dict[value]