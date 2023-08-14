from databaker.framework import *
import pandas as pd

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
    assert len(files) == 3, f"transform only takes in 3 source files, not {len(files)} /n {files}"
    files_ci = [file for file in files if 'quality' in file][0]
    files = [file for file in files if 'quality' not in file]
    
    dataset_id = "wellbeing-quarterly"
    output_file = f"{location}v4-{dataset_id}.csv"
    
    conversionsegments = []
    for file in files:
        if 'nonseasonallyadjusted' in file:
            seasonaladjustment = 'nonseasonallyadjusted'
        else:
            seasonaladjustment = 'seasonallyadjusted'  
        
        tabs = loadxlstabs(file)
        tabs = [tab for tab in tabs if 'UK' in tab.name]
        
        '''DataBaking'''
        for tab in tabs:
            
            start_point = str(tab.excel_ref('A').filter(contains_string('Time period')).y + 1)
            assert tab.excel_ref(f'A{start_point}').value.lower() == 'time period', f"Cell A{start_point} should be 'Time period' but found - {tab.excel_ref(f'A{start_point}').value}"
            time = tab.excel_ref(f'A{int(start_point)+1}').expand(DOWN).is_not_blank().is_not_whitespace()
            
            geog = 'K02000001'
            
            measure = tab.name
            
            estimate_data_accuracy = tab.excel_ref(f'B{start_point}').expand(RIGHT).filter(contains_string('data accuracy'))
            estimate = tab.excel_ref(f'B{start_point}').expand(RIGHT) - estimate_data_accuracy
            
            obs = time.waffle(estimate)
            
            dimensions = [
                    HDim(time, TIME, DIRECTLY, LEFT),
                    HDimConst(GEOG, geog),
                    HDimConst('measure', measure),
                    HDim(estimate, 'estimate', DIRECTLY, ABOVE),
                    HDimConst('seasonaladjustment', seasonaladjustment)
                    ]
            
            conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
            conversionsegments.append(conversionsegment)
            
    tabs_ci = loadxlstabs(files_ci)
    tabs_ci = [tab for tab in tabs_ci if 'UK' in tab.name]
    
    conversionsegments_lcl, conversionsegments_ucl = [], []
    for tab in tabs_ci:
        assert tab.excel_ref('A11').value.lower() == 'time period', f"Cell A11 should be 'Time period' but found - {tab.excel_ref('A11').value}"
        time = tab.excel_ref('A12').expand(DOWN).is_not_blank().is_not_whitespace()
        
        geog = 'K02000001'
            
        measure = tab.name
        
        estimate_lcl = tab.excel_ref('B11').expand(RIGHT).filter(contains_string('LCL'))
        estimate_ucl = tab.excel_ref('B11').expand(RIGHT).filter(contains_string('UCL'))
        
        obs_lcl = time.waffle(estimate_lcl)
        obs_ucl = time.waffle(estimate_ucl)
        
        dimensions_lcl = [
                HDim(time, TIME, DIRECTLY, LEFT),
                HDimConst(GEOG, geog),
                HDimConst('measure', measure),
                HDim(estimate_lcl, 'estimate', DIRECTLY, ABOVE),
                HDimConst('seasonaladjustment', '')
                ]
        
        dimensions_ucl = [
                HDim(time, TIME, DIRECTLY, LEFT),
                HDimConst(GEOG, geog),
                HDimConst('measure', measure),
                HDim(estimate_ucl, 'estimate', DIRECTLY, ABOVE),
                HDimConst('seasonaladjustment', '')
                ]
        
        conversionsegment = ConversionSegment(tab, dimensions_lcl, obs_lcl).topandas()
        conversionsegments_lcl.append(conversionsegment)
        conversionsegment = ConversionSegment(tab, dimensions_ucl, obs_ucl).topandas()
        conversionsegments_ucl.append(conversionsegment)
            
    df = pd.concat(conversionsegments).reset_index(drop=True)
    df_lcl = pd.concat(conversionsegments_lcl).reset_index(drop=True)
    df_ucl = pd.concat(conversionsegments_ucl).reset_index(drop=True)
    
    df_lcl = pd.concat([df_lcl, df_lcl]).reset_index(drop=True)
    df_ucl = pd.concat([df_ucl, df_ucl]).reset_index(drop=True)
    
    df['LCL'] = df_lcl['OBS']
    df['UCL'] = df_ucl['OBS']
    
    df['OBS'] = df['OBS'].apply(DataFormat)
    
    df['Time'] = df['TIME'].apply(TimeLabels)
    df['yyyy-qq'] = df['Time'].apply(Slugize)
    
    df['Geography'] = 'United Kingdom'
    
    df['MeasureOfWellbeing'] = df['measure'].apply(MeasureLabels)
    df['measure-of-wellbeing'] = df['MeasureOfWellbeing'].apply(Slugize)
    
    df['Estimate'] = df['estimate'].apply(EstimateLookup)
    df['wellbeing-estimate'] = df['Estimate'].apply(Slugize)
    
    df['SeasonalAdjustment'] = df['seasonaladjustment'].apply(SeasonalAdjustmentLabels)
    df['seasonal-adjustment'] = df['seasonaladjustment'].apply(SeasonalAdjustmentCodes)

    df.loc[df['seasonal-adjustment'] == 'seasonal-adjustment', 'LCL'] = ''
    df.loc[df['seasonal-adjustment'] == 'seasonal-adjustment', 'UCL'] = ''
    
    df = df.rename(columns={
            'OBS': 'v4_2',
            'GEOG': 'uk-only'
            }
    )
    
    df = df[[
            'v4_2', 'LCL', 'UCL', 'yyyy-qq', 'Time', 'uk-only', 'Geography',
            'measure-of-wellbeing', 'MeasureOfWellbeing', 'wellbeing-estimate', 'Estimate',
            'seasonal-adjustment', 'SeasonalAdjustment'
            ]]
    
    df.to_csv(output_file, index=False)
    return {dataset_id: output_file}


def Slugize(value):
    return value.replace('(', '').replace(')', '').replace(' ', '-').lower()

def TimeLabels(value):
    year = value.split(' ')[-2]
    quarter = value.split(' ')[-1][1:-1]
    new_value = f"{year} {quarter}"
    return new_value

def EstimateLookup(value):
    score = value.split('\n')[2].lower()
    lookup = {
            '(score 9 to 10)': 'Very good',
            '(score 0 to 1)': 'Very good',
            '(score 7 to 8)': 'Good',
            '(score 2 to 3)': 'Good',
            '(score 5 to 6)': 'Fair',
            '(score 4 to 5)': 'Fair',
            '(score 0 to 4)': 'Poor',
            '(score 6 to 10)': 'Poor',
            'out of 10': 'Average (mean)'
            }
    return lookup[score]

def MeasureLabels(value):
    if 'life satisfaction' in value.lower():
        return 'Life satisfaction'
    elif 'worthwhile' in value.lower():
        return 'Worthwhile'
    elif 'happiness' in value.lower():
        return 'Happiness'
    elif 'anxiety' in value.lower():
        return 'Anxiety'
    
def DataFormat(value):
    new_value = str(value)
    after_decimal = new_value.split('.')[-1]
    if len(after_decimal) == 1:
        return new_value + '0'
    else:
        return new_value
    
def SeasonalAdjustmentLabels(value):
    if value == 'nonseasonallyadjusted':
        return 'Non-seasonally adjusted'
    elif value == 'seasonallyadjusted':
        return 'Seasonally adjusted'
    else:
        raise Exception(f"{value} - does not match seasonal adjustment labels")
    
def SeasonalAdjustmentCodes(value):
    if value == 'nonseasonallyadjusted':
        return 'non-seasonal-adjustment'
    elif value == 'seasonallyadjusted':
        return 'seasonal-adjustment'
    else:
        raise Exception(f"{value} - does not match seasonal adjustment codes")
    