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
    assert len(files) == 2, f"transform only takes in 2 source files, not {len(files)} /n {files}"
    
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
            
            assert tab.excel_ref('A14').value.lower() == 'time period', f"Cell A14 should be 'Time period' but found - {tab.excel_ref('A14').value}"
            time = tab.excel_ref('A15').expand(DOWN).is_not_blank().is_not_whitespace()
            
            geog = 'K02000001'
            
            measure = tab.name
            
            estimate_data_accuracy = tab.excel_ref('B14').expand(RIGHT).filter(contains_string('data accuracy'))
            estimate = tab.excel_ref('B14').expand(RIGHT) - estimate_data_accuracy
            
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
            
    df = pd.concat(conversionsegments)
    
    df['OBS'] = df['OBS'].apply(DataFormat)
    
    df['Time'] = df['TIME'].apply(TimeLabels)
    df['yyyy-qq'] = df['Time'].apply(Slugize)
    
    df['Geography'] = 'United Kingdom'
    
    df['MeasureOfWellbeing'] = df['measure'].apply(MeasureLabels)
    df['measure-of-wellbeing'] = df['MeasureOfWellbeing'].apply(Slugize)
    
    df['Estimate'] = df['estimate'].apply(EstimateLookup)
    df['wellbeing-estimate'] = df['Estimate'].apply(Slugize)
    
    df['SeasonalAdjustment'] = df['seasonaladjustment'].apply(SeasonalAdjustmentLabels)
    df['seasonal-adjustment'] = df['SeasonalAdjustment'].apply(Slugize)
    
    df = df.rename(columns={
            'OBS': 'v4_0',
            'GEOG': 'uk-only'
            }
    )
    
    df = df[[
            'v4_0', 'yyyy-qq', 'Time', 'uk-only', 'Geography',
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
        return 'Non Seasonal Adjustment'
    elif value == 'seasonallyadjusted':
        return 'Seasonal Adjustment'
    else:
        raise Exception(f"{value} - does not match seasonal adjustment labels")
    