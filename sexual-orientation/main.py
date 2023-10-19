from databaker.framework import *
import pandas as pd

def transform(files, **kwargs):
    if 'location' in kwargs.keys():
        location = kwargs['location']
    else:
        location = ''
        
    assert type(files) == list, f"transform takes in a list, not {type(files)}"
    assert len(files) == 1, f"transform only takes in 1 source file, not {len(files)} /n {files}"
    source_file = files[0]
    
    # output used for upload
    output = {
            'sexual-orientation-by-region': '',
            'sexual-orientation-by-age-and-sex': ''
            }
    
    source_tabs = loadxlstabs(source_file)

    output_file = sexual_orientation_by_region(source_tabs, location=location)
    output['sexual-orientation-by-region'] = output_file

    output_file = sexual_orientation_by_age_and_sex(source_tabs, location=location)
    output['sexual-orientation-by-age-and-sex'] = output_file

    return output
    
def sexual_orientation_by_region(source_tabs, **kwargs):
    if 'location' in kwargs.keys():
        location = kwargs['location']
        if location == '':
            pass
        elif not location.endswith('/'):
            location += '/'
    else:
        location = ''
        
    dataset_id = "sexual-orientation-by-region"
    output_file = f"{location}v4-{dataset_id}.csv"

    tabs = [tab for tab in source_tabs if '1' in tab.name]

    conversionsegments = []
    for tab in tabs:
        # quick check to make sure data has not moved since last publish
        assert tab.excel_ref('A12').value == "Sexual Orientation", f"Data seems to have moved, cell A12 should be Sexual Orientation not {tab.excel_ref('A12').value}"
        
        year = tab.excel_ref('B13').expand(DOWN).is_not_blank().is_not_whitespace()
        
        sexual_orientation = tab.excel_ref('A13').expand(DOWN).is_not_blank().is_not_whitespace()
        
        geography = tab.excel_ref('C12').expand(RIGHT).is_not_blank().is_not_whitespace()
        
        measure = tab.name
        
        if tab.name.endswith('a'):
            obs_columns = tab.excel_ref("C12").expand(RIGHT).filter(contains_string('Estimate')) 
        elif tab.name.endswith('b'):
            obs_columns = tab.excel_ref("C12").expand(RIGHT).filter(contains_string('Percentage'))
        
        obs = sexual_orientation.waffle(obs_columns)
        cv = obs.shift(1, 0)
        ci = obs.shift(2, 0)
        
        dimensions = [
                HDim(year, TIME, DIRECTLY, LEFT),
                HDim(geography, GEOG, DIRECTLY, ABOVE),
                HDim(sexual_orientation, 'sexual_orientation', DIRECTLY, LEFT),
                HDimConst('measure', measure),
                HDim(cv, 'cv', DIRECTLY, RIGHT),
                HDim(ci, 'ci', DIRECTLY, RIGHT)
                ]
        
        conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
        conversionsegments.append(conversionsegment)
    
    '''Post Processing'''
    df = pd.concat(conversionsegments).reset_index(drop=True)
    
    df['OBS'] = df['OBS'].apply(numberAsInteger)
    
    df['TIME'] = df['TIME'].apply(yearTidy)
    df['calendar-years'] = df['TIME']
    
    df['GEOG'] = df['GEOG'].apply(geogTidy)
    df['administrative-geography'] = df['GEOG'].apply(adminGeogCodes)
    df['GEOG'] = df['GEOG'].apply(adminGeogLabels)
    
    df['sexual-orientation'] = df['sexual_orientation'].apply(slugize)
    
    df['UnitOfMeasure'] = df['measure'].apply(unitOfMeasureLabels)
    df['unit-of-measure'] = df['UnitOfMeasure'].apply(slugize)
    df['UnitOfMeasure'] = df['UnitOfMeasure'].apply(lambda x: x.replace('Number', 'Number of people (thousands)'))
    
    df['DATAMARKER'] = df['DATAMARKER'].apply(dataMarkings)
    
    df['ci'] = df['ci'].apply(dataMarkings)
    df['ci'] = df['ci'].apply(numberAsInteger)
    
    df['cv'] = df['cv'].apply(dataMarkings)
    
    df = df.rename(columns={
            'OBS':'v4_3',
            'TIME':'Time',
            'GEOG':'Geography',
            'sexual_orientation':'SexualOrientation',
            'cv':'CV',
            'ci':'CI+/-',
            'DATAMARKER':'Data Marking'
            }
    )
    
    df = df[['v4_3', 'Data Marking', 'CV', 'CI+/-', 'calendar-years', 'Time', 
             'administrative-geography', 'Geography', 'sexual-orientation', 'SexualOrientation',
             'unit-of-measure', 'UnitOfMeasure']]
    
    df.loc[df['unit-of-measure'] =='percentage', 'v4_3'] = df['v4_3'].apply(percentage_value_tidy)
    
    df.to_csv(output_file, index=False)
    return output_file

def sexual_orientation_by_age_and_sex(source_tabs, **kwargs):
    if 'location' in kwargs.keys():
        location = kwargs['location']
        if location == '':
            pass
        elif not location.endswith('/'):
            location += '/'
    else:
        location = ''
        
    dataset_id = "sexual-orientation-by-age-and-sex"
    output_file = f"{location}v4-{dataset_id}.csv"

    tabs = [tab for tab in source_tabs if '7' in tab.name]
    
    conversionsegments = []
    for tab in tabs:
        # quick check to make sure data has not moved since last publish
        assert tab.excel_ref('A12').value == "Sexual Orientation", f"Data seems to have moved, cell A12 should be Sexual Orientation not {tab.excel_ref('A12').value}"
        
        year = tab.excel_ref('C13').expand(DOWN).is_not_blank().is_not_whitespace()
        
        sex = tab.excel_ref('B13').expand(DOWN).is_not_blank().is_not_whitespace()
        
        sexual_orientation = tab.excel_ref('A13').expand(DOWN).is_not_blank().is_not_whitespace()
        
        age = tab.excel_ref('D12').expand(RIGHT).is_not_blank().is_not_whitespace()
        
        measure = tab.name
        
        if tab.name.endswith('a'):
            obs_columns = tab.excel_ref("D12").expand(RIGHT).filter(contains_string('Estimate')) 
        elif tab.name.endswith('b'):
            obs_columns = tab.excel_ref("D12").expand(RIGHT).filter(contains_string('Percentage'))
        
        obs = sexual_orientation.waffle(obs_columns)
        cv = obs.shift(1, 0)
        ci = obs.shift(2, 0)
        
        dimensions = [
                HDim(year, TIME, DIRECTLY, LEFT),
                HDimConst(GEOG, 'K02000001'),
                HDim(sex, 'sex', DIRECTLY, LEFT),
                HDim(sexual_orientation, 'sexual_orientation', DIRECTLY, LEFT),
                HDim(age, 'age-groups', DIRECTLY, ABOVE),
                HDimConst('measure', measure),
                HDim(cv, 'cv', DIRECTLY, RIGHT),
                HDim(ci, 'ci', DIRECTLY, RIGHT)
                ]
        
        conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
        conversionsegments.append(conversionsegment)
    
    '''Post Processing'''
    df = pd.concat(conversionsegments).reset_index(drop=True)
    
    df['OBS'] = df['OBS'].apply(numberAsInteger)
    
    df['TIME'] = df['TIME'].apply(yearTidy)
    df['calendar-years'] = df['TIME']
    
    df['Geography'] = 'United Kingdom'
    
    df['age-groups'] = df['age-groups'].apply(ageGroupsTidy)
    df['AgeGroups'] = df['age-groups']
    
    df['Sex'] = df['sex'].apply(sexLabels)
    df['sex'] = df['Sex'].apply(slugize)
    
    df['sexual-orientation'] = df['sexual_orientation'].apply(slugize)
    
    df['UnitOfMeasure'] = df['measure'].apply(unitOfMeasureLabels)
    df['unit-of-measure'] = df['UnitOfMeasure'].apply(slugize)
    df['UnitOfMeasure'] = df['UnitOfMeasure'].apply(lambda x: x.replace('Number', 'Number of people (thousands)'))
    
    if 'DATAMARKER' in df.columns:
        raise Exception('Data marker column has not been accounted for..')
    
    df['ci'] = df['ci'].apply(dataMarkings)
    df['ci'] = df['ci'].apply(numberAsInteger)
    
    df['cv'] = df['cv'].apply(dataMarkings)
    
    df = df.rename(columns={
            'OBS':'v4_2',
            'TIME':'Time',
            'GEOG':'uk-only',
            'sexual_orientation':'SexualOrientation',
            'cv':'CV',
            'ci':'CI+/-'
            }
        )
    
    df = df[['v4_2', 'CV', 'CI+/-', 'calendar-years', 'Time', 'uk-only', 'Geography',
             'age-groups', 'AgeGroups', 'sex', 'Sex', 'sexual-orientation', 'SexualOrientation',
             'unit-of-measure', 'UnitOfMeasure']]
    
    df.loc[df['unit-of-measure'] =='percentage', 'v4_2'] = df['v4_2'].apply(percentage_value_tidy)
    
    df.to_csv(output_file, index=False)
    return output_file

def slugize(value):
    new_value = value.replace("'", "").replace(' ', '-').lower()
    return new_value

def numberAsInteger(value):
    # returns a number as an int - removes '.0'
    new_value = str(value)
    if new_value.endswith('.0'):
        new_value = new_value[:-2]
    return new_value

def adminGeogCodes(value):
    # returns admin geography code from label
    lookup = {
            'North East':'E12000001', 
            'North West':'E12000002', 
            'Yorkshire and The Humber':'E12000003',
            'East Midlands':'E12000004', 
            'West Midlands':'E12000005', 
            'East':'E12000006', 
            'London':'E12000007', 
            'South East':'E12000008',
            'South West':'E12000009', 
            'England':'E92000001', 
            'Wales':'W92000004', 
            'Scotland':'S92000003', 
            'Northern Ireland':'N92000002',
            'UK':'K02000001'
            }
    return lookup[value]

def adminGeogLabels(value):
    # tidies up geography labels
    lookup = {
            'UK':'United Kingdom',
            'East':'East of England'
            }
    return lookup.get(value, value)

def unitOfMeasureLabels(value):
    # returns labels for unit of measure
    if value.endswith('a'):
        return 'Number'
    elif value.endswith('b'):
        return 'Percentage'

def sexLabels(value):
    # returns labels for sex
    lookup = {
            'M':'Male',
            'F':'Female'
            }
    return lookup[value]

def geogTidy(value):
    if 'Estimate' in value:
        new_value = value.split('Estimate')[0].strip()
    elif 'Percentage' in value:
        new_value = value.split('Percentage')[0].strip()
    else:
        new_value = value
    new_value = new_value.replace('\n', ' ')
    return new_value

def dataMarkings(value):
    if value == '[x]':
        return 'x'
    else:
        return value
    
def yearTidy(value):
    if 'note' in value:
        new_value = value.split('[')[0].strip()
        return new_value
    else:
        return value
    
def ageGroupsTidy(value):
    if 'Estimate' in value:
        new_value = value.split('Estimate')[0].strip()
    elif 'Percentage' in value:
        new_value = value.split('Percentage')[0].strip()
    else:
        new_value = value
    new_value = new_value.replace('\n', ' ')
    return new_value

def percentage_value_tidy(value):
    if pd.isnull(value):
        return value
    elif value == '':
        return value
    
    if not '.' in value:
        new_value = f"{value}.0"
        return new_value
    else:
        return value