
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
    
    dataset_id = "output-in-the-construction-industry"
    output_file = f"{location}v4-{dataset_id}.csv"
  
    tabs_to_load = [
          'Table 1a', 'Table 1b', 'Table 2a', 'Table 2b', 
          'Table 3a', 'Table 3b'
          ]
    tabs = loadxlstabs(file, tabs_to_load)
  
    conversionsegments = []
    for tab in tabs:

        tab_name = tab.name
        
        if tab .name in ('Table 3a', 'Table 3b'):
            
            assert tab.excel_ref('B6').value == 'New housing public', f"Cell 'B6' should contain 'New housing public' not '{tab.excel_ref('B5').value}'"
            category = tab.excel_ref('B6').expand(RIGHT).is_not_blank().is_not_whitespace()
            
            time = tab.excel_ref('A10').expand(DOWN).is_not_blank().is_not_whitespace()
            
        else:
            
            # quick check on cell B5 
            assert tab.excel_ref('B5').value == 'New housing public', f"Cell 'B5' should contain 'New housing public' not '{tab.excel_ref('B5').value}'"
            category = tab.excel_ref('B5').expand(RIGHT).is_not_blank().is_not_whitespace()
            
            time = tab.excel_ref('A7').expand(DOWN).is_not_blank().is_not_whitespace()
        
        obs = time.waffle(category)
        
        dimensions = [
              HDim(time, TIME, DIRECTLY, LEFT),
              HDimConst('quarters_months', ''),
              HDimConst(GEOG, 'K03000001'),
              HDimConst('tab_name', tab_name),
              HDim(category, 'category', DIRECTLY, ABOVE)
              ]
        
        for cell in dimensions[0].hbagset:
            dimensions[0].AddCellValueOverride(cell, str(cell.value))
        
        conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
        conversionsegments.append(conversionsegment) 
        
    df = pd.concat(conversionsegments, sort=True)
  
    # tidying up
    df['TIME'] = df['TIME'].apply(lambda x: x.strip())
    df['quarters_months'] = df['quarters_months'].apply(lambda x: x.strip())
    df['category'] = df['category'].apply(lambda x: x.strip())
    
    df['TIME'] = df['TIME'].apply(timeCodesNew)
    df['years-quarters-months'] = df['TIME'].apply(slugize)
  
    df['Geography'] = 'Great Britain'
  
    df['seasonal-adjustment'] = df['tab_name'].apply(seasonallyAdjustedFromTableNumber)
    df['SeasonalAdjustment'] = df['seasonal-adjustment'].apply(seasonallyAdjustedLabels)
  
    df['SeriesType'] = df['tab_name'].apply(seriesTypeFromTableNumber)
    df['construction-series-type'] = df['SeriesType'].apply(slugize)
  
    df['construction-classifications'] = df['category'].apply(constructionClassificationCodes)
    df['TypeOfWork'] = df['category']  
  
    df = df.rename(columns={
          'OBS':'v4_1',
          'DATAMARKER':'Data Marking',
          'TIME':'Time', 
          'GEOG':'administrative-geography'
              }
        )
  
    df = df[[
          'v4_1', 'Data Marking', 'years-quarters-months', 'Time', 'administrative-geography', 'Geography',
          'seasonal-adjustment', 'SeasonalAdjustment', 'construction-series-type', 'SeriesType',
          'construction-classifications', 'TypeOfWork'
          ]]
  
    df.to_csv(output_file, index=False)
    SparsityFiller(output_file)

    return {dataset_id: output_file}
    
      
def slugize(value):
    new_value = value.lower().replace(' (2019=100)', '').replace('£', 'pounds-').replace(' - ', '-').replace(' ', '-')
    return new_value

def monthLabels(value):
    # reduces months to 3 characters
    if len(value) == 4:
        return value[:3]
    else:
        return value
    
def quarters(value):
    lookup = {
            'Jan to Mar':'Q1', 
            'Apr to June':'Q2',
            'July to Sept':'Q3', 
            'Oct to Dec':'Q4'
            }
    return lookup[value]
    
def timeCodes(value):
    # returns just the year for yearly codes
    if value.endswith('- '):
        return value[:4]
    else:
        return value
    
def timeCodesNew(value):
    if len(value) == 4:
        return value
    elif len(value) == 19:
        as_datetime = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        new_value = datetime.datetime.strftime(as_datetime, '%Y - %b')
        return new_value
    else:
        year = value[-4:]
        quarter = value[:-4].strip()
        quarter = quarters(quarter)
        return f"{year} - {quarter}"
    
def seasonallyAdjustedFromTableNumber(value):
    # returns seasonally adjusted codes from table number
    lookup = {
            'Table 1a':'seasonal-adjustment', 
            'Table 1b':'non-seasonal-adjustment', 
            'Table 2a':'seasonal-adjustment', 
            'Table 2b':'non-seasonal-adjustment', 
            'Table 3a':'seasonal-adjustment', 
            'Table 3b':'seasonal-adjustment'
            }
    return lookup[value]

def seasonallyAdjustedLabels(value):
    # returns labels which are worded slightly differently
    lookup = {
            'seasonal-adjustment':'Seasonally adjusted',
            'non-seasonal-adjustment':'Non seasonally adjusted'
            }
    return lookup[value]

def seriesTypeFromTableNumber(value):
    # returns construction-series-type labels from table number
    lookup = {
            'Table 1a':'Index numbers (2019=100)', 
            'Table 1b':'Index numbers (2019=100)', 
            'Table 2a':'£million', 
            'Table 2b':'£million', 
            'Table 3a':'Percentage change period on period', 
            'Table 3b':'Percentage change period on period a year earlier'
            }
    return lookup[value]

def constructionClassificationCodes(value):
    # returns codes for construction-classification
    lookup = {
            'New housing public':'1-2-1-1', 
            'New housing private':'1-2-1-2',
            'Total housing':'1-2-1', 
            'Other new work infrastructure':'1-2-3',
            'Other new work excluding infrastructure public':'1-2-2-1-1', 
            'Other new work excluding infrastructure private industrial':'1-2-2-1-2-1',
            'Other new work excluding infrastructure private commercial':'1-2-2-1-2-2',
            'All new work':'1-2', 
            'R&M housing public':'1-1-1-1',
            'R&M housing private':'1-1-1-2',
            'R&M housing total':'1-1-1',
            'Non housing R&M':'1-1-2',
            'All R&M':'1-1',
            'All work':'1'
            }
    return lookup[value]

def constructionClassificationLabels(value):
    # returns labels for construction-classification
    lookup = {
            'New Housing - Public':'New housing - public', 
            'New Housing - Private':'New housing - private',
            'New Housing - Total Housing':'Total Housing', 
            'Other New Work - Infrastruc-ture':'Other new work - infrastructure',
            'Other New Work - Public':'Other new work excluding infrastructure - public', 
            'Other New Work - Private Industrial':'Other new work excluding infrastructure - private industrial',
            'Other New Work - Private Commercial':'Other new work excluding infrastructure - private commericial',
            'Other New Work - All New Work':'All new work', 
            'Repair and Maintenance - Public':'Repair and maintenance - public housing',
            'Repair and Maintenance - Private':'Repair and maintenance - private housing',
            'Repair and Maintenance - Total':'Repair and maintenance - total housing',
            'Repair and Maintenance - Non Housing R&M':'Repair and maintenance - non housing repair and maintenance',
            'Repair and Maintenance - All Repair and Maintenance':'All repair and maintenance',
            'Repair and Maintenance - All Work':'All work'
            }
    return lookup[value]
