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
            "retail-sales-index": "",
            "retail-sales-index-all-businesses": "",
            "retail-sales-index-large-and-small-businesses": ""
            }

    # retail sales
    output_file = retail_sales(source_file, location=location)
    output["retail-sales-index"] = output_file
    
    # retail sales all businesses
    output_file = retail_sales_all_businesses(source_file, location=location)
    output["retail-sales-index-all-businesses"] = output_file
    
    # retail sales large and small businesses
    output_file = retail_sales_large_and_small_businesses(source_file, location=location)
    output["retail-sales-index-large-and-small-businesses"] = output_file

    return output


def retail_sales(source_file, **kwargs):
    # CP & KP tables
    if 'location' in kwargs.keys():
        location = kwargs['location']
        if location == '':
            pass
        elif not location.endswith('/'):
            location += '/'
    else:
        location = ''
        
    dataset_id = "retail-sales-index"
    output_file = f"{location}v4-{dataset_id}.csv"

    print(f"Running transform on {dataset_id}")
        
    wanted_tabs = ['CPSA', 'CPSA1', 'CPSA2', 'CPSA3', 'CPSA4', 'KPSA', 'KPSA1', 'KPSA2', 'KPSA3', 'KPSA4']
    tabs = loadxlstabs(source_file, wanted_tabs)
    
    '''Databaking'''
    conversionsegments = []
    for tab in tabs:
        table_start_point = tab.excel_ref('A').filter(contains_string('Dataset identifier code')).by_index(1)
        unwanted_data = tab.excel_ref('A').filter(contains_string('Revision to index numbers')).expand(DOWN).expand(RIGHT)
        
        time = table_start_point.shift(0, 1).expand(DOWN).is_not_blank().is_not_whitespace()
        time -= unwanted_data
        
        retail = table_start_point.shift(1, -2).expand(RIGHT).is_not_blank().is_not_whitespace()
        
        tab_name = tab.name
        
        obs = time.waffle(retail)
        
        dimensions = [
                HDim(time, TIME, DIRECTLY, LEFT),
                HDimConst(GEOG, 'K03000001'),
                HDim(retail, 'retail', DIRECTLY, ABOVE),
                HDimConst('tab_name', tab_name)
                ]
        
        for cell in dimensions[0].hbagset:
            dimensions[0].AddCellValueOverride(cell, str(cell.value))
            
        conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
        conversionsegments.append(conversionsegment)
            
    df = pd.concat(conversionsegments)
            
    '''Post processing'''
    df['mmm-yy'] = df['TIME'].apply(timeLabels)
    df['Time'] = df['mmm-yy']
    
    df['Geography'] = 'Great Britain'
    
    df['retail'] = df['retail'].apply(sicLabels)
    df['sic-unofficial'] = df['retail'].apply(slugize)
    
    df['Prices'] = df['tab_name'].apply(tabNameToTypeOfPrices)
    df['type-of-prices'] = df['Prices'].apply(slugize)
    
    df['seasonal-adjustment'] = 'seasonal-adjustment'
    df['SeasonalAdjustment'] = 'Seasonally Adjusted'
    
    df.loc[df['OBS'] == '', 'Data Marking'] = '.'
    
    df = df.rename(columns={
            'OBS':'v4_1',
            'GEOG':'countries',
            'retail':'UnofficialStandardIndustrialClassification'
            }
    )
    
    df = df[[
            'v4_1', 'Data Marking', 'mmm-yy', 'Time', 'countries', 'Geography', 
            'sic-unofficial', 'UnofficialStandardIndustrialClassification', 
            'type-of-prices', 'Prices', 'seasonal-adjustment', 'SeasonalAdjustment'
            ]]
    
    df.to_csv(output_file, index=False)
    return output_file
    

def retail_sales_all_businesses(source_file, **kwargs):
    # tables 1 & 2
    if 'location' in kwargs.keys():
        location = kwargs['location']
        if location == '':
            pass
        elif not location.endswith('/'):
            location += '/'
    else:
        location = ''
        
    dataset_id = "retail-sales-index-all-businesses"
    output_file = f"{location}v4-{dataset_id}.csv"

    print(f"Running transform on {dataset_id}")
        
    wanted_tabs = ['Table 1 A', 'Table 1 Q', 'Table 1 M', 'Table 2 A', 'Table 2 Q', 'Table 2 M']
    tabs = loadxlstabs(source_file, wanted_tabs)
    
    '''Databaking'''
    conversionsegments = []
    for tab in tabs:
        table_start_point = tab.excel_ref('A').filter(contains_string('Dataset identifier code')).by_index(1)
        unwanted_data = tab.excel_ref('A').filter(contains_string('Percentage increase on a year earlier')).expand(DOWN).expand(RIGHT)
        #unwanted_data = tab.excel_ref('A').filter(contains_string('Revision to index numbers')).expand(DOWN).expand(RIGHT)
        
        time = table_start_point.shift(0, 1).expand(DOWN).is_not_blank().is_not_whitespace()
        time -= unwanted_data
        
        retail = table_start_point.shift(1, -4).expand(RIGHT).is_not_blank().is_not_whitespace()
        
        tab_name = tab.name
        
        obs = time.waffle(retail)
        
        dimensions = [
                HDim(time, TIME, DIRECTLY, LEFT),
                HDimConst(GEOG, 'K03000001'),
                HDim(retail, 'retail', DIRECTLY, ABOVE),
                HDimConst('tab_name', tab_name)
                ]
        
        for cell in dimensions[0].hbagset:
            dimensions[0].AddCellValueOverride(cell, str(cell.value))
            
        conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
        conversionsegments.append(conversionsegment)
            
    df = pd.concat(conversionsegments)
    
    '''Post processing'''
    df['Time'] = df['TIME'].apply(yearsQuartersMonthsLabels)
    df['years-quarters-months'] = df['Time'].apply(slugize)
    
    df['Geography'] = 'Great Britain'
    
    df['retail'] = df['retail'].apply(sicLabels)
    df['sic-unofficial'] = df['retail'].apply(slugize)
    df['retail'] = df['retail'].apply(sicLabelsSecondTidy)
    df['sic-unofficial'] = df['sic-unofficial'].apply(sicCodesTidy)
    
    df['Prices'] = df['tab_name'].apply(tabNameToTypeOfPrices)
    df['type-of-prices'] = df['Prices'].apply(slugize)
    
    df['seasonal-adjustment'] = 'seasonal-adjustment'
    df['SeasonalAdjustment'] = 'Seasonally Adjusted'
    
    df.loc[df['OBS'] == '', 'Data Marking'] = '.'
    
    df = df.rename(columns={
            'OBS':'v4_1',
            'GEOG':'countries',
            'retail':'UnofficialStandardIndustrialClassification'
            }
        )
    
    df = df[[
            'v4_1', 'Data Marking', 'years-quarters-months', 'Time', 'countries', 'Geography',
            'sic-unofficial', 'UnofficialStandardIndustrialClassification', 
            'type-of-prices', 'Prices', 'seasonal-adjustment', 'SeasonalAdjustment'
            ]]
    
    df.to_csv(output_file, index=False)
    return output_file
    
def retail_sales_large_and_small_businesses(source_file, **kwargs):
    # tables 3 & 4
    if 'location' in kwargs.keys():
        location = kwargs['location']
        if location == '':
            pass
        elif not location.endswith('/'):
            location += '/'
    else:
        location = ''
        
    dataset_id = "retail-sales-index-large-and-small-businesses"
    output_file = f"{location}v4-{dataset_id}.csv"

    print(f"Running transform on {dataset_id}")
        
    wanted_tabs = ['Table 3 A', 'Table 3 Q', 'Table 3 M', 'Table 4 A', 'Table 4 Q', 'Table 4 M']
    tabs = loadxlstabs(source_file, wanted_tabs)
    
    '''Databaking'''
    conversionsegments = []
    for tab in tabs:
        table_start_point = tab.excel_ref('A').filter(contains_string('Dataset identifier code')).by_index(1)
        unwanted_data = tab.excel_ref('A').filter(contains_string('Percentage increase on a year earlier')).expand(DOWN).expand(RIGHT)
        #unwanted_data = tab.excel_ref('A').filter(contains_string('Revision to index numbers')).expand(DOWN).expand(RIGHT)
        
        time = table_start_point.shift(0, 1).expand(DOWN).is_not_blank().is_not_whitespace()
        time -= unwanted_data
        
        retail = table_start_point.shift(1, -2).expand(RIGHT).is_not_blank().is_not_whitespace()
        
        tab_name = tab.name
        
        obs = time.waffle(retail)
        
        dimensions = [
                HDim(time, TIME, DIRECTLY, LEFT),
                HDimConst(GEOG, 'K03000001'),
                HDim(retail, 'retail', DIRECTLY, ABOVE),
                HDimConst('tab_name', tab_name)
                ]
        
        for cell in dimensions[0].hbagset:
            dimensions[0].AddCellValueOverride(cell, str(cell.value))
            
        conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
        conversionsegments.append(conversionsegment)
            
    df = pd.concat(conversionsegments)
    
    '''Post processing'''
    df['Time'] = df['TIME'].apply(yearsQuartersMonthsLabels)
    df['years-quarters-months'] = df['Time'].apply(slugize)
    
    df['Geography'] = 'Great Britain'
    
    df['retail'] = df['retail'].apply(sicLabels)
    df['sic-unofficial'] = df['retail'].apply(slugize)
    df['sic-unofficial'] = df['sic-unofficial'].apply(sicCodesTidy)
    
    df['Prices'] = df['tab_name'].apply(tabNameToTypeOfPrices)
    df['type-of-prices'] = df['Prices'].apply(slugize)
    
    df['seasonal-adjustment'] = 'non-seasonal-adjustment'
    df['SeasonalAdjustment'] = 'Non Seasonally Adjusted'
    
    df.loc[df['OBS'] == '', 'Data Marking'] = '.'
    
    df = df.rename(columns={
            'OBS':'v4_1',
            'GEOG':'countries',
            'retail':'UnofficialStandardIndustrialClassification'
            }
        )
    
    df = df[[
            'v4_1', 'Data Marking', 'years-quarters-months', 'Time', 'countries', 'Geography',
            'sic-unofficial', 'UnofficialStandardIndustrialClassification', 
            'type-of-prices', 'Prices', 'seasonal-adjustment', 'SeasonalAdjustment'
            ]]
    
    df.to_csv(output_file, index=False)
    return output_file

def slugize(value):
    return value.strip().replace(',', '').replace('&', 'and').replace(' - ', '-').replace(' ', '-').lower()

def timeLabels(value):
    # converts time into mmm-yy
    '''
    as_datetime = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    new_value = datetime.datetime.strftime(as_datetime, '%b-%y')
    '''
    new_value = f"{value[-3:]}-{value[2:4]}"
    return new_value

def yearsQuartersMonthsLabels(value):
    # converts time to years-quarters-months codelist labels
    if len(value) == 4: # just a year
        return value
    elif len(value) == 7: # quarter
        year = value[:4]
        quarter = value[-2:]
        return f"{year} - {quarter}"
    elif len(value) == 8: # month
        year = value[:4]
        month = value[-3:]
        return f"{year} - {month}"

def sicLabels(value):
    # tidies up labels for retail sic
    if ']' in value:
        new_value = value.split('[')[0].strip()
        return new_value.capitalize()
    else:
        return value.strip().capitalize()
    
def sicLabelsSecondTidy(value):
    # further tidies up labels for retail sic
    # from differences in tabs
    lookup = {
            'All retailing, including automotive fuel':'All retailing including automotive fuel',
            'All retailing, excluding automotive fuel':'All retailing excluding automotive fuel'
            }
    return lookup.get(value, value)
    
def tabNameToTypeOfPrices(value):
    # tab name to type of prices
    lookup = {
            'CPSA':'Value of retail sales at current prices', 
            'CPSA1':'Current prices - Percentage change on same month a year earlier', 
            'CPSA2':'Current prices - Percentage change 3 months on same period a year earlier', 
            'CPSA3':'Current prices - Percentage change on previous month', 
            'CPSA4':'Current prices - Percentage change 3 months on previous 3 months', 
            'KPSA':'Chained volume of retail sales', 
            'KPSA1':'Chained volume - Percentage change on same month a year earlier',
            'KPSA2':'Chained volume - Percentage change 3 months on same period a year earlier', 
            'KPSA3':'Chained volume - Percentage change on previous month', 
            'KPSA4':'Chained volume - Percentage change 3 months on previous 3 months',
            'Table 1 A':'Chained volume of retail sales', 
            'Table 1 Q':'Chained volume of retail sales', 
            'Table 1 M':'Chained volume of retail sales', 
            'Table 2 A':'Value of retail sales at current prices', 
            'Table 2 Q':'Value of retail sales at current prices', 
            'Table 2 M':'Value of retail sales at current prices',
            'Table 3 A':'Chained volume of retail sales', 
            'Table 3 Q':'Chained volume of retail sales', 
            'Table 3 M':'Chained volume of retail sales', 
            'Table 4 A':'Value of retail sales at current prices', 
            'Table 4 Q':'Value of retail sales at current prices', 
            'Table 4 M':'Value of retail sales at current prices'
            }
    return lookup[value]

def sicCodesTidy(value):
    # making sic codes in large and small businesses dataset unique()
    if not value.endswith('businesses'):
        new_value = value + '-all-businesses'
        return new_value
    else:
        return value