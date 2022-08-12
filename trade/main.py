import pandas as pd
from latest_version import get_latest_version
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
    assert len(files) == 2, f"transform takes in 2 source files, not {len(files)} /n {files}"
    
    dataset_id = "trade"
    output_file = f"{location}v4-{dataset_id}.csv"

    imports_file = [file for file in files if 'import' in file.lower()][0]
    exports_file = [file for file in files if 'export' in file.lower()][0]

    print('Reading in files')

    dfI = pd.read_excel(imports_file, '3. Monthly Imports', skiprows=3)   # create imports dataframe
    dfE = pd.read_excel(exports_file, '3. Monthly Exports', skiprows=3)   # create exports dataframe

    assert len(dfI.columns) == len(dfE.columns), 'number of columns does not match for the import files'

    # getting labels from API
    global commodity_dict
    commodity_dict = get_codes_from_codelist("sitc")

    global country_dict
    country_dict = get_codes_from_codelist("countries-and-territories")    

    print('DataBaking..')
    df_list = []

    num = 0
    for source in (dfI, dfE):
        num += 1 # counter for feedback
        
        for date_col in source.columns[3:]:
            
            df_loop = pd.DataFrame()
            
            df_loop['v4_0'] = source[date_col]
            
            df_loop['mmm-yy'] = TimeCorrector(date_col)
            df_loop['Time'] = df_loop['mmm-yy']
            
            df_loop['uk-only'] = 'K02000001'
            df_loop['Geography'] = 'United Kingdom'
            
            df_loop['sitc'] = source['COMMODITY'].apply(lambda x: x.split(' ')[0].replace('/', '-'))
            df_loop['StandardIndustrialTradeClassification'] = df_loop['sitc'].apply(CommodityLabels)
            
            df_loop['countries-and-territories'] = source['COUNTRY'].apply(lambda x: x.split(' ')[0])
            df_loop['CountriesAndTerritories'] = df_loop['countries-and-territories'].apply(CountryLabels)
            
            df_loop['trade-direction'] = source['DIRECTION'].apply(lambda x: x.split(' ')[0])
            df_loop['Direction'] = df_loop['trade-direction'].apply(DirectionLabel)
            
            df_list.append(df_loop)
            
    df = pd.concat(df_list)

    df['v4_0'] = df['v4_0'].apply(NANRemover) #changes any 'nan' to ''
    df['v4_0'] = df['v4_0'].apply(v4Integers) #changes floats to string-integers

    print('Reading in previous version')
    previous_df = get_latest_version('trade', 'time-series')
    previous_df = previous_df[previous_df['Time'].apply(Year_Remover)]

    new_df = pd.concat([previous_df, df])

    new_df['countries-and-territories'] = new_df['countries-and-territories'].apply(CountryCorrector)

    assert len(new_df) == len(new_df.drop_duplicates()), 'duplicate values in v4'
                
    new_df.to_csv(output_file, index=False)
    print('Transform complete!')

    return {dataset_id: output_file}

def v4Integers(value):
    '''
    treats all values in v4 column as strings
    returns integers instead of floats for numbers ending in '.0'
    '''
    newValue = str(value)
    if newValue.endswith('.0'):
        newValue = newValue[:-2]
    return newValue

def TimeCorrector(value):
    '''
    Converts YYYYMMM into mmm-yy
    '''
    # quick check to make sure time is in expected format
    assert len(value) == 7, '{} is not an expected time format'.format(value)
    
    year = value[:4]
    month = value[4:].title()
    
    # check to make sure format is correct
    try:
        int(year)
    except:
        raise ValueError('First for characters of {} should be a year'.format(value))
        
    return month + '-' + year[2:]  

def CommodityLabels(value):
    # returns sitc labels from api
    return commodity_dict[value]
    
def CountryLabels(value):
    # returns countries-and-territories labels from api
    return country_dict[value]

def DirectionLabel(value):
    lookup = {'IM':'Imports', 'EX':'Exports'}
    return lookup[value]

def NANRemover(value):
    # changes a 'nan' to ''
    if pd.isnull(value):
        return ''
    else:
        return value
    
def Year_Remover(value):
    # removes data from 2018 onwards
    year = int(value.split('-')[-1])
    if 18 <= year < 80:
        return False
    else:
        return True
    
def CountryCorrector(value):
    # corrects issue with Namibia code - 'NA'
    if pd.isnull(value):
        return 'NA'
    else:
        return value

