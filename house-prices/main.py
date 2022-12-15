
import pandas as pd
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
    
    dataset_id = "house-prices-local-authority"
    output_file = f"{location}v4-{dataset_id}.csv"
  
    df = pd.read_csv(file, dtype=str)
    
    df = df.rename(columns={
        'v4_1':'V4_1', 
        'data_marking':'Data Marking', 
        'year_end':'calendar-years', 
        'year_end_lab':'Time',
        'Local authority code':'administrative-geography', 
        'Local authority name':'Geography', 
        'property_type':'property-type',
        'property_type_lab':'PropertyType', 
        'old_new':'build-status', 
        'old_new_lab':'BuildStatus', 
        'stat':'house-sales-and-prices', 
        'stat_lab':'HouseSalesAndPrices'
        }
    )

    df['Data Marking'] = df['Data Marking'].apply(DataMarking)
    df['calendar-years'] = df['Time'].apply(lambda x: x[-4:])
    df['mmm'] = df['Time'].apply(lambda x: x[:3].lower())
    df['Time'] = df['calendar-years']
    df = df[df['mmm'] != '---']
    df['Month'] = df['mmm'].apply(TimeLabels)
    df['PropertyType'] = df['PropertyType'].apply(PropertyTypeLabels)
    df['property-type'] = df['PropertyType'].apply(lambda x: x.replace('/', '-').lower())
    df['build-status'] = df['build-status'].apply(lambda x: x.replace(' ', '-').lower())
    df['house-sales-and-prices'] = df['HouseSalesAndPrices'].apply(HousePriceCodes)
    df['HouseSalesAndPrices'] = df['house-sales-and-prices'].apply(HousePriceLabels)
    df = df[[
            'V4_1', 'Data Marking', 'calendar-years', 'Time', 'mmm', 'Month', 
            'administrative-geography', 'Geography', 'property-type', 'PropertyType', 
            'build-status', 'BuildStatus', 'house-sales-and-prices', 'HouseSalesAndPrices'
            ]]
  
    df = df[df['Time'].apply(YearRemover)]
    
    df.to_csv(output_file, index=False)
    SparsityFiller(output_file)

    return {dataset_id: output_file}
    
      
def DataMarking(value):
    if value == ':':
        return '.'

def TimeCodes(value):
    if len(value) != 6:
        newValue = value[:4] + value[-2:]
        return newValue
    else:
        return value
    
def TimeLabels(value):
    lookup = {
            'jan':'January', 'feb':'February', 'mar':'March',
            'apr':'April', 'may':'May', 'jun':'June',
            'jul':'July', 'aug':'August', 'sep':'September',
            'oct':'October', 'nov':'November', 'dec':'December'
            }
    return lookup[value]
    
def PropertyTypeLabels(value):
    if value == 'Flats/Maisonettes':
        newValue = 'Flat/maisonette'
    else:
        newValue = value[0].upper() + value[1:].lower()
    return newValue

def HousePriceCodes(value):
    lookup = {
            'Tenth Percentile':'tenth-percentile', 
            'Number of Sales':'sales', 
            'Median':'median', 
            'Mean':'mean',
            'Lower Quartile':'lower-quartile'
            }
    return lookup.get(value, value)

def HousePriceLabels(value):
    lookup = {
            'tenth-percentile':'Tenth percentile price', 
            'sales':'Count of sales', 
            'median':'Median price', 
            'mean':'Mean price',
            'lower-quartile':'Lower quartile price'
            }
    return lookup[value]

def YearRemover(value):
    # data has been editioned
    # current data is 2015 onwards
    if int(value) < 2015:
        return False
    else:
        return True