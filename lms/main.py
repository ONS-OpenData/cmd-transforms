import pandas as pd
from latest_version import get_latest_version

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

    dataset_id = "labour-market"
    edition = "PWT23"
    output_file = f"{location}v4-{dataset_id}-{edition}.csv"

    rates_file = [file for file in files if "rates" in file][0]
    levels_file = [file for file in files if "levels" in file][0]
    
    df_rates = pd.read_csv(rates_file, dtype=str)
    df_levels = pd.read_csv(levels_file, dtype=str)

    DataMarkingCheck(df_rates)
    DataMarkingCheck(df_levels)
    
    # renaming the columns we are interested in
    rename_cols = {
            'obs':'v4_0',
            'Time dim it id':'Time',
            'dim1itid':'EconomicActivity',
            'dim2itid':'AgeGroups',
            'dim3itid':'Sex',
            'dim4itid':'SeasonalAdjustment'
            }
    
    df_rates = df_rates.rename(columns=rename_cols)
    df_levels = df_levels.rename(columns=rename_cols)
    
    df_rates['mmm-mmm-yyyy'] = df_rates['Time'].apply(Slugize)
    df_rates['uk-only'] = 'K02000001'
    df_rates['Geography'] = 'United Kingdom'
    df_rates['unit-of-measure'] = 'rates'
    df_rates['UnitOfMeasure'] = 'Rates'
    df_rates['economic-activity'] = df_rates['EconomicActivity'].apply(Slugize)
    df_rates['age-groups'] = df_rates['AgeGroups']
    df_rates['seasonal-adjustment'] = df_rates['SeasonalAdjustment'].apply(Slugize)
    df_rates['seasonal-adjustment'] = df_rates['seasonal-adjustment'].apply(SeasonalCodes)
    df_rates['SeasonalAdjustment'] = df_rates['SeasonalAdjustment'].apply(SeasonalValues)
    df_rates['Sex'] = df_rates['Sex'].apply(SexLabel)
    df_rates['sex'] = df_rates['Sex'].apply(Slugize)
    
    df_levels['mmm-mmm-yyyy'] = df_levels['Time'].apply(Slugize)
    df_levels['uk-only'] = 'K02000001'
    df_levels['Geography'] = 'United Kingdom'
    df_levels['unit-of-measure'] = 'levels'
    df_levels['UnitOfMeasure'] = 'Levels'
    df_levels['economic-activity'] = df_levels['EconomicActivity'].apply(Slugize)
    df_levels['age-groups'] = df_levels['AgeGroups']
    df_levels['seasonal-adjustment'] = df_levels['SeasonalAdjustment'].apply(Slugize)
    df_levels['seasonal-adjustment'] = df_levels['seasonal-adjustment'].apply(SeasonalCodes)
    df_levels['SeasonalAdjustment'] = df_levels['SeasonalAdjustment'].apply(SeasonalValues)
    df_levels['Sex'] = df_levels['Sex'].apply(SexLabel)
    df_levels['sex'] = df_levels['Sex'].apply(Slugize)
    
    # getting rid of unwanted columns
    columns_to_keep = [
        'v4_0', 'mmm-mmm-yyyy', 'Time', 'uk-only', 'Geography',
        'unit-of-measure', 'UnitOfMeasure', 'economic-activity','EconomicActivity', 
        'age-groups', 'AgeGroups', 'sex', 'Sex', 'seasonal-adjustment', 'SeasonalAdjustment'
        ]
    
    df_rates = df_rates[columns_to_keep]
    df_levels = df_levels[columns_to_keep]
    
    # combining the two
    df = pd.concat([df_rates, df_levels])

    previous_v4 = get_latest_version(dataset_id, edition)
    if "Data Marking" in previous_v4.columns:
        raise NotImplementedError("Transform not set up to include data markings from previous v4")

    new_df = pd.concat([previous_v4, df]).drop_duplicates()
    new_df.to_csv(output_file, index=False)

    return {dataset_id: output_file}

def SeasonalCodes(value):
    lookup = {
            'non-seasonal-adjusted':'non-seasonal-adjustment',
            'seasonal-adjusted':'seasonal-adjustment'
            }
    return lookup[value]

def SeasonalValues(value):
    if value.startswith('Non'):
        return 'Not Seasonally Adjusted'
    else:
        return 'Seasonally Adjusted'

def SexLabel(value):
    # changes 'people' to 'all adults'
    lookup = {'People':'All adults'}
    return lookup.get(value, value)

def Slugize(value):
    new_value = value.replace(' ', '-').lower()
    return new_value

def DataMarkingCheck(dataframe):
    assert len(dataframe["data marking"].unique()) == 1, "There are data markings in the source data, transform is not set up to include these"
    return
