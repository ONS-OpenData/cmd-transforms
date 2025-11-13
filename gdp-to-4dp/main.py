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
    assert len(files) == 3, f"transform takes in 3 source files, not {len(files)} /n {files}"
    
    dataset_id = "gdp-to-four-decimal-places"
    output_file = f"{location}v4-{dataset_id}.csv"

    all_tabs = []
    for file in files:
        if file.split('/')[-1].lower().startswith('monthly'):
            read_file = loadxlstabs(file, ['Monthly GDP'])
            
        elif file.split('/')[-1].lower().startswith('rft_'):
            read_file = loadxlstabs(file, ['IoP and Sectors to 4dp'])
            
        elif file.split('/')[-1].lower().startswith('rftds'):
            read_file = loadxlstabs(file, ['Sheet1'])

        all_tabs.append(read_file)

    # above process creates a list of lists
    # need to flatten the lists    
    tabs = [item for subitem in all_tabs for item in subitem]
    del all_tabs

    '''DataBaking'''
    conversionsegments = []
    for tab in tabs:
        
        time = tab.excel_ref('A').filter(contains_string('1997JAN')).expand(DOWN).is_not_blank().is_not_whitespace()
        
        if tab.name == 'Monthly GDP':
            assert tab.excel_ref('A4').value == 'Month', f"data has moved in {tab.name}"
            section = tab.excel_ref('A4').fill(RIGHT).is_not_blank().is_not_whitespace()
            section_labels = section
        else:
            assert tab.excel_ref('A7').value == 'Section', f"data has moved in {tab.name}"
            section = tab.excel_ref('A7').fill(RIGHT).is_not_blank().is_not_whitespace()
            section_labels = section.shift(0, -1)
            
        obs = time.waffle(section)

        dimensions = [
                HDim(time, TIME, DIRECTLY, LEFT),
                HDimConst(GEOG, 'K02000001'),
                HDim(section, 'section', DIRECTLY, ABOVE),
                HDim(section_labels, 'section_labels', DIRECTLY, ABOVE)
                ]
        
        conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
        conversionsegments.append(conversionsegment)    

    df = pd.concat(conversionsegments)    

    '''Post Processing'''
    df['TIME'] = df['TIME'].apply(TimeLabels)
    df['Time'] = df['TIME']

    df['Geography'] = 'United Kingdom'

    df['sic-unofficial'] = df['section']
    df['section'] = df['section_labels']

    # fixing new format
    df['sic-unofficial'] = df['sic-unofficial'].apply(SicCodes_New_Format)
    df['section'] = df['section'].apply(SicLabels_New_Format)

    df['section'] = df['section'].apply(SicLabels)
    df['section'] = df['sic-unofficial'].apply(SicCodes1) + ' : ' +  df['section']
    df['sic-unofficial'] = df['sic-unofficial'].apply(SicCodes2)

    df = df.rename(columns = {
            'OBS':'v4_0',
            'TIME':'mmm-yy',
            'GEOG':'uk-only',
            'section':'UnofficialStandardIndustrialClassification'
            }
        )

    df = df[['v4_0', 'mmm-yy', 'Time', 'uk-only', 'Geography', 'sic-unofficial', 'UnofficialStandardIndustrialClassification']]

    df = df.drop_duplicates()
    df.to_csv(output_file, index=False)

    return {dataset_id: output_file}

def TimeLabels(value):
    new_value = value[-3:].title() + '-' + value[2:4]
    return new_value

def SicLabels(value):
    lookup = {'Services':'Index of Services',
              'Production':'Production Industries'}
    return lookup.get(value, value)

def SicCodes1(value):
    # Tidys codes to combine with labels
    if value == 'B+C+D+E':
        return 'B-E'
    new_value = value.replace('&', ' & ')
    return new_value

def SicCodes2(value):
    # Tidys codes to match code list
    lookup = {
            'B+C+D+E':'B--E',
            'G&I':'G-and-I',
            'H&J':'H-and-J',
            }
    return lookup.get(value, value.replace('-', '--'))

def SicCodes_New_Format(value):
    # fixes the sic codes for the different format
    lookup = {'Monthly GDP (A-T)':'A-T',
              'Agriculture (A)':'A',
              'Production (B-E)':'B-E',
              'Construction (F) [note1],[note 2]':'F',
              'Services (G-T)':'G-T'
            }
    return lookup.get(value, value)

def SicLabels_New_Format(value):
    # fixes the sic codes for the different format
    lookup = {'Monthly GDP (A-T)':'Monthly GDP',
              'Agriculture (A)':'Agriculture',
              'Production (B-E)':'Production Industries',
              'Construction (F) [note1],[note 2]':'Construction',
              'Services (G-T)':'Index of Services'
            }
    return lookup.get(value, value)
