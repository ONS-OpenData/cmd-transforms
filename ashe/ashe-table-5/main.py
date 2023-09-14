import pandas as pd
from databaker.framework import *
from ashe_functions import *
from code_list import get_codes_from_codelist
from sparsity_functions import SparsityFiller
import math

def transform(files, **kwargs):    
    dataset_id = "ashe-table-5"
    output_file = f"v4-{dataset_id}.csv"
    year_of_data = kwargs['year_of_data']
    
    # ignoring any files that are not ashe files and ignoring gender pay gap file
    files = [file for file in files if '.12' not in file]
    
    # separate data and CV interval data
    files_cv = [file for file in files if file.endswith('CV.xls')]
    files = [file for file in files if not file.endswith('CV.xls')]
    
    # making sure both lists are in the same order
    files = sorted(files)
    files_cv = sorted(files_cv)
                    
    # loading in all tabs for data
    all_tabs = []
    for file in files:
        read_file = loadxlstabs(file)
        all_tabs.append(read_file)
    
    # loading in all tabs for CV interval data
    all_tabs_cv = []
    for file in files_cv:
        read_file = loadxlstabs(file)
        all_tabs_cv.append(read_file)
        
    # above process creates a list of lists
    # need to flatten the lists    
    flat_list = [item for subitem in all_tabs for item in subitem]
    flat_list_cv = [item for subitem in all_tabs_cv for item in subitem]
    
    # removing the info tabs from each spreadsheet
    tabs = [tab for tab in flat_list if tab.name != 'Notes']
    tabs_cv = [tab for tab in flat_list_cv if tab.name != 'CV notes']
    
    # quick check to make sure number of files or number of tabs hasn't changed
    if len(tabs) != len(tabs_cv) or len(tabs) != len(files) * 9:
        raise Exception('Number of files or number of tabs has changed')
    
    # to be removed
    ###############
    tabs = [tab for tab in tabs if tab.name=='All']
    tabs_cv = [tab for tab in tabs_cv if tab.name=='All']
    ###############
    
    max_length = []
    for tab in tabs:
        tab_max = len(tab.excel_ref('A'))
        max_length.append(tab_max)
    max_length = max(max_length)
    batch_number = 10    # iterates over this many rows at a time
    number_of_iterations = math.ceil(max_length/batch_number)   # databaking will iterate this many times
    
    del flat_list, flat_list_cv, read_file, all_tabs, all_tabs_cv # freeing up some memory
    
    cs = []
    for tab in tabs:
        if tab.name == 'Female Part-Time':
            print("Final tab for sheet")

        for i in range(0, number_of_iterations):
            Min = str(6 + batch_number * i)  # data starts on row 6
            Max = str(int(Min) + batch_number - 1)

            # columns are named badly
            # quick check to make sure they haven't changed
            if tab.excel_ref('C5').value != '(thousand)':
                raise Exception("Column names aren't right")
                
            if tab.excel_ref('S7').value != 'Key':
                raise Exception('Key has moved')
                
            junk = tab.excel_ref('A').filter(contains_string('a  Employees')).expand(DOWN)
            
            description = tab.excel_ref(f'A{Min}:A{Max}') - junk 
            description_code = description.shift(1, 0)
            
            # ignoring the annual percentage change and number of jobs
            columns_to_ignore = tab.excel_ref('C') | tab.excel_ref('E') | tab.excel_ref('G')
            variable = tab.excel_ref('C5').expand(RIGHT).is_not_blank().is_not_whitespace() - columns_to_ignore 
            
            tab_name = tab.name
            
            sheet_name = tab.excel_ref('a1').value.split(' ')[2]
        
            obs = description.waffle(variable)
        
            dimensions = [
                    HDimConst(TIME, year_of_data),
                    HDim(description, 'description', DIRECTLY, LEFT),
                    HDim(description_code, 'description_code', DIRECTLY, LEFT),
                    HDim(variable, 'variable', DIRECTLY, ABOVE),
                    HDimConst('tab_name', tab_name),
                    HDimConst('sheet_name', sheet_name)
                    ]
            
            if len(obs) != 0:
               conversionsegment = ConversionSegment(tab,dimensions,obs).topandas()
               cs.append(conversionsegment)

    df = pd.concat(cs)

    '''databaking CV interval data'''
    print('Databaking the CV intervals...')
    
    cs = []
    for tab in tabs_cv:
        if tab.name == 'Female Part-Time':
            print("Final tab for CV sheet")

        for i in range(0, number_of_iterations): 
            Min = str(6 + batch_number * i)  # data starts on row 6
            Max = str(int(Min) + batch_number - 1)

            # columns are named badly
            # quick check to make sure they haven't changed
            if tab.excel_ref('C5').value != '(thousand)':
                raise Exception("Column names aren't right")
                
            if tab.excel_ref('S7').value != 'Key':
                raise Exception('Key has moved')
                
            junk = tab.excel_ref('A').filter(contains_string('a  Employees')).expand(DOWN)
            
            description = tab.excel_ref(f'A{Min}:A{Max}') - junk 
            description_code = description.shift(1, 0)
            
            # ignoring the annual percentage change and number of jobs
            columns_to_ignore = tab.excel_ref('C') | tab.excel_ref('E') | tab.excel_ref('G')
            variable = tab.excel_ref('C5').expand(RIGHT).is_not_blank().is_not_whitespace() - columns_to_ignore 
            
            tab_name = tab.name
            
            sheet_name = tab.excel_ref('a1').value.split(' ')[2]
        
            obs = description.waffle(variable)
        
            dimensions = [
                    HDimConst(TIME, year_of_data),
                    HDim(description, 'description', DIRECTLY, LEFT),
                    HDim(description_code, 'description_code', DIRECTLY, LEFT),
                    HDim(variable, 'variable', DIRECTLY, ABOVE),
                    HDimConst('tab_name', tab_name),
                    HDimConst('sheet_name', sheet_name)
                    ]
            
            if len(obs) != 0:
               conversionsegment = ConversionSegment(tab,dimensions,obs).topandas()   
               cs.append(conversionsegment)

    df_cv = pd.concat(cs)
    
    # quick check to make sure data and CV data is same length
    if len(df.index) != len(df_cv.index):
        raise Exception('Data and CV interval data lengths don\'t match')
    
    # v4 column for df_cv is the CV intervals for data
    df = df.reset_index(drop=True)
    df_cv = df_cv.reset_index(drop=True)
    df_cv.loc[df_cv['OBS'] == '', 'OBS'] = df_cv['DATAMARKER']
    df['CV'] = df_cv['OBS']
    
    del df_cv # freeing up space
    
    ''' Functions '''
    
    sheet_name_dict = get_codes_from_codelist('hours-and-earnings')
    sheet_name_dict = Code_To_Labels(sheet_name_dict)
    
    geog_lookup_dict = get_codes_from_codelist('administrative-geography')
    
    sic_lookup_dict = get_codes_from_codelist('sic-unofficial')
    
    def sheetNameCodeLookup(value):
        '''returns hours-and-earnings codes from labels'''
        return sheet_name_dict[value]
    
    def geogLabelLookup(value):
        return geog_lookup_dict[value]
    
    def sicLabelLookup(value):
        return sic_lookup_dict[value]
    
    def geogNames(value):
        if value.lower().strip() == 'united kingdom':
            return 'K02000001'
        
        if value.lower().strip() == 'great britain':
            return 'K03000001'
        
        if value.lower().strip() == 'england and wales':
            return 'K04000001'
        
        if value.lower().strip() == 'england':
            return 'E92000001'
        
        if value.lower().strip() == 'northern ireland':
            return 'N92000002'
        
        if value.lower().startswith('north east'):
            return 'E12000001'
        
        if value.lower().startswith('north west'):
            return 'E12000002'
        
        if value.lower().startswith('yorkshire and the humber'):
            return 'E12000003'
        
        if value.lower().startswith('east midlands'):
            return 'E12000004'
        
        if value.lower().startswith('west midlands'):
            return 'E12000005'
        
        if value.lower().startswith('east'):
            return 'E12000006'
        
        if value.lower().startswith('london'):
            return 'E12000007'
        
        if value.lower().startswith('south east'):
            return 'E12000008'
        
        if value.lower().startswith('south west'):
            return 'E12000009'
        
        if value.lower().startswith('wales'):
            return 'W92000004'
        
        if value.lower().startswith('scotland'):
            return 'S92000003'
        
        return 'K02000001'            
        
    def sicCodesTidy(value):
        if value in (
                'K02000001', 'K03000001', 'K04000001', 'E92000001', 'E12000001', 'E12000002', 'E12000003',
                'E12000004', 'E12000005', 'E12000006', 'E12000007', 'E12000008', 'E12000009', 'W92000004',
                'S92000003', 'N92000002'
                ):
            return 'total'
        
        elif pd.isnull(value):
            return ''
        
        else:
            try:
                if int(value) < 10:
                    return f"0{str(value)}"
                else:
                    return str(value)
            
            except:
                return value
        
    def slugize(value):
        return value.lower().strip().replace(' ', '-')
    
    def descriptionParser(value):
        try:
            if value.lower().startswith('north-east'):
                return value.split('north-east-')[1]
            
            if value.lower().startswith('north-west'):
                return value.split('north-west-')[1]
            
            if value.lower().startswith('yorkshire-and-the-humber'):
                return value.split('yorkshire-and-the-humber-')[1]
            
            if value.lower().startswith('east-midlands'):
                return value.split('east-midlands-')[1]
            
            if value.lower().startswith('west-midlands'):
                return value.split('west-midlands-')[1]
            
            if value.lower().startswith('east'):
                return value.split('east-')[1]
                
            if value.lower().startswith('london'):
                return value.split('london-')[1]
                
            if value.lower().startswith('south-east'):
                return value.split('south-east-')[1]
            
            if value.lower().startswith('south-west'):
                return value.split('south-west-')[1]
            
            if value.lower().startswith('wales'):
                return value.split('wales-')[1]
                
            if value.lower().startswith('scotland'):
                return value.split('scotland-')[1]
                
            return value
        
        except:
            return value
        
    '''Post processing'''
    
    df['Time_codelist'] = df['TIME']
    
    df['administrative-geography'] = df['description'].apply(geogNames)
    df['Geography'] = df['administrative-geography'].apply(geogLabelLookup)
    
    df['description_code'] = df['description_code'].apply(sicCodesTidy) 
    df['description'] = df['description'].apply(slugize)
    df['description'] = df['description'].apply(descriptionParser)
    df.loc[df['description_code'] == '', 'description_code'] = df['description']
    df['sic_labels'] = df['description_code'].apply(sicLabelLookup)
    
    df['sheet_name'] = df['sheet_name'].apply(sheetNameLookup)
    df['sheet_name_codelist'] = df['sheet_name'].apply(sheetNameCodeLookup)
    
    df['variable'] = df['variable'].apply(variableType)
    df['variable_codelist'] = df['variable'].apply(variableTypeCodeLookup)
    
    df['Sex'] = df['tab_name'].apply(sexLabels)
    df['sex'] = df['Sex'].apply(Lower)
    
    df['WorkingPattern'] = df['tab_name'].apply(workingPatternLabels)
    df['working-pattern'] = df['WorkingPattern'].apply(Lower)


    rename_columns = {
            'OBS':'v4_2',
            'DATAMARKER': 'Data Marking',
            'TIME': 'Time',
            'Time_codelist': 'calendar-years',
            'GEOG': 'administrative-geography',
            'variable': 'AveragesAndPercentiles',
            'variable_codelist': 'averages-and-percentiles',
            'sheet_name': 'HoursAndEarnings',
            'sheet_name_codelist': 'hours-and-earnings',
            'description_code': 'sic-unofficial',
            'sic_labels': 'UnofficialStandardIndustrialClassification'
            }
    
    df = df[['OBS', 'DATAMARKER', 'CV', 'Time_codelist', 'TIME', 
             'administrative-geography', 'Geography', 'variable_codelist', 'variable',
             'description_code', 'sic_labels', 'sheet_name_codelist', 'sheet_name',
             'sex', 'Sex', 'working-pattern', 'WorkingPattern']]
    
    df = df.rename(columns=rename_columns)
    
    df.loc[df['Data Marking'] == '.', 'Data Marking'] = 'x'
    df.loc[df['CV'] == '.', 'CV'] = 'x'
    df.loc[df['CV'] == '', 'CV'] = 'x'
    
    df.to_csv(output_file, index=False)
    SparsityFiller(output_file, 'x')
    return {dataset_id: output_file}
    