import pandas as pd
from databaker.framework import *
from ashe_functions import *
import math

def transform(files, **kwargs):    
    dataset_id = "ashe-tables-27-and-28"
    output_file = f"{dataset_id}.csv"
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
    
    # iterating the databaking process
    max_length = []  #max number of rows out of all the sheets
    for tab in tabs:
        tab_max = len(tab.excel_ref('A'))
        max_length.append(tab_max)
    max_length = max(max_length)
    batch_number = 20    # iterates over this many rows at a time
    number_of_iterations = math.ceil(max_length / batch_number)   # databaking will iterate this many times
    
    

    def sheetNameCodeLookup(value):
        '''returns hours-and-earnings codes from labels'''
        return sheet_name_dict[value]

    def tableNumberLookup(value):
        lookup = {
                '27':'Workplace',
                '28':'Residence'
                }
        return lookup[value]

    def GeogLabels(value):
        return geog_dict[value]

    
    # pull in codelist for sheetName (ashe-earnings)
    sheet_name_dict = CodeList_Codes_and_Labels('hours-and-earnings')
    sheet_name_dict = Code_To_Labels(sheet_name_dict)
    geog_dict = CodeList_Codes_and_Labels('enterprise-regions')
    
    
    print('Databaking...')
    conversionsegments = []
    
    for i in range(0, number_of_iterations):
    
        Min = str(6 + batch_number * i)  # data starts on row 6
        Max = str(int(Min) + batch_number - 1)
    
        for tab in tabs:
            
            # columns are named badly
            # quick check to make sure they haven't changed
            if tab.excel_ref('C5').value != '(thousand)':
                raise Exception("Column names aren't right")
                
            if tab.excel_ref('S7').value != 'Key':
                raise Exception('Key has moved')
                
            # ignoring the junk
            junk = tab.excel_ref('C').filter(lambda cell: cell.value == '' and cell.y > 40).expand(LEFT).expand(DOWN)
            
            geography_codes = tab.excel_ref(f'B{Min}:B{Max}').is_not_blank().is_not_whitespace()
            
            # ignoring the annual percentage change and number of jobs
            columns_to_ignore = tab.excel_ref('E') | tab.excel_ref('G') | tab.excel_ref('C')
            variable = tab.excel_ref('C5').expand(RIGHT).is_not_blank().is_not_whitespace() - columns_to_ignore
            
            tab_name = tab.name
            
            sheet_name = tab.excel_ref('a1').value.split(' ')[1]
        
            table_number = sheet_name.split('.')[0]
        
            obs = variable.waffle(geography_codes)
            
            dimensions = [
                    HDimConst(TIME, year_of_data),
                    HDim(geography_codes, GEOG, DIRECTLY, LEFT),
                    HDim(variable, 'variable', DIRECTLY, ABOVE),
                    HDimConst('tab_name', tab_name),
                    HDimConst('sheet_name', sheet_name),
                    HDimConst('table_number', table_number)
                    ]
            
            if len(obs) != 0:
                #only use ConversionSegment if there is data
                conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
                conversionsegments.append(conversionsegment)
                
    df = pd.concat(conversionsegments)
    
    '''databaking CV interval data'''
    print('Databaking the CV intervals...')
    
    conversionsegments = []
    
    for i in range(0, number_of_iterations):
    
        Min = str(6 + batch_number * i)  #data starts on row 6
        Max = str(int(Min) + batch_number - 1)
      
        for tab in tabs_cv:
            
            # columns are named badly
            # quick check to make sure they haven't changed
            if tab.excel_ref('C5').value != '(thousand)':
                raise Exception("Column names aren't right")
                
            if tab.excel_ref('S7').value != 'Key':
                raise Exception('Key has moved')
                
            # ignoring the junk
            junk = tab.excel_ref('C').filter(lambda cell: cell.value == '' and cell.y > 40).expand(LEFT).expand(DOWN)
            
            geography_codes = tab.excel_ref(f'B{Min}:B{Max}').is_not_blank().is_not_whitespace()
            
            # ignoring the annual percentage change and number of jobs
            columns_to_ignore = tab.excel_ref('E') | tab.excel_ref('G') | tab.excel_ref('C')
            variable = tab.excel_ref('C5').expand(RIGHT).is_not_blank().is_not_whitespace() - columns_to_ignore
            
            tab_name = tab.name
            
            sheet_name = tab.excel_ref('a1').value.split(' ')[1]
        
            table_number = sheet_name.split('.')[0]
        
            obs = variable.waffle(geography_codes)
            
            dimensions = [
                    HDimConst(TIME, year_of_data),
                    HDim(geography_codes, GEOG, DIRECTLY, LEFT),
                    HDim(variable, 'variable', DIRECTLY, ABOVE),
                    HDimConst('tab_name', tab_name),
                    HDimConst('sheet_name', sheet_name),
                    HDimConst('table_number', table_number)
                    ]
            
            if len(obs) != 0:
                #only use ConversionSegment if there is data
                conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
                conversionsegments.append(conversionsegment)
            
    df_cv = pd.concat(conversionsegments)
    
    # quick check to make sure data and CV data is same length
    if len(df.index) != len(df_cv.index):
        raise Exception('Data and CV interval data lengths don\'t match')
    
    # v4 column for dfCV is the CV intervals for data
    df = df.reset_index(drop=True)
    df_cv = df_cv.reset_index(drop=True)
    df_cv.loc[df_cv['OBS'] == '', 'OBS'] = df_cv['DATAMARKER']
    df['CV'] = df_cv['OBS']
    
    '''Post processing'''
    
    # renaming columns
    rename_columns = {
            'OBS':'v4_2',
            'DATAMARKER': 'Data Marking',
            'TIME':'Time',
            'time_codelist': 'calendar-years',
            'geography_codelist':'enterprise-regions',
            'variable':'AveragesAndPercentiles',
            'variable_codelist':'averages-and-percentiles',
            'sheet_name':'HoursAndEarnings',
            'sheet_name_codelist':'hours-and-earnings',
            'table_number':'WorkplaceOrResidence',
            'table_number_codelist':'workplace-or-residence'
            }
    
    df['time_codelist'] = df['TIME']
    
    df['geography_codelist'] = df['GEOG'].apply(lambda x:x.strip('c').strip('b').strip('a'))
    df['geography'] = df['geography_codelist'].apply(GeogLabels)
    
    df['sheet_name'] = df['sheet_name'].apply(sheetNameLookup)
    df['sheet_name_codelist'] = df['sheet_name'].apply(sheetNameCodeLookup)
    
    df['table_number'] = df['table_number'].apply(tableNumberLookup)
    df['table_number_codelist'] = df['table_number'].apply(lambda x:x.lower())
    
    df['variable'] = df['variable'].apply(variableType)
    df['variable_codelist'] = df['variable'].apply(variableTypeCodeLookup)
    
    df['Sex'] = df['tab_name'].apply(sexLabels)
    df['sex'] = df['Sex'].apply(Lower)
    
    df['WorkingPattern'] = df['tab_name'].apply(workingPatternLabels)
    df['working-pattern'] = df['WorkingPattern'].apply(Lower)
    
    # reordering columns
    df = df[[
             'OBS', 'DATAMARKER', 'CV', 'time_codelist', 'TIME',
             'geography_codelist', 'geography', 'variable_codelist', 'variable',
             'sex', 'Sex', 'working-pattern', 'WorkingPattern', 
             'sheet_name_codelist', 'sheet_name', 'table_number_codelist', 'table_number'
             ]]
    
    df = df.rename(columns=rename_columns)
    
    df.loc[df['Data Marking'] == '.', 'Data Marking'] = 'x'
    df.loc[df['CV'] == '.', 'CV'] = 'x'
    df.loc[df['CV'] == '', 'CV'] = 'x'
    
    #Correcting issue with databaker
    df_error = df[df['v4_2'] == '']
    df_error = df_error[pd.isnull(df_error['Data Marking'])]
    error_list = list(df_error.index)
    df['Data Marking'].loc[error_list] = 'x'
    df['CV'].loc[error_list] = 'x'
    
    df.to_csv(output_file, index=False)
    return {dataset_id: output_file}