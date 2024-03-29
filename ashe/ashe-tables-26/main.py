import pandas as pd
from databaker.framework import *
from ashe_functions import *

def transform(files, **kwargs): 
    dataset_id = "ashe-tables-26"
    output_file = f"v4-{dataset_id}.csv"
    year_of_data = kwargs['year_of_data']
    
    # ignoring any files that are not ashe files and ignoring gender pay gap file
    files = [file for file in files if '.12' not in file]
    files = [file for file in files if '.docx' not in file]
    files = [file for file in files if 'note' not in file.lower()]    
    
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
    tabs_cv = [tab for tab in flat_list_cv if 'note' not in tab.name.lower()] 
    
    # quick check to make sure number of files or number of tabs hasn't changed
    if len(tabs) != len(tabs_cv) or len(tabs) != len(files) * 9:
        raise Exception('Number of files or number of tabs has changed')
                    
    # pull in codelist for sheetName (ashe-earnings)
    sheetNameDict = CodeList_Codes_and_Labels('hours-and-earnings')
    sheetNameDict = Code_To_Labels(sheetNameDict)
    
    def sheetNameCodeLookup(value):
        '''returns hours-and-earnings codes from labels'''
        return sheetNameDict[value]
    
    '''databaking data'''
    print('Databaking...')
    conversionsegments = []
    
    for tab in tabs:
        # columns are named badly
        # quick check to make sure they haven't changed
        if tab.excel_ref('C5').value != '(thousand)':
            raise Exception("Column names aren't right")
            
        geogCodes = 'K02000001'
        
        description = tab.excel_ref('A6')
        
        # ignoring the annual percentage change and number of jobs
        columnsToIgnore = tab.excel_ref('E') | tab.excel_ref('G') | tab.excel_ref('C')
        variable = tab.excel_ref('C5').expand(RIGHT).is_not_blank().is_not_whitespace() - columnsToIgnore
        
        tabName = tab.name
            
        sheetName = tab.excel_ref('a1').value.split(' ')[1]
        
        obs = variable.waffle(description)
        
        dimensions = [
                HDimConst(TIME, year_of_data),
                HDimConst(GEOG, geogCodes),
                HDim(description, 'care', DIRECTLY, LEFT),
                HDim(variable, 'Variable', DIRECTLY, ABOVE),
                HDimConst('tabName', tabName),
                HDimConst('sheetName', sheetName)
                ]
        
        conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
        conversionsegments.append(conversionsegment)
    
    df = pd.concat(conversionsegments)
    
    conversionsegments = []
    
    for tab in tabs_cv:
        # columns are named badly
        # quick check to make sure they haven't changed
        if tab.excel_ref('C5').value != '(thousand)':
            raise Exception("Column names aren't right")
            
        geogCodes = 'K02000001'
        
        description = tab.excel_ref('A6')
        
        # ignoring the annual percentage change and number of jobs
        columnsToIgnore = tab.excel_ref('E') | tab.excel_ref('G') | tab.excel_ref('C')
        variable = tab.excel_ref('C5').expand(RIGHT).is_not_blank().is_not_whitespace() - columnsToIgnore
        
        tabName = tab.name
            
        sheetName = tab.excel_ref('a1').value.split(' ')[1]
        
        obs = variable.waffle(description)
        
        dimensions = [
                HDimConst(TIME, year_of_data),
                HDimConst(GEOG, geogCodes),
                HDim(description, 'care', DIRECTLY, LEFT),
                HDim(variable, 'Variable', DIRECTLY, ABOVE),
                HDimConst('tabName', tabName),
                HDimConst('sheetName', sheetName)
                ]
        
        conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
        conversionsegments.append(conversionsegment)
        
    dfCV = pd.concat(conversionsegments)
    
    #quick check to make sure data and CV data is same length
    if len(df.index) != len(dfCV.index):
        raise Exception('Data and CV interval data lengths don\'t match')
    
    #V4 column for dfCV is the CV intervals for data
    df = df.reset_index(drop = True)
    dfCV = dfCV.reset_index(drop = True)
    dfCV.loc[dfCV['OBS'] == '', 'OBS'] = dfCV['DATAMARKER']
    df['CV'] = dfCV['OBS']
    
    '''Post Processing'''
    df['Time_codelist'] = df['TIME']
    
    df['Geography'] = 'United Kingdom'
    
    df['sheetName'] = df['sheetName'].apply(sheetNameLookup)
    df['sheetName_codelist'] = df['sheetName'].apply(sheetNameCodeLookup)
    
    df['Variable'] = df['Variable'].apply(variableType)
    df['Variable_codelist'] = df['Variable'].apply(variableTypeCodeLookup)
    
    df['Sex'] = df['tabName'].apply(sexLabels)
    df['sex'] = df['Sex'].apply(Lower)
    
    df['WorkingPattern'] = df['tabName'].apply(workingPatternLabels)
    df['working-pattern'] = df['WorkingPattern'].apply(Lower)
    
    renameCols = {
            'OBS':'v4_2',
            'Time_codelist':'calendar-years',
            'TIME':'Time',
            'GEOG':'uk-only',
            'Variable':'AveragesAndPercentiles',
            'Variable_codelist':'averages-and-percentiles',
            'sheetName':'HoursAndEarnings',
            'sheetName_codelist':'hours-and-earnings',
            'DATAMARKER':'Data Marking'
            }
    
    df = df[[
            'OBS', 'DATAMARKER', 'CV', 'Time_codelist', 'TIME',
            'GEOG', 'Geography', 'Variable_codelist', 'Variable',
            'sex', 'Sex', 'working-pattern', 'WorkingPattern', 
            'sheetName_codelist', 'sheetName'
            ]]
    
    df = df.rename(columns=renameCols)
    
    df.loc[df['Data Marking'] == '.', 'Data Marking'] = 'x'
    df.loc[df['CV'] == '.', 'CV'] = 'x'
    df.loc[df['CV'] == '', 'CV'] = 'x'
    
    #Correcting issue with databaker
    dfError = df[df['v4_2'] == '']
    dfError = dfError[pd.isnull(dfError['Data Marking'])]
    errorList = list(dfError.index)
    df['Data Marking'].loc[errorList] = 'x'
    df['CV'].loc[errorList] = 'x'
    
    df.to_csv(output_file, index=False)
    return {dataset_id: output_file}
