import pandas as pd
from databaker.framework import *
from ashe_functions import *
import math

def transform(files, **kwargs):    
    dataset_id = "ashe-tables-11-and-12"
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
    maxLength = []
    for tab in tabs:
        tabMax = len(tab.excel_ref('A'))
        maxLength.append(tabMax)
    maxLength = max(maxLength)
    batchNumber = 10    #iterates over this many rows at a time
    numberOfIterations = math.ceil(maxLength/batchNumber)   #databaking will iterate this many times
    
    geogNameDict = CodeList_Codes_and_Labels('travel-to-work-area')

    def GeogLabels(value):
        return geogNameDict[value]
    
    def NullGeogCodes(value):
        value = value.strip()
        lookup = {
                'United Kingdom':'K02000001',
                'Great Britain':'K03000001',
                'England and Wales':'K04000001',
                'England':'E92000001',
                'Wales':'W92000004',
                'Wales / Cymru':'W92000004',
                'Scotland':'S92000003',
                'Northern Ireland':'N92000002'
                }
        return lookup.get(value, value)
    
    #pull in codelist for sheetName (ashe-earnings)
    sheetNameDict = CodeList_Codes_and_Labels('hours-and-earnings')
    sheetNameDict = Code_To_Labels(sheetNameDict)
    
    def sheetNameCodeLookup(value):
        '''returns hours-and-earnings codes from labels'''
        return sheetNameDict[value]
    
    def tableNumberLookup(value):
        lookup = {
                '11':'Workplace',
                '12':'Residence'
                }
        return lookup[value]
    
    '''databaking data'''
    print('Databaking...')
    conversionsegments = []
    
    for i in range(0, numberOfIterations):
    
        Min = str(6 + batchNumber * i)  #data starts on row 6
        Max = str(int(Min) + batchNumber - 1)
    
        for tab in tabs:
            
            #columns are named badly
            #quick check to make sure they haven't changed
            if tab.excel_ref('C5').value != '(thousand)':
                raise Exception("Column names aren't right")
                
            #if tab.excel_ref('S7').value != 'Key':
                #raise Exception('Key has moved')
                
            key = tab.excel_ref('S7').expand(RIGHT).expand(DOWN)    #referenced but not used
            junk = tab.excel_ref('A').filter(contains_string('Not Classified')).expand(DOWN)
            
            geographyNames = tab.excel_ref('A'+Min+':A'+Max).is_not_blank().is_not_whitespace() - junk
            geographyCodes = tab.excel_ref('B'+Min+':B'+Max).is_not_blank().is_not_whitespace()
            
            #ignoring the annual percentage change and number of jobs
            columnsToIgnore = tab.excel_ref('E') | tab.excel_ref('G') | tab.excel_ref('C')
            variable = tab.excel_ref('C5').expand(RIGHT).is_not_blank().is_not_whitespace() - columnsToIgnore
            
            tabName = tab.name
            
            sheetName = tab.excel_ref('a1').value.split(' ')[1]
        
            tableNumber = sheetName.split('.')[0]
        
            obs = variable.waffle(geographyNames)
            
            dimensions = [
                    HDimConst(TIME, year_of_data),
                    HDim(geographyCodes, GEOG, DIRECTLY, LEFT),
                    HDim(geographyNames, 'GeogNames', DIRECTLY, LEFT),
                    HDim(variable, 'Variable', DIRECTLY, ABOVE),
                    HDimConst('tabName', tabName),
                    HDimConst('sheetName', sheetName),
                    HDimConst('tableNumber', tableNumber)
                    ]
            
            if len(obs) != 0:
                #only use ConversionSegment if there is data
                conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
                conversionsegments.append(conversionsegment)        
        
    df = pd.concat(conversionsegments)
    
    '''databaking CV interval data'''
    print('Databaking the CV intervals...')
    
    conversionsegments = []
    
    for i in range(0, numberOfIterations):
    
        Min = str(6 + batchNumber * i)  #data starts on row 6
        Max = str(int(Min) + batchNumber - 1)
      
        for tab in tabs_cv:
            
            #columns are named badly
            #quick check to make sure they haven't changed
            if tab.excel_ref('C5').value != '(thousand)':
                raise Exception("Column names aren't right")
                
            #if tab.excel_ref('S7').value != 'Key':
                #raise Exception('Key has moved')
                
            key = tab.excel_ref('S7').expand(RIGHT).expand(DOWN)    
            junk = tab.excel_ref('A').filter(contains_string('Not Classified')).expand(DOWN)
            
            geographyNames = tab.excel_ref('A'+Min+':A'+Max).is_not_blank().is_not_whitespace() - junk
            geographyCodes = tab.excel_ref('B'+Min+':B'+Max).is_not_blank().is_not_whitespace()
            
            #ignoring the annual percentage change and number of jobs
            columnsToIgnore = tab.excel_ref('E') | tab.excel_ref('G') | tab.excel_ref('C')
            variable = tab.excel_ref('C5').expand(RIGHT).is_not_blank().is_not_whitespace() - columnsToIgnore
            
            tabName = tab.name
            
            sheetName = tab.excel_ref('a1').value.split(' ')[1]
        
            tableNumber = sheetName.split('.')[0]
        
            obs = variable.waffle(geographyNames)
            
            dimensions = [
                    HDimConst(TIME, year_of_data),
                    HDim(geographyCodes, GEOG, DIRECTLY, LEFT),
                    HDim(geographyNames, 'GeogNames', DIRECTLY, LEFT),
                    HDim(variable, 'Variable', DIRECTLY, ABOVE),
                    HDimConst('tabName', tabName),
                    HDimConst('sheetName', sheetName),
                    HDimConst('tableNumber', tableNumber)
                    ]
            
            if len(obs) != 0:
                #only use ConversionSegment if there is data
                conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
                conversionsegments.append(conversionsegment)
            
    dfCV = pd.concat(conversionsegments)
    
    #quick check to make sure data and CV data is same length
    if len(df.index) != len(dfCV.index):
        raise Exception('Data and CV interval data lengths don\'t match')
    
    #V4 column for dfCV is the CV intervals for data
    df = df.reset_index(drop=True)
    df_cv = df_cv.reset_index(drop=True)
    df_cv.loc[df_cv['OBS'] == '', 'OBS'] = df_cv['DATAMARKER']
    df['CV'] = df_cv['OBS']
    
    '''Post processing'''
    
    #renaming columns
    colsRename = {
            'OBS':'V4_2',
            'DATAMARKER': 'Data Marking',
            'TIME':'Time',
            'Time_codelist':'calendar-years',
            'GEOG':'travel-to-work-area',
            'Variable':'AveragesAndPercentiles',
            'Variable_codelist':'averages-and-percentiles',
            'sheetName':'HoursAndEarnings',
            'sheetName_codelist':'hours-and-earnings',
            'tableNumber':'WorkplaceOrResidence',
            'tableNumber_codelist':'workplace-or-residence'
            }
    
    df['Time_codelist'] = df['TIME']
    
    df['Geography'] = df['GEOG'].apply(GeogLabels)
    
    df['sheetName'] = df['sheetName'].apply(sheetNameLookup)
    df['sheetName_codelist'] = df['sheetName'].apply(sheetNameCodeLookup)
    df['sheetName_codelist'] = df['sheetName_codelist'].apply(lambda x:x.replace(' ', '-'))
    
    df['tableNumber'] = df['tableNumber'].apply(tableNumberLookup)
    df['tableNumber_codelist'] = df['tableNumber'].apply(Lower)
    
    df['Variable'] = df['Variable'].apply(variableType)
    df['Variable_codelist'] = df['Variable'].apply(variableTypeCodeLookup)
    
    df['tabName_codelist'] = df['tabName'].apply(lambda x:x.lower().replace('-', '_'))
    
    df['Sex'] = df['tabName'].apply(sexLabels)
    df['sex'] = df['Sex'].apply(Lower)
    
    df['WorkingPattern'] = df['tabName'].apply(workingPatternLabels)
    df['working-pattern'] =df['WorkingPattern'].apply(Lower)
    
    #reordering columns
    df = df[['OBS', 'DATAMARKER', 'CV', 'Time_codelist', 'TIME',
             'GEOG', 'Geography', 'Variable_codelist', 'Variable',
             'sex', 'Sex', 'working-pattern', 'WorkingPattern', 
             'sheetName_codelist', 'sheetName', 'tableNumber_codelist', 'tableNumber']]
    
    df = df.rename(columns=colsRename)
    
    #Countries are in the list twice
    df = df.drop_duplicates()
    
    df.loc[df['Data Marking'] == '.', 'Data Marking'] = 'x'
    df.loc[df['CV'] == '', 'CV'] = 'x'
    df.loc[df['CV'] == '.', 'CV'] = 'x'
    
    #Correcting issue with databaker
    dfError = df[df['V4_2'] == '']
    #dfError = df[pd.isnull(df['V4_2'] )]
    dfError = dfError[pd.isnull(dfError['Data Marking'])]
    errorList = list(dfError.index)
    df['Data Marking'].loc[errorList] = 'x'
    df['CV'].loc[errorList] = 'x'
    
    df.to_csv(output_file, index=False)
    return {dataset_id: output_file}