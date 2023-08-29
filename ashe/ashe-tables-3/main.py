import pandas as pd
from databaker.framework import *
from ashe_functions import *
from sparsity_functions import SparsityFiller
import math

def transform(files, **kwargs):    
    dataset_id = "ashe-tables-3"
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
    numberOfIterations = math.ceil(maxLength/batchNumber)   # databaking will iterate this many times

    #pull in codelist for sheetName (ashe-earnings)
    sheetNameDict = CodeList_Codes_and_Labels('hours-and-earnings')
    sheetNameDict = Code_To_Labels(sheetNameDict)
    
    def sheetNameCodeLookup(value):
        '''returns hours-and-earnings codes from labels'''
        return sheetNameDict[value]
    
    #pull in codelist for 'variable' (ashe-statistics)
    variableDict = CodeList_Codes_and_Labels('averages-and-percentiles')
    variableDict = Code_To_Labels(variableDict)
    
    def SOCcodes(value):
        if value[0].isalpha():
            return 'all'
        else:
            return value
        
    def SOClabels(value):
        geogList = [
                'United Kingdom', 'North East', 'North West',
                'Yorkshire and The Humber', 'East Midlands', 'West Midlands',
                'London', 'South East', 'South West', 'Wales', 'Scotland',
                'Northern Ireland', 'East'
                ]
        for geog in geogList:
            if geog + ',' in value:
                newValue = value.replace(geog + ',', '')
                return newValue
            elif geog in value:
                return 'All' 
        return value
    
    '''DataBaking'''
      
    print('DataBaking...')
    conversionsegments = []
    
    for i in range(0, numberOfIterations):
    
        Min = str(6 + batchNumber * i)  #data starts on row 6
        Max = str(int(Min) + batchNumber - 1) 
    
        for tab in tabs:
            
            #columns are named badly
            #quick check to make sure they haven't changed
            if tab.excel_ref('C5').value != '(thousand)':
                raise Exception("Column names aren't right - {}".format(tab.name))
                
            if tab.excel_ref('S7').value != 'Key':
                raise Exception('Key has moved - {}'.format(tab.name))
            
            #ignoring "Not Classified" row
            junk = tab.excel_ref('A').filter(contains_string('Not Classified')).expand(DOWN)
            
            geographyNames = tab.excel_ref('A').is_bold() - junk
            geographyCodes = geographyNames.shift(1, 0)
            
            socNames = tab.excel_ref('A' + Min + ':A' + Max).is_not_blank().is_not_whitespace() - junk
            socCodes = socNames.shift(1, 0)
            
            #ignoring the annual percentage change and number of jobs
            columnsToIgnore = tab.excel_ref('E') | tab.excel_ref('G') | tab.excel_ref('C')
            variable = tab.excel_ref('C5').expand(RIGHT).is_not_blank().is_not_whitespace() - columnsToIgnore
            
            tabName = tab.name
            
            sheetName = tab.excel_ref('a1').value.split(' ')[1]
        
            obs = variable.waffle(socNames)
            
            dimensions = [
                    HDimConst(TIME, year_of_data),
                    HDim(geographyCodes, GEOG, CLOSEST, ABOVE),
                    HDim(geographyNames, 'GeogNames', CLOSEST, ABOVE),
                    HDim(socCodes, 'socCodes', DIRECTLY, LEFT),
                    HDim(socNames, 'socNames', DIRECTLY, LEFT),
                    HDim(variable, 'Variable', DIRECTLY, ABOVE),
                    HDimConst('tabName', tabName),
                    HDimConst('sheetName', sheetName)
                    ]
            
            if len(obs) != 0:
                #only use ConversionSegment if there is data
                conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
                conversionsegments.append(conversionsegment)
                
    df = pd.concat(conversionsegments)    
    
    '''databaking CV interval data'''
    #same process as above
    print('Databaking the CV intervals...')
    
    conversionsegments = []
      
    for i in range(0, numberOfIterations):
    
        Min = str(6 + batchNumber * i)
        Max = str(int(Min) + batchNumber - 1) 
    
        for tab in tabs_cv:
            
            #columns are named badly
            #quick check to make sure they haven't changed
            if tab.excel_ref('C5').value != '(thousand)':
                raise Exception("Column names aren't right - {}".format(tab.name))
                
            if tab.excel_ref('S7').value != 'Key':
                raise Exception('Key has moved - {}'.format(tab.name))
            
            #ignoring "Not Classified" row
            junk = tab.excel_ref('A').filter(contains_string('Not Classified')).expand(DOWN)
            
            geographyNames = tab.excel_ref('A').is_bold() - junk
            geographyCodes = geographyNames.shift(1, 0)
            
            socNames = tab.excel_ref('A' + Min + ':A' + Max).is_not_blank().is_not_whitespace() - junk
            socCodes = socNames.shift(1, 0)
            
            #ignoring the annual percentage change and number of jobs
            columnsToIgnore = tab.excel_ref('E') | tab.excel_ref('G') | tab.excel_ref('C')
            variable = tab.excel_ref('C5').expand(RIGHT).is_not_blank().is_not_whitespace() - columnsToIgnore
            
            tabName = tab.name
            
            sheetName = tab.excel_ref('a1').value.split(' ')[1]
        
            obs = variable.waffle(socNames)
            
            dimensions = [
                    HDimConst(TIME, year_of_data),
                    HDim(geographyCodes, GEOG, CLOSEST, ABOVE),
                    HDim(geographyNames, 'GeogNames', CLOSEST, ABOVE),
                    HDim(socCodes, 'socCodes', DIRECTLY, LEFT),
                    HDim(socNames, 'socNames', DIRECTLY, LEFT),
                    HDim(variable, 'Variable', DIRECTLY, ABOVE),
                    HDimConst('tabName', tabName),
                    HDimConst('sheetName', sheetName)
                    ]
            
            if len(obs) != 0:
                #only use ConversionSegment if there is data
                conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
                conversionsegments.append(conversionsegment)
                
    dfCV = pd.concat(conversionsegments)    
    
    #quick check to make sure data and CV data is same length
    if len(df.index) != len(dfCV.index):
        raise Exception('Data and CV interval data lengths don\'t match')
        
    # v4 column for dfCV is the CV intervals for data
    df = df.reset_index(drop=True)
    dfCV = dfCV.reset_index(drop=True)
    dfCV.loc[dfCV['OBS'] == '', 'OBS'] = dfCV['DATAMARKER']
    df['CV'] = dfCV['OBS']
    
    '''Post processing'''
    
    #renaming columns
    renameCols = {
            'OBS':'v4_2',
            'DATAMARKER': 'Data Marking',
            'TIME': 'Time',
            'Time_codelist':'calendar-years',
            'Geography':'Geography',
            'GEOG':'administrative-geography',
            'Variable':'AveragesAndPercentiles',
            'Variable_codelist':'averages-and-percentiles',
            'sheetName':'HoursAndEarnings',
            'sheetName_codelist':'hours-and-earnings',
            'socCodes_codelist':'soc',
            'socCodes':'StandardOccupationalClassification'
            }
    
    df['Time_codelist'] = df['TIME']

    df['Geography'] = df['GeogNames']
    df['GEOG'] = df['GEOG'].apply(lambda x: x.strip())
    
    df['socCodes_codelist'] = df['socCodes']
    df['socCodes'] = df['socNames']
    
    df['socCodes_codelist'] = df['socCodes_codelist'].apply(SOCcodes)
    df['socCodes'] = df['socCodes'].apply(SOClabels)
    df['socCodes'] = df['socCodes'].apply(lambda x: x.strip())
    
    df['sheetName'] = df['sheetName'].apply(sheetNameLookup)
    df['sheetName_codelist'] = df['sheetName'].apply(sheetNameCodeLookup)
    
    df['Variable'] = df['Variable'].apply(variableType)
    df['Variable_codelist'] = df['Variable'].apply(variableTypeCodeLookup)
    
    df['tabName_codelist'] = df['tabName'].apply(lambda x: x.lower().replace('-', '_'))
    
    df['Sex'] = df['tabName'].apply(sexLabels)
    df['sex'] = df['Sex'].apply(Lower)
    
    df['WorkingPattern'] = df['tabName'].apply(workingPatternLabels)
    df['working-pattern'] = df['WorkingPattern'].apply(Lower)
    
    df = df[[
            'OBS', 'DATAMARKER', 'CV', 'Time_codelist', 'TIME', 'GEOG',
            'Geography', 'socCodes_codelist', 'socCodes', 'Variable_codelist', 'Variable', 
            'sheetName_codelist', 'sheetName', 'sex', 'Sex', 'working-pattern', 'WorkingPattern'
            ]]
    
    df = df.rename(columns=renameCols)
    
    df.loc[df['Data Marking'] == '.', 'Data Marking'] = 'x'
    df.loc[df['CV'] == '.', 'CV'] = 'x'
    df.loc[df['CV'] == '', 'CV'] = 'x'
    
    #Correcting issue with databaker
    dfError = df[df['v4_2'] == '']
    #dfError = df[pd.isnull(df['V4_2'] )]
    dfError = dfError[pd.isnull(dfError['Data Marking'])]
    errorList = list(dfError.index)
    df['Data Marking'].loc[errorList] = 'x'
    df['CV'].loc[errorList] = 'x'
    
    df.to_csv(output_file, index=False)
    SparsityFiller(output_file, 'x')
    return {dataset_id: output_file}