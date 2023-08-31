import pandas as pd
from databaker.framework import *
from ashe_functions import *
import math

def transform(files, **kwargs):    
    dataset_id = "ashe-tables-7-and-8"
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
    
    # iterating the databaking process
    maxLength = []
    for tab in tabs:
        tabMax = len(tab.excel_ref('A'))
        maxLength.append(tabMax)
    maxLength = max(maxLength)
    batchNumber = 10    #iterates over this many rows at a time
    numberOfIterations = math.ceil(maxLength/batchNumber)    # databaking will iterate this many times

    geogLookupDict = CodeList_Codes_and_Labels('administrative-geography')
    
    def geogLabelLookup(value):
        return geogLookupDict[value]
    
    def NullGeogCodes(value):
        value = value.strip()
        lookup = {
                'United Kingdom':'K02000001',
                'Great Britain':'K03000001',
                'England and Wales':'K04000001',
                'England':'E92000001',
                'North East':'E12000001',
                'North West':'E12000002',
                'Yorkshire and The Humber':'E12000003',
                'East Midlands':'E12000004',
                'West Midlands':'E12000005',
                'East':'E12000006',
                'London':'E12000007',
                'South East':'E12000008',
                'South West':'E12000009',
                'Wales':'W92000004',
                'Wales / Cymru':'W92000004',
                'Scotland':'S92000003',
                'Northern Ireland':'N92000002'
                }
        return lookup.get(value, value)
    
    sheetNameDict = CodeList_Codes_and_Labels('hours-and-earnings')

    def sheetNameLookup(value):
        '''returns ashe-earnings labels from sheetName'''
        value = '.' + value.split('.')[1]
        lookup = {
                '.1a':'Weekly pay - Gross',
                '.2a':'Weekly pay - Excluding overtime',
                '.3a':'Basic pay - Including other pay',
                '.4a':'Overtime pay',
                '.5a':'Hourly pay - Gross',
                '.6a':'Hourly pay - Excluding overtime',
                '.7a':'Annual pay - Gross',
                '.8a':'Annual pay - Incentive',
                '.9a':'Paid hours worked - Total',
                '.10a':'Paid hours worked - Basic',
                '.11a':'Paid hours worked - Overtime'
                }
        return lookup[value]
    
    def sheetNameCodeLookup(value):
        '''returns ashe-earnings codes from labels'''
        return sheetNameDict.get(value, value.lower().replace(' - ', '-').replace(' ', '-'))
    
    def tableNumberLookup(value):
        lookup = {
                '7':'Workplace',
                '8':'Residence'
                }
        return lookup[value]
    
    variableDict = CodeList_Codes_and_Labels('averages-and-percentiles')

    def variableTypeCodeLookup(value):
        '''returns ashe-statistics code from label'''
        return variableDict.get(value, value)
    
    def variableType(value):
        '''returns variable labels in a more useable format (string) also matches labels'''
        lookup = {
                '10.0':'10', 
                '20.0':'20', 
                '25.0':'25', 
                '30.0':'30',
                '40.0':'40', 
                '60.0':'60', 
                '70.0':'70', 
                '75.0':'75', 
                '80.0':'80', 
                '90.0':'90'
                }
        return lookup.get(value, value)
    
    def variableTypeLabels(value):
        lookup = {
                '10':'10th percentile', 
                '20':'20th percentile', 
                '25':'25th percentile', 
                '30':'30th percentile',
                '40':'40th percentile', 
                '60':'60th percentile', 
                '70':'70th percentile', 
                '75':'75th percentile', 
                '80':'80th percentile', 
                '90':'90th percentile'
                }
        return lookup.get(value, value)
    
    #splitting tabName into sex and working pattern
    
    def sexLabels(value):
        '''returns ashe-sex labels from tabName'''
        lookup = {
                'Full-Time':'All', 
                'Part-Time':'All',
                'Male Full-Time':'Male', 
                'Male Part-Time':'Male', 
                'Female Full-Time':'Female',
                'Female Part-Time':'Female'
                }
        return lookup.get(value, value)
    
    def sexCodes(value):
        '''returns ashe-sex codes from labels'''
        return value.lower()
    
    def workingPatternLabels(value):
        '''returns working patterns labels from tabName'''
        lookup = {
                'Male':'All', 
                'Female':'All',
                'Male Full-Time':'Full-Time', 
                'Male Part-Time':'Part-Time', 
                'Female Full-Time':'Full-Time',
                'Female Part-Time':'Part-Time'
                }
        return lookup.get(value, value)
    
    def workingPatternCodes(value):
        '''returns working pattern codes from labels'''
        return value.lower()
    
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
                
            #if tab.excel_ref('S7').value != 'Key':
                #raise Exception('Key has moved - {}'.format(tab.name))
                
            junk = tab.excel_ref('A').filter(contains_string('Not Classified')).shift(DOWN).expand(DOWN)
            
            geographyNames = tab.excel_ref('A' + Min + ':A' + Max).is_not_blank().is_not_whitespace() - junk
            geographyCodes = tab.excel_ref('B' + Min + ':B' + Max).is_not_blank().is_not_whitespace()
            
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
                
            #if tab.excel_ref('S7').value != 'Key':
                #raise Exception('Key has moved - {}'.format(tab.name))
                
            junk = tab.excel_ref('A').filter(contains_string('Not Classified')).shift(DOWN).expand(DOWN)
            
            geographyNames = tab.excel_ref('A' + Min + ':A' + Max).is_not_blank().is_not_whitespace() - junk
            geographyCodes = tab.excel_ref('B' + Min + ':B' + Max).is_not_blank().is_not_whitespace()
            
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

    #renaming columns
    colsRename = {
            'OBS':'v4_2',
            'DATAMARKER':'Data Marking',
            'TIME':'Time',
            'Time_codelist':'calendar-years',
            'GEOG':'administrative-geography',
            'Variable':'AveragesAndPercentiles',
            'Variable_codelist':'averages-and-percentiles',
            'sheetName':'HoursAndEarnings',
            'sheetName_codelist':'hours-and-earnings',
            'tableNumber':'WorkplaceOrResidence',
            'tableNumber_codelist':'workplace-or-residence'
            }
    
    df['Time_codelist'] = df['TIME']
    df['Geography'] = df['GeogNames']
    
    #sorting geography
    df.loc[df['Geography'] == 'Not Classified', 'GEOG'] = 'not-classified'
    df.loc[pd.isnull(df['GEOG']), 'GEOG'] = df['Geography'].apply(NullGeogCodes)
    df['Geography'] = df['GEOG'].apply(geogLabelLookup)
    
    '''applying functions'''
    
    df['sheetName'] = df['sheetName'].apply(sheetNameLookup)
    df['sheetName_codelist'] = df['sheetName'].apply(sheetNameCodeLookup)
    df['sheetName_codelist'] = df['sheetName_codelist'].apply(lambda x: x.replace(' ', '-'))
    
    df['tableNumber'] = df['tableNumber'].apply(tableNumberLookup)
    df['tableNumber_codelist'] = df['tableNumber'].apply(lambda x: x.lower())
    
    df['Variable'] = df['Variable'].apply(variableType)
    df['Variable_codelist'] = df['Variable'].apply(variableTypeCodeLookup)
    df['Variable'] = df['Variable'].apply(variableTypeLabels)
    
    df['tabName_codelist'] = df['tabName'].apply(lambda x: x.lower().replace('-', '_'))
    
    df['Sex'] = df['tabName'].apply(sexLabels)
    df['sex'] = df['Sex'].apply(sexCodes)
    
    df['WorkingPattern'] = df['tabName'].apply(workingPatternLabels)
    df['working-pattern'] = df['WorkingPattern'].apply(workingPatternCodes)
    
    #reordering columns
    df = df[['OBS', 'DATAMARKER', 'CV', 'Time_codelist', 'TIME',
             'GEOG', 'Geography', 'Variable_codelist', 'Variable',
             'sex', 'Sex', 'working-pattern', 'WorkingPattern', 
             'sheetName_codelist', 'sheetName', 'tableNumber_codelist', 'tableNumber']]
    
    df = df.rename(columns = colsRename)
    
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
    return {dataset_id: output_file}