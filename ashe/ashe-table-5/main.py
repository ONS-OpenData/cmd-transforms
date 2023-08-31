import pandas as pd
from databaker.framework import *
from ashe_functions import *
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
    
    maxLength = []
    for tab in tabs:
        tabMax = len(tab.excel_ref('A'))
        maxLength.append(tabMax)
    maxLength = max(maxLength)
    batchNumber = 10    #iterates over this many rows at a time
    numberOfIterations = math.ceil(maxLength/batchNumber)   #databaking will iterate this many times

    regions = ['NORTH EAST','NORTH WEST','EAST MIDLANDS','WEST MIDLANDS','YORKSHIRE AND THE HUMBER','EAST','LONDON','SOUTH EAST','SOUTH WEST','ENGLAND','WALES','SCOTLAND','NORTHERN IRELAND','ENGLAND AND WALES','GREAT BRITAIN','UNITED KINGDOM']
    region_codes = ['K02000001','K03000001','K04000001','E92000001','E12000001','E12000002','E12000003','E12000004','E12000005','E12000006','E12000007','E12000008','E12000009','W92000004','S92000003','N92000002']
    
    conversionsegments = []
    for i in range(0,numberOfIterations):    
    
        Min = str(6+batchNumber*i)
        Max = str(39+batchNumber*i)
        
        for tab in tabs:
            
            #columns are named badly
            #quick check to make sure they haven't changed
            if tab.excel_ref('C5').value != '(thousand)':
                raise Exception("Column names aren't right")
                
            if tab.excel_ref('S7').value != 'Key':
                raise Exception('Key has moved')
                
            key = tab.excel_ref('S7').expand(RIGHT).expand(DOWN)    #referenced but not used (waffle)
            junk = tab.excel_ref('A').filter(contains_string('a  Employees')).expand(DOWN)
            
            geographyNames = tab.excel_ref('A'+Min+':A'+Max) - junk 
            geographyCodes = tab.excel_ref('B'+Min+':B'+Max)
            
            #ignoring the annual percentage change and number of jobs
            columnsToIgnore = tab.excel_ref('E') | tab.excel_ref('G') | tab.excel_ref('C')
            variable = tab.excel_ref('C5').expand(RIGHT).is_not_blank().is_not_whitespace() - columnsToIgnore 
            
            tabName = tab.name
            
            sheetName = tab.excel_ref('a1').value.split(' ')[2]
        
            tableNumber = sheetName.split('.')[0]
        
            obs = tab.excel_ref('D'+Min+':D'+Max).expand(RIGHT) - junk - columnsToIgnore - key  #waffle used incase gaps in data
        
            dimensions = [
                    HDimConst(TIME,year_of_data),
                    HDim(geographyCodes,GEOG,DIRECTLY,LEFT),
                    HDim(geographyNames,'GeogNames',DIRECTLY,LEFT),
                    HDim(variable,'Variable',DIRECTLY,ABOVE),
                    HDimConst('tabName',tabName),
                    HDimConst('sheetName',sheetName),
                    HDimConst('tableNumber',tableNumber)
                    ]
            
            if len(obs) != 0:
               conversionsegment = ConversionSegment(tab,dimensions,obs).topandas()
                
            conversionsegments.append(conversionsegment)

    data = pd.concat(conversionsegments)
    
    #remove nulls
    data = data[data.GeogNames.notnull()]
    data = data[data.Variable.notnull()]
    
    data = data.reset_index(drop=True)
    data['region'] = ''
    
    data['GeogNames'] = data['GeogNames'].str.strip()
    data['GeogNamesOriginal'] = data['GeogNames']
    data['region'] = data['region'].str.upper()
    data['GeogNames'] = data['GeogNames'].str.replace('All ', ', All ')
    data['GeogNames'] = data['GeogNames'].str.replace('ALL ', ', ALL ')      
    data['GeogNames'] = data['GeogNames'].str.replace(' , , ', ', ')
    data['GeogNames'] = data['GeogNames'].str.replace(', , ', ', ')
    data['GeogNames'] = data['GeogNames'].str.replace(',, ', ', ')
    data['GeogNames'].loc[data['GeogNames'].isnull()] = 'None'
    
    #split region faster
    f = lambda x: x["GeogNames"].split(", ")[0]
    data['region'] = data.apply(f, axis=1)
    
    data['region'] = data['region'].str.strip()
    data['region'] = data['region'].str.upper()
    
    #change regions that weren't back to UK  
    data.loc[~data['region'].isin(regions), 'region'] = 'UNITED KINGDOM'
    
    #remove wrong ONS codes from SIC code
    data.loc[data['GEOG'].isin(region_codes), 'GEOG'] = ''
    
    #set up industry
    data['industry'] = ''
    
    #change GeogNames to upper
    data['GeogNames'] = data['GeogNames'].str.upper()
    
    #get industry from GeogNames by splitting against region
    g = lambda x: x['GeogNames'].replace(x['region'],'')
    data['industry'] = data.apply(g, axis=1)
    
    data['industry'] = data['industry'].str.replace(', ALL','ALL')
    data['industry'] = data['industry'].str.replace(',  ','ALL')
      
    #remove commas by getting first few rows
    data['industry'] = data['industry'].str.strip()
    data['first'] = data['industry'].astype(str).str[0]
    
    #remove first comma if applicable
    data['industry']= data.apply(lambda x: x['industry'][2:] if x['first'] == ',' else x['industry'],axis=1)
    
    #convert to proper
    data['industry'] = data['industry'].str.title() 
    data['region'] = data['region'].str.title()
    
    #tidying industries after proper
    data['industry'] = data['industry'].str.replace(' And ',' and ')
    data['industry'] = data['industry'].str.replace(' Of ',' of ')
    data['industry'] = data['industry'].str.replace(' With ',' with ')
    data['industry'] = data['industry'].str.replace(' Via ',' via ')
    data['region'] = data['region'].str.replace(' And ',' and ')
    data['industry'] = data['industry'].str.replace(', All ','ALL')
    
    #change back to all if it's a region
    data['industry2'] = data['industry'].str.upper()
    data['industry2']= data.apply(lambda x: 'All' if x['industry'] == '' else x['industry'],axis=1)
    
    #remove unnecessary cols
    data = data.drop('first', 1)
    data = data.drop('GeogNamesOriginal', 1)
    data = data.drop('GeogNames', 1)
    data = data.drop('tableNumber', 1)
    
    #make df
    df = data.copy()
    
    '''databaking CV interval data'''
    print('Databaking the CV intervals...')
    
    conversionsegments = []
      
    for i in range(0,numberOfIterations):    
    
        Min = str(6+batchNumber*i)
        Max = str(39+batchNumber*i)

        for tab in tabs_cv:
            
            #columns are named badly
            #quick check to make sure they haven't changed
            if tab.excel_ref('C5').value != '(thousand)':
                raise Exception("Column names aren't right")
                
            if tab.excel_ref('S7').value != 'Key':
                raise Exception('Key has moved')
                
            key = tab.excel_ref('S7').expand(RIGHT).expand(DOWN)    #referenced but not used (waffle)
            junk = tab.excel_ref('A').filter(contains_string('a  Employees')).expand(DOWN)
            
            geographyNames = tab.excel_ref('A'+Min+':A'+Max) - junk 
            geographyCodes = tab.excel_ref('B'+Min+':B'+Max)
            
            #ignoring the annual percentage change and number of jobs
            columnsToIgnore = tab.excel_ref('E') | tab.excel_ref('G') | tab.excel_ref('C')
            variable = tab.excel_ref('C5').expand(RIGHT).is_not_blank().is_not_whitespace() - columnsToIgnore
            
            tabName = tab.name
            
            sheetName = tab.excel_ref('a1').value.split(' ')[2]
        
            tableNumber = sheetName.split('.')[0]
        
            obs = tab.excel_ref('D'+Min+':D'+Max).expand(RIGHT) - junk - columnsToIgnore - key  #waffle used incase gaps in data

            dimensions = [
                    HDimConst(TIME,year_of_data),
                    HDim(geographyCodes,GEOG,DIRECTLY,LEFT),
                    HDim(geographyNames,'GeogNames',DIRECTLY,LEFT),
                    HDim(variable,'Variable',DIRECTLY,ABOVE),
                    HDimConst('tabName',tabName),
                    HDimConst('sheetName',sheetName),
                    HDimConst('tableNumber',tableNumber)
                    ]
            
            if len(obs) != 0:
               conversionsegment = ConversionSegment(tab,dimensions,obs).topandas()

            conversionsegments.append(conversionsegment)
           

    dataCV = pd.concat(conversionsegments)
    dataCV = dataCV[dataCV.GeogNames.notnull()]
    dataCV = dataCV[dataCV.Variable.notnull()]
    dataCV = dataCV.reset_index(drop=True)
    dfCV = dataCV.copy()
    
    #quick check to make sure data and CV data is same length
    if len(df.index) != len(dfCV.index):
        raise Exception('Data and CV interval data lengths don\'t match')
    
    #V4 column for dfCV is the CV intervals for data
    df = df.reset_index(drop=True)
    dfCV = dfCV.reset_index(drop=True)
    dfCV.loc[dfCV['OBS'] == '', 'OBS'] = dfCV['DATAMARKER']
    df['CV'] = dfCV['OBS']

    #more tidying
    df['industry_codelist'] = df['GEOG'].copy(deep = True)  
    
    df2 = df.copy(deep = True)
    df = df2.copy(deep = True)
    
    #create codelist if all
    df['industry3'] = df['industry2'].copy().str.lower().str.replace(' ','-')
    df['industry_codelist']= df.apply(lambda x: x['industry3'] if x['industry_codelist'] == '' else x['industry_codelist'],axis=1)
    
    '''add in codelists'''
    
    def geogLabelLookup(value):
        '''returns region codes'''
        lookup = {
                'North East':'E12000001',
                'North West':'E12000002',
                'Yorkshire and The Humber':'E12000003',
                'East Midlands':'E12000004',
                'West Midlands':'E12000005',
                'East':'E12000006',
                'London':'E12000007',
                'South East':'E12000008',
                'South West':'E12000009',
                'England':'E92000001',
                'Wales':'W92000004',
                'Scotland':'S92000003',
                'Northern Ireland':'N92000002',
                'England and Wales':'K04000001',
                'Great Britain':'K03000001',
                'United Kingdom':'K02000001'
                }
        return lookup[value]
    
    #pull in codelist for sheetName (ashe-earnings)
    sheetNameDict = CodeList_Codes_and_Labels('hours-and-earnings')
    sheetNameDict = Code_To_Labels(sheetNameDict)
    
    def sheetNameCodeLookup(value):
        '''returns hours-and-earnings codes from labels'''
        return sheetNameDict[value]
    
    #renaming columns
    colsRename = {
            'OBS':'v4_2',
            'DATAMARKER':'Data Marking',
            'TIME':'Time',
            'Time_codelist':'calendar-years',
            'region':'Geography',
            'region_codelist':'administrative-geography',
            'Variable':'AveragesAndPercentiles',
            'Variable_codelist':'averages-and-percentiles',
            'sheetName':'HoursAndEarnings',
            'sheetName_codelist':'hours-and-earnings',
            'industry2':'StandardIndustrialClassification',
            'industry_codelist':'sic'
            }
    
    df['Time_codelist'] = df['TIME']

    #sorting geography
    df['region_codelist'] = df['region'].apply(geogLabelLookup)
    
    '''applying functions'''
    
    df['sheetName'] = df['sheetName'].apply(sheetNameLookup)
    df['sheetName_codelist'] = df['sheetName'].apply(sheetNameCodeLookup)
    df['sheetName_codelist'] = df['sheetName_codelist'].apply(lambda x: x.replace(' ', '-'))
    
    df['Variable'] = df['Variable'].apply(variableType)
    df['Variable_codelist'] = df['Variable'].apply(variableTypeCodeLookup)
    
    df['tabName_codelist'] = df['tabName'].apply(Lower)
    
    df = df.drop('industry', axis=1)
    
    #change to percentiles
    def percentileChange(value):
        #one of these lookups needs removing
        '''matches percentiles'''
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
                '90':'90th percentile',
                'Median':'Median',
                'Mean':'Mean'
                }
        return lookup.get(value,value)
    
    #change industries
        
    #leading zero first
    def leadingZero(value):
        #one of these lookups needs removing
        '''matches percentiles'''
        lookup = {
                '1':'01',
                '2':'02',
                '3':'03',
                '4':'04',
                '5':'05',
                '6':'06',
                '7':'07',
                '8':'08',
                '9':'09'
                }
        return lookup.get(value,value)
    
    
    def industryChange(value):
        #one of these lookups needs removing
        '''matches labels that need changing back'''
        lookup = {
                'all : All':'Total',
                'all-manufacturing : All Manufacturing':'All Manufacturing',
                'all-index-of-production-industries : All Index of Production Industries':'All Index of Production Industries',
                'all-industries-and-services : All Industries and Services':'All Industries and Services',
                'all-service-industries : All Service Industries':'All Service Industries'
                }
        return lookup.get(value,value)
    
    def industryLabelChange(value):
        #one of these lookups needs removing
        '''changes all'''
        lookup = {
                'all':'total'
                }
        return lookup.get(value,value)
    
    df['WorkingPattern'] = df['tabName'].apply(workingPatternLabels)
    df['working-pattern'] = df['WorkingPattern'].apply(industryChange).str.lower()
    
    df['Sex'] = df['tabName'].apply(sexLabels)
    df['sex'] = df['Sex'].apply(Lower)
    
    df3 = df.copy(deep = True)
    df = df3.copy(deep = True)
    
    
    #add code to label
    df['industry_codelist'] = df['industry_codelist'].apply(leadingZero)
    df['industry2'] = df['industry_codelist'] + ' : ' + df['industry2']
    df['industry2'] = df['industry2'].apply(industryChange)
    df['industry_codelist'] = df['industry_codelist'].apply(industryLabelChange)
    
    
    #reordering columns
    df = df[['OBS', 'DATAMARKER','CV','Time_codelist', 'TIME',
             'region_codelist','region','Variable_codelist','Variable',
             'industry_codelist','industry2', 
             'sheetName_codelist','sheetName','sex','Sex', 'working-pattern', 'WorkingPattern']]
    
    df = df.rename(columns = colsRename)
    df['AveragesAndPercentiles'] = df['AveragesAndPercentiles'].apply(percentileChange)
    
    #data markers for CV's need to be filled in
    df.loc[df['CV'] == '','CV'] = 'x'
    
    #find cases where both data marking and obs are NA
    
    df.loc[df['v4_2'] == '','Data Marking'] = 'x'
    
    df.to_csv(output_file, index=False)
    return {dataset_id: output_file}
    