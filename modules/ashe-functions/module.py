import requests

def CodeList_Codes_and_Labels(codelist_id):
    '''
    Returns a code label dict from the code list API for a given codelist
    works around the size limit within the API
    '''
    
    edition = 'one-off'
    url = 'https://api.beta.ons.gov.uk/v1/code-lists/{}/editions/{}/codes'.format(codelist_id, edition)
    codelist_dict = requests.get(url).json()
    # total number of codes
    total_count = codelist_dict['total_count'] 
    
    codes_label_dict = {}
    
    # if < 1000 codes, no iteration needed
    if total_count <= 1000:
        new_url = url + '?limit=1000'
        whole_codelist_dict = requests.get(new_url).json()
        for item in whole_codelist_dict['items']:
            codes_label_dict.update({item['code']:item['label']})
        
    # otherwise iterations are needed, API only has limit size of 1000
    else:
        number_of_iterations = round(total_count / 1000) + 1
        offset = 0
        for i in range(number_of_iterations):
            new_url = url + '?limit=1000&offset={}'.format(offset)
            whole_codelist_dict = requests.get(new_url).json()
            for item in whole_codelist_dict['items']:
                codes_label_dict.update({item['code']:item['label']})
            offset += 1000
            
    return codes_label_dict   


#############################################
    


def Code_To_Labels(code_to_label_dict):
    '''
    Takes a dict of codes-labels as keys-values and reverses them to labels-codes
    returns the dict
    '''
    label_to_code_dict = {}
    for key in code_to_label_dict.keys():
        label_to_code_dict[code_to_label_dict[key]] = key
    return label_to_code_dict


def Lower(value):
    return value.lower()


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
    
    
def variableType(value):
    '''returns variable labels in a more useable format (string) also matches labels'''
    lookup = {
            '10.0':'10th percentile', 
            '20.0':'20th percentile', 
            '25.0':'25th percentile', 
            '30.0':'30th percentile',
            '40.0':'40th percentile', 
            '60.0':'60th percentile', 
            '70.0':'70th percentile', 
            '75.0':'75th percentile', 
            '80.0':'80th percentile', 
            '90.0':'90th percentile'
            }
    return lookup.get(value, value)

def variableTypeCodeLookup(value):
    '''returns ashe-statistics code from label'''
    try:
        newCode = int(value[:2])
        return str(newCode)
    except:
        return value.lower()


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