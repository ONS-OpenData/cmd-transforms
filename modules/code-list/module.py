import math, requests, sys

def get_codes_from_codelist(code_list):
    '''
    gets all the codes from a code list
    works around the size limit within the API
    '''
    if sys.platform.lower().startswith('win'):
        verify = False
        requests.packages.urllib3.disable_warnings()
    else:
        verify = True

    url = f"https://api.beta.ons.gov.uk/v1/code-lists/{code_list}/editions/one-off/codes"
    codelist_dict = requests.get(url, verify=verify).json()
    # total number of codes
    total_count = codelist_dict['total_count'] 
    
    codes_label_dict = {}
    
    # if < 1000 codes, no iteration needed
    if total_count <= 1000:
        new_url = url + '?limit=1000'
        whole_codelist_dict = requests.get(new_url, verify=verify).json()
        for item in whole_codelist_dict['items']:
            codes_label_dict.update({item['code']:item['label']})
        
    # otherwise iterations are needed, API only has limit size of 1000
    else:
        number_of_iterations = math.ceil(total_count / 1000)
        offset = 0
        for i in range(number_of_iterations):
            new_url = url + '?limit=1000&offset={}'.format(offset)
            whole_codelist_dict = requests.get(new_url, verify=verify).json()
            for item in whole_codelist_dict['items']:
                codes_label_dict.update({item['code']:item['label']})
            offset += 1000
            
    return codes_label_dict