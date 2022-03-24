
import pandas as pd

def transform(file):
    """
    Test function
    """

    df = pd.read_csv(file, dtype=str)
    print(f"columns from function - {df.columns}")

    df = df[[
        'v4_1', 'Data Marking', 'calendar-years', 'Time', 'uk-only',
        'Geography', 'adzuna-jobs-category', 'AdzunaJobsCategory',
        'week-number', 'Week'
        ]]
    print(f"columns from function after transform - {df.columns}")
    
    return list(df.columns)
        

