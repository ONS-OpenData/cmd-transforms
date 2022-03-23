
import pandas as pd
import glob

def transform(file):
    """
    Test function
    """

    df = pd.read_csv(file, dtype=str)
    columns = lsit(df.columns)
    print(f"columns from function - {columns}")
    return columns
        

