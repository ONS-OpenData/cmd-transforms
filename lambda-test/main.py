
import pandas as pd

def transform(files):
    """
    Test function
    """
    assert type(files) == list, f"transform takes in a list, not {type(files)}"
    assert len(files) == 1, f"transform only takes in 1 source file, not {len(files)}"

    output_file = "/tmp/v4-cpih.csv"

    df['Time'] = df['time']

    df['uk-only'] = 'K02000001'
    df['Geography'] = 'United Kingdom'

    df = df.rename(
        columns = {
            'time':'mmm-yy',
            'Code':'cpih1dim1aggid',
            'Code desc':'Aggregate'
            }
    )

    df = df[[
        'v4_0', 'mmm-yy', 'Time', 'uk-only', 'Geography', 'cpih1dim1aggid', 'Aggregate'
        ]]

    df.to_csv(output_file, index=False)

    return output_file

