from databaker.framework import *
import pandas as pd

def transform(files):
    assert type(files) == list, f"transform takes in a list, not {type(files)}"
    assert len(files) == 1, f"transform only takes in 1 source file, not {len(files)}"
    file = files[0]
    output_file = "/tmp/v4-suicides-in-the-uk.csv"
    
    tabs = loadxlstabs(file, ["Table 1"])

    """DataBaking"""
    conversionsegments = []
    for tab in tabs:
        
        junk = tab.excel_ref("A4").expand(DOWN).filter(contains_string("Footnote")).expand(DOWN).expand(RIGHT)
        time = tab.excel_ref("E6").expand(RIGHT).is_not_blank().is_not_whitespace()
        geog = tab.excel_ref("A8").expand(DOWN).is_not_blank().is_not_whitespace()
        geog -= junk
        geog_labels = tab.excel_ref('B8:D8').expand(DOWN).is_not_blank().is_not_whitespace()
        geog_labels -= junk
        
        obs = geog.waffle(time)
        
        dimensions = [
                HDim(time, TIME, DIRECTLY, ABOVE),
                HDim(geog, GEOG, DIRECTLY, LEFT),
                HDim(geog_labels, 'geog_labels', DIRECTLY, LEFT)
                ]
        
        conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
        conversionsegments.append(conversionsegment)
        
    df = pd.concat(conversionsegments)

    '''Post processing'''
    df["v4_0"] = df["OBS"].apply(v4Integers)
    df["Time"] = df["TIME"]
    df = df[df["GEOG"] != "J99000001"]

    df = df.rename(columns={
            "TIME": "calendar-years",
            "GEOG": "administrative-geography",
            "geog_labels": "Geography"
            }
        )

    df = df[[
            "v4_0", "calendar-years", "Time", "administrative-geography", "Geography"
            ]]

    df.to_csv(output_file, index=False)
    print("Transform Complete")
    return output_file


def v4Integers(value):
    """
    treats all values in v4 column as strings
    returns integers instead of floats for numbers ending in ".0"
    """
    new_value = str(value)
    if new_value.endswith(".0"):
        new_value = new_value[:-2]
    return new_value
        