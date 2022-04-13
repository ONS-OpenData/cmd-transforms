import pandas as pd
#from databakerUtils.sparsityFunctions import SparsityFiller

def Transform(files):
    assert type(files) == list, f"transform takes in a list, not {type(files)}"
    assert len(files) == 1, f"transform only takes in 1 source file, not {len(files)}"
    file = files[0]
    output_file = "/tmp/v4-index-private-housing-rental-prices.csv"

    source = pd.read_csv(file, dtype=str)

    df_list = []
    for col in ["12m growth", "Index value"]:
        df_loop = pd.DataFrame()
        df_loop["v4_1"] = source[col]
        df_loop["Data Marking"] = ""
        df_loop["mmm-yy"] = source["Date"].apply(time_values)
        df_loop["Time"] = df_loop["mmm-yy"]
        df_loop["administrative-geography"] = source["RegionCode"].apply(geography_codes)
        df_loop["Geography"] = source["Geography"]
        df_loop["index-and-year-change"] = index_and_year_change(col)
        df_loop["IndexAndYearChange"] = df_loop["index-and-year-change"].apply(index_and_year_change)
        df_list.append(df_loop)
    df = pd.concat(df_list)

    # Corrects the data markings
    df.loc[df["v4_1"] == "-", "Data Marking"] = "."
    df.loc[df["v4_1"] == "-", "v4_1"] = ""

    df.to_csv(output_file, index=False)
    #SparsityFiller(output_file)
    print("Transform Complete")
    return output_file

def time_values(value):
    # changes format from mmmyyyy to mmm-yy
    assert len(value) == 7, "Date from source data has a different format to MMMYYYY"
    new_value = f"{value[:3]}-{value[5:]}"
    return new_value

def geography_codes(value):
    # N.I. code is incorrect
    lookup = {"N92000001": "N92000002"}
    return lookup.get(value, value)

def index_and_year_change(value):
    # codes and labels for index and year change dimension
    lookup = {
            "12m growth": "year-on-year-change",
            "Index value": "index",
            "index": "Index",
            "year-on-year-change": "Year-on-year change"
            }
    return lookup[value]
