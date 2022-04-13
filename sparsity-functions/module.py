import pandas as pd
import datetime

def SparsityFiller(csv, DataMarker = "."):
    """
    Finds all the missing data (from sparsity) and writes a new file with the complete data
    Complete data means showing data markings
    pass DataMarker = "..." to choose the data marker
    """
    
    currentTime = datetime.datetime.now()
    df = pd.read_csv(csv, dtype = str)
    outputFile = csv
    
    # Make sure csv is a v4 file - first column is V4
    if not df.columns[0].lower().startswith("v"):
        raise Exception("Doesn't look like a V4 file")
    
    # first a quick check to see if dataset is actually sparse
    v4marker = int(df.columns[0][-1])   # only interested in codelist columns
    columnList = list(df.columns)
    columnCodeList = columnList[v4marker + 1::2]    # just codelist id columns
    columnLabelList = columnList[v4marker + 2::2]
    
    unsparseLength = 1  # total length of df if 100% complete
    for col in columnCodeList:
        unsparseLength *= df[col].unique().size
    numberOfObs = df.index.size
    if unsparseLength == numberOfObs:
        print("Dataset looks complete, sparsityFiller not needed")
        return("Dataset looks complete, sparsityFiller not needed")
        
    # list of lists of unique values for each dimension
    uniqueListOfCodesInColumns = UniqueListOfCodesInColumns(df, columnCodeList)
    
    # list of all combinations to fill a dict
    listsToFillDataDict = ListsToFillDataDict(uniqueListOfCodesInColumns)
    
    # dicts to fill in labels
    dictsToSortLabels = DictsToSortLabels(df, columnLabelList, columnCodeList)
    
    # creating a new dataframe
    data = DataDict(columnCodeList, listsToFillDataDict)
    newDF = pd.DataFrame(data, columns=columnList)
    newDF["Data Marking"] = DataMarker
    
    # applying the dicts
    newDF = ApplyingTheDicts(newDF, dictsToSortLabels, columnCodeList, columnLabelList)
    
    # concating the df"s
    concatDF = ReorderingDF(newDF, df, v4marker)
    
    # removing duplicates
    indexToKeep = IndexOfDuplicates(concatDF, columnCodeList)
    concatDF = concatDF.loc[indexToKeep]
    
    # writing file to new/existing file name
    concatDF.to_csv(outputFile, index = False)
    
    print(f"SparsityFiller took {datetime.datetime.now() - currentTime}")


def UniqueListOfCodesInColumns(df, columnCodeList):
    """
    columnCodeList is a list of column code ID names
    returns a list of lists of unique values for each dimension
    """
    #vall unique values from each code column
    allList = []
    for column in columnCodeList:
        allList.append(list(df[column].unique()))
    return allList
    
    
def ListsToFillDataDict(uniqueListOfCodesInColumns):
    """
    returns a list of lists to be used to fill a dict
    each list will be as long as the unsparse version
    arranges it so each line will be a unique combination
    currently quite hacky but works..
    """
    totalSize = 1       #vlength of unsparse version
    for List in uniqueListOfCodesInColumns:
        totalSize *= len(List)
    allList = []
    appendThisManyTimes = totalSize        #vused to create unique combinations
    for List in uniqueListOfCodesInColumns:
        listToAppend = []
        appendThisManyTimes /= len(List)
        appendThisManyTimes = int(appendThisManyTimes)
        listDuplicatesTimes = int(totalSize / appendThisManyTimes / len(List))    #vwill duplicate the listToAppend
        for item in List:
            for i in range(appendThisManyTimes):
                listToAppend.append(item)
        listToAppend *= listDuplicatesTimes
        allList.append(listToAppend)
    return allList


def DataDict(columnCodeList, listsToFillDataDict):
    """
    Creates a dict to fill a dataframe
    """
    data = {}
    length = len(columnCodeList)
    for i in range(length):
        data.update({columnCodeList[i]:listsToFillDataDict[i]})
    return data


def DictsToSortLabels(df, columnLabelList, columnCodeList):
    """
    creates a list of dicts that will be used to fill in the labels 
    (using the existing data from the v4)
    """
    allList = []
    length = len(columnCodeList)
    for i in range(length):
        newDict = dict(zip(df[columnCodeList[i]], df[columnLabelList[i]]))
        allList.append(newDict)
    return allList


def ApplyingTheDicts(df, dictsToSortLabels, columnCodeList, columnLabelList):
    """
    applys the dicts to populate the label columns
    """
    newDF = df.copy()
    length = len(dictsToSortLabels)
    for i in range(length):
        newDF[columnLabelList[i]] = newDF[columnCodeList[i]].apply(lambda x:dictsToSortLabels[i][x])
    return newDF


def ReorderingDF(newDF, df, v4marker):
    """
    checks if Data_Marking column already exists and changes name of V4 column if required
    concatenates the original df with the newDF (df of missing combinations)
    reorders the columns if required
    returns concated df
    """
    originalDF = df.copy()
    if "Data Marking" in originalDF.columns:
        concatDF = pd.concat([originalDF, newDF])
    else:
        originalDF["Data Marking"] = ""
        concatDF = pd.concat([originalDF, newDF])
        concatDF = concatDF.rename(columns={
                f"V4_{str(v4marker)}": f"V4_{str(v4marker + 1)}",
                f"v4_{str(v4marker)}": f"v4_{str(v4marker + 1)}"
                }
    )
    
    # reordering columns in case data markings in wrong place
    if concatDF.columns[1] != "Data Marking":
        newColsOrder = [concatDF.columns[0], "Data Marking"]
        for col in concatDF.columns:
            if col not in newColsOrder:
                newColsOrder.append(col)
        concatDF = concatDF[newColsOrder]  
    concatDF = concatDF.reset_index(drop=True)
    return concatDF  
  

def IndexOfDuplicates(concatDF, columnCodeList):
    """
    returns a list of all duplicate lines of the concated df
    ignoring observations and data markings
    """
    newDF = concatDF.copy()
    newDF = newDF.reset_index(drop=True)
    newDF = newDF[columnCodeList]
    newDF = newDF.drop_duplicates()
    indexToKeep = list(newDF.index)
    return indexToKeep