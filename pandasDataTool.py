# -*- coding: utf-8 -*-
"""
Created on Tue Feb  18 2019
@author: Pedro Ortiz
This is a tool to operate dataframes, sometimes as a set other with usefull functions
"""
import pandas as pd
import numpy as np
import glob
from pandasDataTool import *
#--------------------------------------------------------------------------------------
def importFiles(FileInit, indexTagNAME=None):
    if ".csv" in FileInit:
        if indexTagNAME:
            dfIn = pd.read_csv(FileInit,encoding='utf-8').set_index(indexTagNAME)
        else:
            dfIn = pd.read_csv(FileInit, encoding='utf-8')
    elif ".xlsx" in FileInit:
        if indexTagNAME:
            dfIn = pd.read_excel(FileInit, encoding='utf-8').set_index(indexTagNAME)
        else:
            dfIn = pd.read_excel(FileInit, encoding='utf-8')
    else:
        dfIn = FileInit 
    return dfIn
#--------------------------------------------------------------------------------------
#- Treating the dataframes as a sets, and making a difference set operation
def differenceAminusBDataFrame(Adf, Bdf,  indexTagNAME="NAME", index=False, subset=None):
    Bdf = importFiles(Bdf, indexTagNAME) 
    Adf = importFiles(Adf, indexTagNAME) 
    Adf["SetA"] = "A"
    Adf["SetB"] = np.nan
    Bdf["SetB"] = "B"
    Bdf["SetA"] = np.nan
    print("len A==> ",len(Adf))
    print("len B==> ",len(Bdf))
    if index:
        df = Adf.append(Bdf)
        print("len A+B==> ",len(df))
        df = df.drop_duplicates(subset=subset, keep=False)
        print("len dropduplicates df ==> ", len(df))
        df = df[ df["SetA"]=="A" & (df["SetB"].isna()) ]
        print("len set(A-B)==> ", len(df) )

    else:
        Adf = Adf.reset_index()
        Bdf = Bdf.reset_index()
        df = Adf.append(Bdf)
        print("len A+B==> ",len(df))
        df = df.drop_duplicates(subset=subset, keep=False)
        print("len dropduplicates df ==> ", len(df))
        df = df[(df["SetA"]=="A") & (df["SetB"].isna())]
        print("len set(A-B)==> ",len(df))

    df = df.drop(['SetB', 'SetA'], axis=1)
    #df = df.drop_duplicates(subset=subset)
    return df
#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------
#--> An inner join between two dataframes
def AintersectionB(Adf, Bdf, indexName="NAME", index=False, subset=None, threshNAN=None):
    Bdf = importFiles(Bdf) 
    Adf = importFiles(Adf) 
    print("len A==> ",len(Adf))
    print("len B==> ",len(Bdf))
    Adf["SetA"] = "A"
    Adf["SetB"] = np.nan
    Bdf["SetB"] = "B"
    Bdf["SetA"] = np.nan
    if index:
        df = Adf.merge(Bdf, left_index=True, right_index=True, how="inner")
        df = df.drop_duplicates(subset=subset)
        print("len dropduplicates df ==> ", len(df))
        #df = df.dropna(thresh=threshNAN)
        df = df[ df["SetA"]=="A" & (df["SetB"]== "B") ]
        print("len set(A intersection B)==> ", len(df) )
    else:
        Adf = Adf.reset_index()
        Bdf = Bdf.reset_index()
        df = Adf.append(Bdf)
        print("len A+B==> ",len(df))
        df = df.drop_duplicates(subset=subset, keep=False)
        print("len dropduplicates df ==> ", len(df))
        df = df[ df["SetA"]=="A" & (df["SetB"]== "B") ]
        print("len set(A intersection B)==> ", len(df) )
    print("len final DF==> ",len(df))
    return df
#--------------------------------------------------------------------------------------
def mergingFilesInFolder(pathFolder, filesPattern, indexTagNAME=None):
    pathFiles = r"{}\{}".format(pathFolder, filesPattern)
    files = glob.glob(pathFiles)
    lisFiles = []
    for nameFile in files:
        df = importFiles(nameFile, indexTagNAME )
        lisFiles.append(df)
    print("Number of appended DFs===> ",len(lisFiles))
    dfOut = pd.concat(lisFiles, axis=0, join="outer")
    return dfOut 
#--------------------------------------------------------------------------------------
#--> splitting a string into multiple rows
def splitColumToRows(fileA, splitColumn, pivotColumn, newColumn=None):
    if newColumn:
        newColumn = newColumn
    else:
        newColumn = splitColumn
    dfA = importFiles(fileA)
    dfB = pd.DataFrame([(item, columns[pivotColumn]) for index, columns in dfA.iterrows() for item in columns[splitColumn].split()]).rename(columns={0:newColumn, 1:pivotColumn})
    df = dfB.merge(dfA, on=[pivotColumn], how='inner')
    return df
#-------------------------------------------------------------------------------------
#--> stacking Columns Into One
def stackingColumnsInOne(fileA, listColumns, newColumn):
    dfA = importFiles(fileA)
    listDF = []
    names = set(list(dfA.columns))
    print('colum names--> ',list(names))
    olderNames = set(listColumns)
    leftNames = list(names - olderNames)
    leftNames.append(newColumn)
    print('New names columns --> ',leftNames)
    for item in listColumns:
        df = dfA[dfA[item].notna()].rename(columns={item:newColumn})[leftNames]
        if len(df)>0:
            listDF.append(df)
    df = pd.concat(listDF, axis=0, sort=False)
    return df    