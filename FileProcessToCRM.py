import numpy as np
import pandas as pd
from nltk.tokenize import word_tokenize
from nltk import sent_tokenize
import re
from pandasDataTool import *
#----------------------------------------------------------------------
#--> files which have the information about somethings
filePrefijos = r"prefijos.xlsx"
fileCities = r"uscitiesv.csv"
utilityFile = r'utility_zip.xlsx'
#----------------------------------------------------------------------
def importFiles(FileInit, indexTagNAME=None):
    if ".csv" in FileInit:
        if indexTagNAME:
            dfIn = pd.read_csv(FileInit).set_index(indexTagNAME)
        else:
            dfIn = pd.read_csv(FileInit)
    elif ".xlsx" in FileInit:
        if indexTagNAME:
            dfIn = pd.read_excel(FileInit).set_index(indexTagNAME)
        else:
            dfIn = pd.read_excel(FileInit)
    else:         
        if indexTagNAME:
            dfIn = FileInit
            dfIn = dfIn.set_index(indexTagNAME)
        else:
            dfIn = FileInit
    return dfIn
#----------------------------------------------------------------------
#--> Making all the columns and index as UPPER CASE words
def indexUPPER(dfIn):
    dfIn = importFiles(dfIn)
    dfIn.index = dfIn.index.map(str).str.upper()
    return dfIn

def columnUPPER(dfIn, columnsList):
    dfIn = importFiles(dfIn)
    for column in columnsList:
        dfIn[column] = dfIn[column].map(str).str.upper()
    return dfIn
#---------------------------------------------------------------------
def camelCaseSplit(identifier):
    matches = re.finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
    #matches = re.finditer(r'.+?(?:(?<=[a-z])(?=[A-Z])|(?=[A-Z])(?=[A-Z][a-z])|(?<=[A-Z][A-Z])(?=[a-z])|(?<=[a-z])(?=[0-9])|(?<=[0-9])(?=[A-Z])|$)', identifier)
    return " ".join([m.group(0) for m in matches]).split()
#---------------------------------------------------------------------
#--> spliting the words for camecase and numbers-words
def numWordsSplit(txt):
    lista = []
    for word in word_tokenize(txt):
        words = camelCaseSplit(word)
        for item in words:
            splitWord = re.split(r"(\d+)",item)
            if splitWord:
                lista.append(" ".join(splitWord))
    return " ".join(lista)
#---------------------------------------------------------------------
def columnsStr(dfIn, listColumns):
    for column in listColumns:
        dfIn[column] = dfIn[column].map(str)
    return dfIn
#---------------------------------------------------------------------
#--> Splitin the address into STREET, CITY, STATE, ZIP
def splitAddress(dfIn,duplicateItem,  Ads = "STREET"):
    dfIn = importFiles(dfIn)
    dfIn['STREET'] = ""
    dfPre = importFiles(filePrefijos)
    dfPre["PRE"] = dfPre["PRE"].str.lower()
    dfCities = importFiles(fileCities, indexTagNAME=None)
    dfCities["city"] = dfCities["city"].str.lower()
    for index, column in dfIn.iterrows():
        split1 = str(column[Ads]).split(",")
        if len(split1) == 1:
            dfIn.at[index, "STREET"] = np.nan
            dfIn.at[index, "CITY"] = split1[0]
            dfIn.at[index, "STATE"] = "CA"
            dfIn.at[index, "ZIP"] = np.nan
            dfIn.at[index, "PHONE"] = int("".join(re.findall(r"\d", str(column[duplicateItem]))))
        elif len(split1) == 2:
            street = re.findall(r"\S+", numWordsSplit(split1[0]))
            if len(street) > 2:
                if dfCities["city"].str.contains( " ".join(street[-1:]) ).any():
                    streetName = " ".join(re.findall(r"\S+", numWordsSplit(split1[0]))[:-1])
                    city = " ".join(re.findall(r"\S+", numWordsSplit(split1[0]))[-1:])
                elif  dfCities["city"].str.contains( " ".join(street[-2:]) ).any():
                    streetName = " ".join(re.findall(r"\S+", numWordsSplit(split1[0]))[:-2])
                    city = " ".join(re.findall(r"\S+", numWordsSplit(split1[0]))[-2:])
                elif  dfPre["PRE"].str.contains(street[-2].lower()).any():
                    streetName = " ".join(re.findall(r"\S+", numWordsSplit(split1[0]))[:-1])
                    city = " ".join(re.findall(r"\S+", numWordsSplit(split1[0]))[-1:])
                elif len(street[-2]) < 2:
                    streetName = " ".join(re.findall(r"\S+", numWordsSplit(split1[0]))[:-1])
                    city = " ".join(re.findall(r"\S+", numWordsSplit(split1[0]))[-1:])
                elif re.findall(r"\d", street[-2]):
                    streetName = " ".join(re.findall(r"\S+", numWordsSplit(split1[0]))[:-1])
                    city = " ".join(re.findall(r"\S+", numWordsSplit(split1[0]))[-1:])
                elif len(street) == 3:
                    streetName = " ".join(re.findall(r"\S+", numWordsSplit(split1[0]))[:-1])
                    city = " ".join(re.findall(r"\S+", numWordsSplit(split1[0]))[-1:])
                else:
                    streetName = " ".join(re.findall(r"\S+", numWordsSplit(split1[0]))[:-2])
                    city = " ".join(re.findall(r"\S+", numWordsSplit(split1[0]))[-2:])
            else:
                streetName = " "
                city = " ".join(re.findall(r"\S+", numWordsSplit(split1[0])))           
            zipCode = re.findall(r"\S+", split1[1])[1]
            state = re.findall(r"\S+", split1[1])[0]
            dfIn.at[index, "STREET"] = str(streetName)
            dfIn.at[index, "CITY"] = city
            dfIn.at[index, "STATE"] = "California"
            dfIn.at[index, "ZIP"] = int(zipCode)
            dfIn.at[index, "PHONE"] = int("".join(re.findall(r"\d", str(column[duplicateItem]))))
        elif len(split1) == 3:
            streetName = split1[0]
            city = split1[1]                           
            zipCode = re.findall(r"\S+", split1[2])[1]
            state = re.findall(r"\S+", split1[2])[0]
            dfIn.at[index, "STREET"] = streetName
            dfIn.at[index, "CITY"] = city
            dfIn.at[index, "STATE"] = state
            dfIn.at[index, "ZIP"] = int(zipCode)
            dfIn.at[index, "PHONE"] = int("".join(re.findall(r"\d", str(column[duplicateItem]))))
    return dfIn
#---------------------------------------------------------------------
#--> Filter the new ones from the oldest items
def filtereingNewItemsFromOldest(dfIn, dfOldest, duplicateItem, indexTagNAME=None):
    dfIn = importFiles(dfIn)
    dfIn["STATE"] = "California"
    dfOldest = importFiles(dfOldest, indexTagNAME)
    dfMerge = differenceAminusBDataFrame(dfIn, dfOldest,  indexTagNAME=None, index=False, subset=duplicateItem)
    print("merge_filtering--> ",len(dfMerge))
    dfMerge = dfMerge[ dfMerge["STATE"] == "California" ]
    print("STATE--> ", len(dfMerge))
    dfOut = dfMerge[dfMerge["ZIP"].notna()]
    print("ZIP--> ", len(dfMerge))
    dfMergeLeft = dfMerge[dfMerge["ZIP"].isna()]
    return dfOut, dfMergeLeft
#---------------------------------------------------------------------
#--> A function to know is the address is for a landamark
def utilityLADWPconstrains(dfIn, dfUtility):
    dfUtility = importFiles(dfUtility, indexTagNAME=None)
    #dfUtility = dfUtility.drop_duplicates(subset="ZIP")
    dfUtility["ZIP"] = dfUtility["ZIP"].map(float)
    dfIn["NAME"] = dfIn.index
    dfIn["ZIP"] = dfIn["ZIP"].map(float)
    dfIn = dfIn.merge(dfUtility, on=["ZIP"], how='inner')
    print("merging LADWP, IID y MID--> ", len(dfIn) )
    dfIn = dfIn[~(dfIn["Utility"] == "LADWP")]
    dfIn = dfIn[~(dfIn["Utility"] == "IID") ]
    dfIn = dfIn[~(dfIn["Utility"] == "MID")].set_index("NAME")
    print("atfer merging LADWP, IID y MID----> ",len(dfIn))
    #dfIn = dfIn[dfIn["ZIP"].notna()]
    #dfIn["ZIP"] = dfIn["ZIP"].map(int)
    return dfIn
#--------------------------------------------------------------------
#--> Cities with CHARACTETISTIC
def charCities(dfIn):
    dfCities = pd.read_excel(r'PACE Eligibility 112118.xlsx').set_index('California Cities')[['A', 'B', 'C', 'D', 'E']]
    for PACE in ['A', 'B', 'C', 'D', 'E']:
        for index, columns in dfCities.iterrows():
            if dfCities[PACE].str.contains(index).any():
                dfCities.at[index, PACE] =  PACE
            else:
                dfCities.at[index, PACE] = np.nan
    dfCities = dfCities[dfCities['A'].notna() | dfCities['B'].notna() | dfCities['C'].notna() | dfCities['D'].notna()  | dfCities['E'].notna()]
    dfCities= dfCities.reset_index().rename(columns={'California Cities':'CITY'})
    df = dfIn.merge(dfCities, on=["CITY"], how='inner')
    dfLeft = differenceAminusBDataFrame(dfIn.merge(dfCities, on=["CITY"], how='outer'), df, indexTagNAME='name')
    return df, dfLeft    
#---------------------------------------------------------------------
#--> is a land mark?
def isLandMark(dfIn, FileLandMark):
    dfIn = importFiles(dfIn)
    dfIn["ads"] = dfIn["STREET"] + " " + dfIn["CITY"]
    dfLandMark = importFiles(FileLandMark, indexTagNAME=None)
    dfLandMark["ads"] = dfLandMark["ADDRESS"] + " " + dfLandMark["CITY"]
    dfOut = differenceAminusBDataFrame(dfIn, dfLandMark, subset="ads", index=False)
    return dfOut
#---------------------------------------------------------------------
#--> final format to the file
def finalFileFromYellowPages(dfIn, listColumnsT= ["STREET", "CITY", "STATE", "ZIP", "PHONE"], businessType="NON_PROFIT", orgType=0, Index=True):
    ot = ["RELIGIOUS", "NON-RELIGIOUS", "OLD-PEOPLE"]
    organizationType = ot[orgType]
    DESCRIPTION = ""
    STATUS = ""
    EIN = ""
    SUBSECTION = ""
    NTEE = ""
    YEARLY_INCOME = ""
    LEAD_SOURCE = "gpc_rd_yellow_pages"
    dfIn = importFiles(dfIn, indexTagNAME='name')
    dfIn = dfIn[listColumnsT]
    dfIn["ORGANIZATION_TYPE"] = organizationType
    dfIn["BUSINESS_TYPE"] = businessType
    dfIn["DESCRIPTION"] = DESCRIPTION
    dfIn["STATUS"] = STATUS
    dfIn["EIN"] = EIN
    dfIn["SUBSECTION"] = SUBSECTION
    dfIn["NTEE"] = NTEE
    dfIn["YEARLY_INCOME"] = YEARLY_INCOME
    dfIn["LEAD_SOURCE"] = LEAD_SOURCE
    if Index:        
        dfIn.index.name = "ORGANIZATION_NAME"
    else:
        dfIn = dfIn.set_index("name")
        dfIn.index.name = "ORGANIZATION_NAME"

    return dfIn
#---------------------------------------------------------------------
#---> All Process
def processedFileToCRM(FileInit, FileUtilities, fileNameExit, fileOldestItems, date, duplicateItem="PHONE", indexTagNAME="NAME", listColumns=["CITY", "STATE", "ZIP"], uppercolumns = ["STREET", "CITY", "STATE"], Ads = "STREET", orgType=0):
    """
    --> FileInit, it's the variable to save the path from the initial valuable
    --> FileUtilities, it's the variable to save the path from the utility file whice are indexing by Zip code
    --> fileNameExit, it's the variable to save just the name of the variable without extension
    --> fileOldest, the oldest file to compare if the new ones are uniques
    --> date, the date of the file
    --> duplicateItem, a variable to drop duplicate within certain column (subset)
    --> indexTagNAME, a variable wich is the name of the index in the DF
    --> listColumns, list of the new columns 
    --> uppercolumns, the columns name to make UPPER case
    --> Ads, a variable to point in wich column are the address of the items from the initial file
    --> businessType, a variable to said what kind of business the items are
    """
    #--> importing files
    df = importFiles(FileInit, indexTagNAME=indexTagNAME)
    #--> importing making the index UPPER case
    df = indexUPPER(df)
    #--> making strings all the nan values for the empty series
    df = columnsStr(df, listColumns)
    #--> spliting the Address into street, state and zip
    df = splitAddress(df, duplicateItem, Ads)
    print("splitAddress--> ",len(df))
    #--> filtering the new one from the oldest data
    df, dfLeft = filtereingNewItemsFromOldest(df, fileOldestItems, duplicateItem)
    print("filtering--> ",len(df))
    #--> making uppper case to the strings into the caloumns
    df = columnUPPER(df, uppercolumns)
    print("columnUpper--> ",len(df))
    #-->dropping all the duplicated phones
    df = df.drop_duplicates(subset=duplicateItem)
    print("After duplicated phones--> ",len(df))
    #--> Looking for nonprofits with LADWP
    df = utilityLADWPconstrains(df, FileUtilities)
    print("utilityLADWPconstrains--> ",len(df))
    #--> Characteristics cities
    df, dfLeftCities = charCities(df)
    print('charCities--> ', len(df))
    #--> Is there a land mark?
# =============================================================================
#     FileLandMark = r"historic_landmark_addresses_ca.xlsx"
#     dfLandMark = isLandMark(df, FileLandMark)
#     print("Difference Landmarks ==> ", len(dfLandMark) - len(df))
# =============================================================================
    #--> final format to the file from yellow pages scraping
    df = finalFileFromYellowPages(df, businessType="NON_PROFIT", orgType=orgType, Index=True)
    dfLandMark = ""#finalFileFromYellowPages(dfLandMark, Index=True)
    df = df.drop_duplicates(subset="PHONE")
    print("final file--> ",len(df))
    #--> writing the file
    df.to_csv("{}{}.csv".format(fileNameExit, date))
    dfLeft.to_csv("{}_left{}.csv".format(fileNameExit, date))
    return df, dfLeftCities
#---------------------------------------------------------------------
#--> Contains APT?
def containsAPT(df):
    df = importFiles(df)
    dfAp = df[~df["ADDRESS"].map(str).str.contains("Apt") & ~df["ADDRESS"].map(str).str.contains("Apt.") & ~df["ADDRESS"].map(str).str.contains("APT")]
    return dfAp
#---------------------------------------------------------------------
#--> Create new columns
def columnsNew(df, listColumns):
    for column in listColumns:
        df[column] = ""
    return df
#---------------------------------------------------------------------
#---> All Process toVIEJITOS
def processedFileToCRMViejitos(FileInit, FileUtilities, fileNameExit, fileOldestItems, date, duplicateItem="PHONE", indexTagNAME="NAME", listColumns=["CITY", "STATE", "ZIP"], uppercolumns = ["STREET", "CITY", "STATE"], Ads = "STREET", businessType="OLD"):
    """ 
    --> FileInit, it's the variable to save the path from the initial valuable
    --> FileUtilities, it's the variable to save the path from the utility file whice are indexing by Zip code
    --> fileNameExit, it's the variable to save just the name of the variable without extension
    --> fileOldest
    --> date, the date of the file
    --> duplicateItem, a variable to drop duplicate within certain column (subset)
    --> indexTagNAME, a variable wich is the name of the index in the DF
    --> listColumns, list of the new columns 
    --> uppercolumns, the columns name to make UPPER case
    --> Ads, a variable to point in wich column are the address of the items from the initial file
    --> businessType, a variable to said what kind of business the items are
    """
    #--> importing files
    df = importFiles(FileInit, indexTagNAME=indexTagNAME)
    #--> contains APT?
    df = containsAPT(df)
    #--> importing making the index UPPER case
    df = indexUPPER(df)
    #--> making strings all the nan values for the empty series
    columnNames = input("Does your initial DF has a names for the column CITY, STATE, etc?===> ")
    if columnNames == "y":
        df = columnsStr(df, listColumns)
    else:
        df = columnsNew(df, listColumns)
    #--> spliting the Address into street, state and zip
    # Ads has to be equal to the column name of the address from the file
    # it means that if you initial file has Direccion
    # Ads = "Direccion"
    df = splitAddress(df, Ads)
    print("splitAddress--> ",len(df))
    #--> filtering the new one from the oldest
    #--> making uppper case to the strings into the caloumns
    df = columnUPPER(df, uppercolumns)
    print("columnUpper--> ",len(df))
    #-->dropping all the duplicated phones
    df = df.drop_duplicates(subset=duplicateItem)
    print("After duplicated phones--> ",len(df))
    #--> Looking for nonprofits with LADWP
    df = utilityLADWPconstrains(df, FileUtilities)
    print("utilityLADWPconstrains--> ",len(df))
    #--> final format to the file from yellow pages scraping
    df = finalFileFromYellowPages(df, businessType=businessType, Index=True, orgType=2)
    print("final file--> ",len(df))
    #--> writing the file
    df.to_csv("{}{}.csv".format(fileNameExit, date))
    return df
#---------------------------------------------------------------------------------
#--> processing the data from the Whitepages
def fileFromWhitePages(FileInit):
    df = importFiles(FileInit)
    #--> Just take the items with phones
    df = df[df["PHONE"].notna()] 
    #--> Not allowed items iunto addresses
    listNotDefine = ["NO INFO", "not allowed", "Not allow", "Not allowed", "not allow"]
    for item in listNotDefine:
        df = df[~df["PHONE"].str.contains(item)]
    #--> just take tha items without APT within the Address
    df = containsAPT(df)
    return df