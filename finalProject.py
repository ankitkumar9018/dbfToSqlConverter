# -*- coding: utf-8 -*-
"""
Created on Tue May 26 20:18:22 2020

@author: ankit
"""


#%%
import dbfread
import numpy as np
import datetime

test = dbfread.open('G:/work/code/converter/project/PRO_DRU_Clip.DBF')
rawData = test[0].keys()
#test.pop(0)

#%%
headerList =[]
tableList = []
typesListVariables = []
ntype = True
typesList = []

#%% this is only specific to my code as I was not able to to read memo type file so I was reading it through excel file
import pandas as pd
data = pd.read_excel ('G:/work/code/converter/project/project.XLS') 
df = pd.DataFrame(data, columns= ["Unnamed: 31"])
print (df)
notizList = df.values.tolist()


#%%
for header in rawData:
    headerList.append(header)

firstRowIgnore = True
localIndex = 0    
for values in test:
    valueList = []
    for header in headerList:
        if header == "NOTIZ":
            if notizList[localIndex][0] is np.nan:
                valueList.append("")
            else:
                valueList.append(notizList[localIndex][0])
        else:
            valueList.append(values[header])
    tableList.append(valueList)
    localIndex = localIndex + 1
       
#%%
lengthOfList = len(tableList)
index = 0 
  
while lengthOfList > 0 and ntype == True:
    if index == 0:
        typesListVariables = tableList[index]
    localIndex = 0
    for valueLocal in typesListVariables:
        if valueLocal is None:
                if tableList[index][localIndex] is not None:
                    typesListVariables[localIndex] = tableList[index][localIndex]
                    ntype = False
                else:
                    ntype = True
        localIndex = localIndex + 1
    index = index + 1
    lengthOfList = lengthOfList - 1


    
#%%
import mysql.connector as mysql

db = mysql.connect(
    host = "localhost",
    user = "root",
    passwd = "password",
    database = "dbname"
)

print(db) # it will print a connection object if everything is fine

cursor = db.cursor()



#%% switch case
def typeSwitch(i):
    switcher={
          int:"BIGINT",
          str:"VARCHAR(500)",
datetime.date:"DATE",
        float:"DOUBLE",
             }
    return switcher.get(i,"VARCHAR(500)")


#%%

db = mysql.connect(
    host = "localhost",
    user = "root",
    passwd = "password",
    database = "dbname"
)

cursor = db.cursor()

typesList = [type(k) for k in  typesListVariables]

baseCreationQuery = "CREATE TABLE testProject "
queryTemp = ""

tempIndex = 0
for typeVariable in typesList:
    if tempIndex != 0:
        queryTemp = queryTemp + ", "
    if( headerList[tempIndex] == "NUMMER"):
        queryTemp = queryTemp + headerList[tempIndex] + " " + typeSwitch(typeVariable) + " " + "NOT NULL PRIMARY KEY" 
    elif( headerList[tempIndex] == "NOTIZ"):
        queryTemp = queryTemp + headerList[tempIndex] + " " + "VARCHAR(2000)" 
    else:
        queryTemp = queryTemp + headerList[tempIndex] + " " + typeSwitch(typeVariable)
    tempIndex = tempIndex + 1

baseCreationQuery = baseCreationQuery + " (" + queryTemp + ")"

#%%table creation
cursor.execute("DROP TABLE testProject")
cursor.execute(baseCreationQuery)

#%%
## defining the Query
insertQueryFinal = "INSERT INTO testProject"
insertQueryHeaderName = ""
insertQueryValueName = ""

tempIndex = 0
for header in headerList:
    if tempIndex != 0:
        insertQueryHeaderName = insertQueryHeaderName + ", "
        insertQueryValueName = insertQueryValueName + ", "
    insertQueryHeaderName = insertQueryHeaderName + header
    insertQueryValueName = insertQueryValueName + "%s"
    tempIndex = tempIndex + 1

insertQueryFinal = insertQueryFinal + " (" + insertQueryHeaderName + ") VALUES (" + insertQueryValueName + ")"


## executing the query with values
cursor.executemany(insertQueryFinal, tableList)
db.commit()

print(cursor.rowcount, "records inserted")
