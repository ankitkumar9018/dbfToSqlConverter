# -*- coding: utf-8 -*-
"""
Created on Tue May 26 20:18:22 2020

@author: ankit

duplicates primary 1, 3770
"""


#%%
#import dbfread
#import numpy as np
#import datetime
##for record in dbfread.open('D:/converter/PRO_DRU_Clip.DBF'):
##    print(record)
#    
##test = dbfread.open('D:/converter/SUCHPRO.DBF')
#test = dbfread.open('G:/work/code/converter/project/SUCHPRO.DBF')
#rawData = test[0].keys()
#headerList =[]
#notizList = []
#notizListComplete = []
#
#for header in rawData:
#    headerList.append(header)
#
#notiz = headerList[31]
#
#for record in test:
#    notizListComplete.append(record[notiz])
#    if record[notiz] is not None:
#        notizList.append(record[notiz])

    
#dbt =  dbfread.memo.open_memofile('D:/converter/SUCHPRO.DBT',1)
#dbtFile =  dbfread.memo.open_memofile('D:/converter/SUCHPRO.DBT',1)
        
        
#%%
#i = 1
#newList = []
#indexList = []
#ind = 0
#for record in test:
#    if record['NUMMER'] is not None:
#        if record['NUMMER'] != i:
#            newList.append(record['NUMMER'])
#            indexList.append(ind)
#            i = record['NUMMER']
#        i = i + 1
#    ind = ind + 1


#%%
#import csv
#import sys
#table = dbfread.DBF('D:/converter/SUCHPRO.DBF')
##writer = csv.writer(sys.stdout)
##
##writer.writerow(table.field_names)
##for record in table:
##    writer.writerow(list(record.values()))
#    
#
#f = open('D:/converter/test.csv', 'w')
#
#with f:
#
#    writer = csv.writer(f)
#    
#    for row in table:
#        writer.writerow(list(record.values()))



#%%  making a sql table from dbf
        
import dbfread
import numpy as np
import datetime
#for record in dbfread.open('D:/converter/PRO_DRU_Clip.DBF'):
#    print(record)
    
#test = dbfread.open('D:/converter/SUCHPRO.DBF')
test = dbfread.open('G:/work/code/converter/project/SUCHPRO.DBF')
rawData = test[0].keys()
test.pop(0)

#%%
headerList =[]
tableList = []
typesListVariables = []
ntype = True
typesList = []

#%%

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

## connecting to the database using 'connect()' method
## it takes 3 required parameters 'host', 'user', 'passwd'
db = mysql.connect(
    host = "localhost",
    user = "root",
    passwd = "ankitRaj21!",
    database = "ute"
)

print(db) # it will print a connection object if everything is fine

cursor = db.cursor()



#%% switch case


#def week(i):
#    switcher={
#            0:'Sunday',
#            1:'Monday',
#            2:'Tuesday',
#            3:'Wednesday',
#            4:'Thursday',
#            5:'Friday',
#            6:'Saturday'
#             }
#    return switcher.get(i,"Invalid day of week")

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
    passwd = "ankitRaj21!",
    database = "ute"
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

## creating a table called 'users' in the 'datacamp' database
#cursor.execute("CREATE TABLE testProject (name VARCHAR(255), user_name VARCHAR(255))")

#statement = """INSERT INTO menu(name, price) VALUES(%s, %s)""" %(fish, price)

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

#query = "INSERT INTO users (name, user_name) VALUES (%s, %s)"
## storing values in a variable
#values = [
#    ("Peter", "peter"),
#    ("Amy", "amy"),
#    ("Michael", "michael"),
#    ("Hennah", "hennah")
#]

#tableList.pop(0)

## executing the query with values
cursor.executemany(insertQueryFinal, tableList)

## to make final output we have to run the 'commit()' method of the database object
db.commit()

print(cursor.rowcount, "records inserted")