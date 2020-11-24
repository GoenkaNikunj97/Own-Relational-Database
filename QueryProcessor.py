import os
import json

# User_defined Classes
import DataType
import Presentation

class QueryProcessor:

    def __init__(self):
        self.databaseName = ""
        self.databaseDir = ""
    
    def checkIfFilePathExist(self, dirName):
        if os.path.exists(dirName):
            return True
        else:
            return False
        
    def useDb(self, DBName):
        dbDir = "AllDatabase/" + DBName
        if( self.checkIfFilePathExist(dbDir)):
            self.databaseName = DBName
            self.databaseDir = "AllDatabase/" + self.databaseName + "/"
            print(self.databaseName, " Selected")
        else:
            raise Exception("Database " +DBName+" Does Not Exist")
        
    def createDB(self, DBName):
        dbDir = "AllDatabase/" + DBName
        if( self.checkIfFilePathExist(dbDir)):
            raise Exception("Database already Exist")
        else:
            os.makedirs(dbDir)
            print(DBName, " Created")
            self.useDb(DBName)

    def createTable(self, tableName, columnDict, primaryKeyList, foreignKeyList=[]):
        if(self.databaseName == ""):
            raise Exception("Select Database first")
        else:
            tableDir = self.databaseDir + tableName + "/"
            tableMetadatFilePath = tableDir + tableName + "_metadata.json"
            tableDataFilePath = tableDir + tableName + "_data.json"

            if self.checkIfFilePathExist(tableDir):
                raise Exception("table already Exist")
            else:
                os.makedirs(tableDir)

            tabelMatadata = dict()
            tabelMatadata["table_name"] = tableName
            columnList = list()

            for key in columnDict.keys():
                columnInputType = DataType.getPythonBasedDataType(columnDict[key])
                columnList.append({"column_name": key, "type": columnInputType})

            tabelMatadata["columns"] = columnList
            tabelMatadata["primary_key"] = primaryKeyList
            tabelMatadata["foreign_key"] = foreignKeyList
            tabelMatadata = json.dumps(tabelMatadata)

            with open(tableMetadatFilePath, 'w') as f:
                f.truncate()
                f.write(tabelMatadata)

            with open(tableDataFilePath, 'w') as f:
                f.truncate()
                f.write("[]")

            print(tableName + " Table Created")

    def selectQuery(self, tableName, columnListToDisplay=[], condition={}):
        
        tableDir = self.databaseDir + tableName + "/"
        
        if self.checkIfFilePathExist(tableDir):
            tableDataFilePath = tableDir + tableName + "_data.json"
            tableData = ""
            
            with open (tableDataFilePath) as file:
                tableData = json.load(file)
            if(len(tableData) == 0):
                print(tableName + " Table is Empty")
                return
            else:
                if( len(condition) > 0 ):
                    data = list()
                    for row in tableData :
                        colToCheck = condition["columnName"]
                        operator = condition["operator"]
                        valueToCheck = condition["value"]
                        valueType = type(valueToCheck)
                        if( colToCheck in row.keys()):
                            if (operator == "="):
                                if(str(row[colToCheck]) == (valueToCheck)):
                                    data.append(row)
                            elif (operator == ">"):
                                if(str(row[colToCheck]) > (valueToCheck)):
                                    data.append(row)
                            elif (operator == "<"):
                                if(str(row[colToCheck]) < (valueToCheck)):
                                    data.append(row)
                            elif (operator == ">="):
                                if(str(row[colToCheck]) >= (valueToCheck)):
                                    data.append(row)
                                
                            elif (operator == "<="):
                                if(str(row[colToCheck]) <= (valueToCheck)):
                                    data.append(row)
                            elif (operator == "!="):
                                if(str(row[colToCheck]) != (valueToCheck)):
                                    data.append(row)
                    tableHeader = dict()
                    for key in tableData[0].keys():
                        tableHeader[key] = ""
                    data.insert(0 , tableHeader)
                    tableData = data
                    
                if(len(columnListToDisplay) > 0):
                    data = list()
                    for row in tableData :
                        selectedCols = dict()
                        for col in columnListToDisplay:
                            selectedCols[col] = row[col]
                        data.append(selectedCols)
                    tableHeader = dict()
                    for key in data[0].keys():
                        tableHeader[key] = ""
                    data.insert(0 , tableHeader)
                    tableData = data
  
            Presentation.displayJSONListData(tableData)
        else:
            raise Exception(tableName + " Table does not Exist")
            

