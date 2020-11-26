import os
import json
import shutil

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

    def getDataFromTable(self, tableName):
        if(self.databaseDir == ""):
            raise Exception ("Select Database First")
        tableDir = self.databaseDir + tableName + "/"
        if self.checkIfFilePathExist(tableDir):
            tableDataFilePath = tableDir + tableName + "_data.json"
            tableData = ""
            with open (tableDataFilePath) as file:
                tableData = json.load(file)
            return (tableData)
        else:
            raise Exception (tableName + " Table does not Exist")

    def getValueType(self, data):
        dataType = ["str", "int", "float"]
        if (data == "str"):
            return "string"
        elif (data == "int"):
            return 1
        elif (data == "float"):
            return 1.1

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

            column = dict()
            for key in columnDict.keys():
                columnInputType = DataType.getPythonBasedDataType(columnDict[key])
                column[key] = columnInputType

            tabelMatadata["columns"] = column
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
        tableData = self.getDataFromTable(tableName)

        if (len(tableData) == 0):
            print(tableName + " Table is Empty")
            return
        else:
            if (len(condition) > 0):
                data = list()
                for row in tableData:
                    colToCheck = condition["columnName"]
                    operator = condition["operator"]
                    valueToCheck = condition["value"]
                    if (colToCheck in row.keys()):
                        if (operator == "="):
                            if (str(row[colToCheck]) == (valueToCheck)):
                                data.append(row)
                        elif (operator == ">"):
                            if (str(row[colToCheck]) > (valueToCheck)):
                                data.append(row)
                        elif (operator == "<"):
                            if (str(row[colToCheck]) < (valueToCheck)):
                                data.append(row)
                        elif (operator == ">="):
                            if (str(row[colToCheck]) >= (valueToCheck)):
                                data.append(row)

                        elif (operator == "<="):
                            if (str(row[colToCheck]) <= (valueToCheck)):
                                data.append(row)
                        elif (operator == "!="):
                            if (str(row[colToCheck]) != (valueToCheck)):
                                data.append(row)
                '''
                tableHeader = dict()
                for key in tableData[0].keys():
                    tableHeader[key] = ""
                data.insert(0, tableHeader)
                '''
                tableData = data

            if ("*" not in (columnListToDisplay)):
                data = list()
                for row in tableData:
                    selectedCols = dict()
                    for col in columnListToDisplay:
                        selectedCols[col] = row[col]
                    data.append(selectedCols)
                '''
                tableHeader = dict()
                for key in data[0].keys():
                    tableHeader[key] = ""
                data.insert(0, tableHeader)
                '''
                tableData = data

        Presentation.displayJSONListData(tableData)

    def deleteQuery(self, tableName, condition = []):
        tableData = self.getDataFromTable(tableName)
        if (len(tableData) == 0):
            print(tableName + " Table is Empty")
            return
        if (len(condition) > 0):
            dataLength = len(tableData)
            data = tableData[:]
            i = 0
            while i < dataLength:
                row = data[i]
                colToCheck = condition["columnName"]
                operator = condition["operator"]
                valueToCheck = condition["value"]
                if (colToCheck in row.keys()):
                    if (operator == "="):
                        if (str(row[colToCheck]) == (valueToCheck)):
                            data.remove(row)
                            i = i -1
                            dataLength = dataLength - 1
                    elif (operator == ">"):
                        if (str(row[colToCheck]) > (valueToCheck)):
                            data.remove(row)
                            i = i - 1
                            dataLength = dataLength - 1
                    elif (operator == "<"):
                        if (str(row[colToCheck]) < (valueToCheck)):
                            data.remove(row)
                            i = i - 1
                            dataLength = dataLength - 1
                    elif (operator == ">="):
                        if (str(row[colToCheck]) >= (valueToCheck)):
                            data.remove(row)
                            i = i - 1
                            dataLength = dataLength - 1
                    elif (operator == "<="):
                        if (str(row[colToCheck]) <= (valueToCheck)):
                            data.remove(row)
                            i = i - 1
                            dataLength = dataLength - 1
                    elif (operator == "!="):
                        if (str(row[colToCheck]) != (valueToCheck)):
                            data.remove(row)
                            i = i - 1
                            dataLength = dataLength - 1
                i = i + 1

            tableData = data
        tableDir = self.databaseDir + tableName + "/"
        tableDataFilePath = tableDir + tableName + "_data.json"
        with open(tableDataFilePath, 'w') as f:
            f.truncate()
            if(len(condition) > 0):
                json.dump(tableData, f)
            else:
                f.write("[]")

    def insertQuery(self, tableName, valueList, colList = []):
        tableData = self.getDataFromTable(tableName)
        tableDir = self.databaseDir + tableName + "/"
        tableDataFilePath = tableDir + tableName + "_metadata.json"
        metaData = ""
        with open(tableDataFilePath) as file:
            metaData = json.load(file)
        metaData = metaData["columns"]
        if( len(colList) == 0):
            if( len(valueList) == len(metaData)):
                i = 0
                dataRow = dict()
                for key in metaData.keys():
                    if(type(valueList[i]) == type(self.getValueType(metaData[key]))):
                        dataRow[key] = valueList[i]
                    else:
                        raise Exception (str(key) + " should be of type " + str(type(self.getValueType(metaData[key]))))
                    i = i + 1

                tableData.append(dataRow)
                print("data added")
            else:
                raise Exception( tableName + " have "+ str(len(metaData)) + " columns but "+str(len(valueList)) +" values was given" )
        else:
            i = 0
            dataRow = dict()
            for col in colList:
                if col in metaData.keys():
                    if (type(valueList[i]) == type(self.getValueType(metaData[col]))):
                        dataRow[col] = valueList[i]
                    else:
                        raise Exception(str(col) + " should be of type " + str(type(self.getValueType(metaData[col]))))
                i = i + 1
            for key in metaData:
                if key in dataRow.keys():
                    pass
                else:
                    dataRow[key] = ""
            tableData.append(dataRow)
            print("data added")

        tableDir = self.databaseDir + tableName + "/"
        tableDataFilePath = tableDir + tableName + "_data.json"
        with open(tableDataFilePath, 'w') as f:
            f.truncate()
            json.dump(tableData, f)

        return metaData

    def dropTable(self, tableName):
        tableData = self.getDataFromTable(tableName)
        tableDir = self.databaseDir + tableName + "/"
        if(self.checkIfFilePathExist(tableDir)):
            shutil.rmtree(tableDir)
        else:
            raise Exception ("Wrong Table Name")

    def updateRow(self, tableData, i, colList, metaData):
        for key in colList.keys():
           if key in tableData[i].keys():
               if (type(colList[key]) == type(self.getValueType(metaData[key]))):
                   tableData[i][key] = colList[key]
               else:
                   raise Exception(str(key) + " should be of type " + str(type(self.getValueType(metaData[key]))))
           else:
               raise Exception (key+ " not present in table")



    def updateQuery(self, tableName, colList, condition ):
        #" UPDATE Customers SET ContactName='Juan' WHERE Country='Mexico';"
        tableData = self.getDataFromTable(tableName)

        tableDir = self.databaseDir + tableName + "/"
        tableDataFilePath = tableDir + tableName + "_metadata.json"
        metaData = ""
        with open(tableDataFilePath) as file:
            metaData = json.load(file)
        metaData = metaData["columns"]

        if (len(tableData) == 0):
            print(tableName + " Table is Empty")
            return
        else:
            if (len(condition) > 0):
                dataLength = len(tableData)
                i = 0
                while i < dataLength:
                    row = tableData[i]
                    colToCheck = condition["columnName"]
                    operator = condition["operator"]
                    valueToCheck = condition["value"]
                    if (colToCheck in row.keys()):
                        if operator == "=" :
                            if (str(row[colToCheck]) == (valueToCheck)):
                                self.updateRow(tableData, i, colList, metaData)
                        elif operator == ">":
                            if (str(row[colToCheck]) > (valueToCheck)):
                                self.updateRow(tableData, i, colList, metaData)
                        elif (operator == "<"):
                            if (str(row[colToCheck]) < (valueToCheck)):
                                self.updateRow(tableData, i, colList, metaData)
                        elif (operator == ">="):
                            if (str(row[colToCheck]) >= (valueToCheck)):
                                self.updateRow(tableData, i, colList, metaData)
                        elif (operator == "<="):
                            if (str(row[colToCheck]) <= (valueToCheck)):
                                self.updateRow(tableData, i, colList, metaData)
                        elif (operator == "!="):
                            if (str(row[colToCheck]) != (valueToCheck)):
                                self.updateRow(tableData, i, colList, metaData)
                    i = i + 1

        tableDir = self.databaseDir + tableName + "/"
        tableDataFilePath = tableDir + tableName + "_data.json"
        with open(tableDataFilePath, 'w') as f:
            f.truncate()
            json.dump(tableData, f)

        print("Table Updated")
