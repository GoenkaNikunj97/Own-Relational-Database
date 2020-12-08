import os
import json
import shutil
from termcolor import colored
# User_defined Classes
import DataType
import Presentation
import LockingMechanismChecker as l

class QueryProcessor:
    def __init__(self):
        self.databaseName = ""
        self.databaseDir = ""
        self.transaction = False
        self.locker = l.LockingMechanismChecker()
    # ==========================common methods used below===========================================
    def checkIfFilePathExist(self, dirName):
        if os.path.exists(dirName):
            return True
        else:
            return False

    def getDataFromTable(self, tableName):
        if (self.databaseDir == ""):
            raise Exception("Select Database First")
        tableDir = self.databaseDir + tableName + "/"
        if self.checkIfFilePathExist(tableDir):
            tableDataFilePath = tableDir + tableName + "_data.json"
            tableData = ""
            with open(tableDataFilePath) as file:
                tableData = json.load(file)
            return (tableData)
        else:
            raise Exception(tableName + " Table does not Exist")

    def getValueType(self, data):
        dataType = ["str", "int", "float"]
        if (data == "str"):
            return "string"
        elif (data == "int"):
            return 1
        elif (data == "float"):
            return 1.1

    def getDataInRequiredFormat(self, value):
        try:
            data = int(value)
            return data
        except ValueError as e:
            data = float(value)
            return data

    def saveDataToTable(self, tableName, tableData):
        tableDataFilePath = self.databaseDir + tableName + "/" + tableName + "_data.json"
        with open(tableDataFilePath, 'w') as f:
            f.truncate()
            json.dump(tableData, f)

    # ============================== commands not afftected by transaction =========================================
    def useDb(self, DBName):
        dbDir = "AllDatabase/" + DBName
        if (self.checkIfFilePathExist(dbDir)):
            self.databaseName = DBName
            self.databaseDir = "AllDatabase/" + self.databaseName + "/"
            print(self.databaseName, " Selected")
        else:
            raise Exception("Database " + DBName + " Does Not Exist")

    def createDB(self, DBName):
        dbDir = "AllDatabase/" + DBName
        if (self.checkIfFilePathExist(dbDir)):
            raise Exception("Database already Exist")
        else:
            print(DBName, " Created")
            os.makedirs(dbDir)
            self.useDb(DBName)

    def dropDb(self, databaseName):
        databaseDir = "AllDatabase/" + databaseName
        if (self.checkIfFilePathExist(databaseDir)):
            shutil.rmtree(databaseDir)
            print(databaseName + " Database droped")
        else:
            raise Exception("Database does not exist " + databaseName)

    def createTable(self, tableName, columnDict, primaryKeyList, foreignKeyList=[]):
        if (self.databaseName == ""):
            raise Exception("Select Database first")
        else:
            tableName = tableName.rstrip().lstrip()
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

    def dropTable(self, tableName):
        tableDir = self.databaseDir + tableName + "/"
        if (self.checkIfFilePathExist(tableDir)):
            shutil.rmtree(tableDir)
            print(tableName + " table dropped")
        else:
            raise Exception("Wrong Table Name")

    def dump(self, dbName):
        dbDir = "AllDatabase/" + dbName
        if (self.checkIfFilePathExist(dbDir)):
            databaseDir = "AllDatabase/" + dbName + "/"
            tableList = os.listdir("AllDatabase/" + dbName)
            queryList = ""
            for table in tableList:
                if table == "lock.json":
                    continue
                query = "CREATE TABLE " + table + " ( "

                tableDir = databaseDir + table + "/"
                tableDataFilePath = tableDir + table + "_metadata.json"

                metaData = ""

                with open(tableDataFilePath) as file:
                    metaData = json.load(file)

                for key in metaData["columns"]:
                    query = query + key + " " + metaData["columns"][key] + ","

                query = query.rstrip(query[-1])
                query = query + " );"
                queryList = queryList + query + "\n"

            sqlFolder = "sql"
            if (self.checkIfFilePathExist(sqlFolder)):
                pass
            else:
                os.makedirs("sql")
            dumpLocation = "sql/" + dbName + ".txt"
            with open(dumpLocation, 'w') as f:
                f.truncate()
                f.write(queryList)

            print("SQL Dump Created at location: "+dumpLocation )
        else:
            raise Exception("Database " + dbName + " Does Not Exist")

    # ================================== Transaction COMMANDS ====================================================
    def startTransaction(self, DbName, tableName):
        if(self.locker.checkLock(tableName,DbName)):
            raise Exception ("Other User have lock on this table")
        else:
            self.locker.addLock(tableName, DbName)
            self.transaction = True
            self.tableName = tableName
            self.databaseName = DbName
            self.tableDataList = list()
            self.savePointDict = dict()  # { 'name': index, }
            self.useDb(DbName)
            self.tableDataPath = "AllDatabase/" + DbName + tableName + "_data.json"
            self.tableDataList.append(self.getDataFromTable(tableName))
            print("transaction started")

    def setSavePoint(self, name):
        self.savePointDict[name] = len(self.tableDataList) - 1
        print("Save point set")

    def rollback(self, savePoint=""):
        if savePoint == "":
            self.tableDataList = self.tableDataList[:1]
            print("rolled back to orignal file")
            self.locker.removeLock(self.tableName, self.databaseName)
            self.transaction=False
        elif savePoint in self.savePointDict.keys():
            # rollback to save point
            while len(self.tableDataList) > self.savePointDict[savePoint] + 1:
                self.tableDataList.pop()
            print("Rolled back to :" + savePoint)
        else:
            raise Exception("No such savepoint present")

    def commit(self):
        tableData = self.tableDataList[-1]
        tableName = self.tableName
        self.saveDataToTable(tableName, tableData)
        print("Data Saved to db")
        self.transaction = False
        self.locker.removeLock(self.tableName , self.databaseName)

    def isTransaction(self):
        return self.transaction

        # ========================================query that dont use Transactions====================================

    # ============================== commands afftected by transaction =========================================
    def selectQuery(self, tableName, columnListToDisplay=[], condition={}):
        if self.isTransaction():
            if (tableName == self.tableName):
                tableData = self.tableDataList[-1].copy()
            else:
                raise Exception(
                    "transaction have lock on table " + self.tableName + " Commit or rollback to start new transaction")
        else:
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

        # Display to user
        Presentation.displayJSONListData(tableData)

    def deleteQuery(self, tableName, condition=[]):
        if self.isTransaction():
            if (tableName == self.tableName):
                tableData = self.tableDataList[-1].copy()
            else:
                raise Exception(
                    "transaction have lock on table " + self.tableName + " Commit or rollback to start new transaction")
        else:
            tableData = self.getDataFromTable(tableName)
            if(self.locker.checkLock(tableName,self.databaseName)):
                raise Exception("This table is locked by other user")
            else:
                self.locker.addLock(tableName,self.databaseName)

        if (len(tableData) == 0):
            print(tableName + " Table is Empty")
            self.locker.removeLock(tableName, self.databaseName)
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
                            i = i - 1
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

        if self.isTransaction():
            self.tableDataList.append(tableData)
        else:
            tableDir = self.databaseDir + tableName + "/"
            tableDataFilePath = tableDir + tableName + "_data.json"
            with open(tableDataFilePath, 'w') as f:
                f.truncate()
                if (len(condition) > 0):
                    json.dump(tableData, f)
                else:
                    f.write("[]")
            self.locker.removeLock(tableName,self.databaseName)

    def insertQuery(self, tableName, valueList, colList=[]):
        if self.isTransaction():
            if (tableName == self.tableName):
                tableData = self.tableDataList[-1].copy()
            else:
                raise Exception(
                    "transaction have lock on table " + self.tableName + " Commit or rollback to start new transaction")
        else:
            tableData = self.getDataFromTable(tableName)
            if (self.locker.checkLock(tableName, self.databaseName)):
                raise Exception("This table is locked by other user")
            else:
                self.locker.addLock(tableName, self.databaseName)

        tableDir = self.databaseDir + tableName + "/"
        tableDataFilePath = tableDir + tableName + "_metadata.json"
        metaData = ""
        with open(tableDataFilePath) as file:
            metaData = json.load(file)
        metaData = metaData["columns"]
        if (len(colList) == 0):
            if (len(valueList) == len(metaData)):
                i = 0
                dataRow = dict()
                for key in metaData.keys():
                    if type(self.getValueType(metaData[key])) != type("str"):
                        try:
                            dataInNeededFormat = self.getDataInRequiredFormat(valueList[i])
                        except:
                            self.locker.removeLock(tableName, self.databaseName)
                            raise Exception(
                                str(key) + " should be of type " + str(type(self.getValueType(metaData[key]))))
                        if (type(dataInNeededFormat) == type(self.getValueType(metaData[key]))):
                            dataRow[key] = dataInNeededFormat
                        else:
                            self.locker.removeLock(tableName, self.databaseName)
                            raise Exception(
                                str(key) + " should be of type " + str(type(self.getValueType(metaData[key]))))
                    else:
                        # its in str format so just put it in
                        dataRow[key] = valueList[i]
                    i = i + 1

                tableData.append(dataRow)
                print("data added")
            else:
                self.locker.removeLock(tableName, self.databaseName)
                raise Exception(tableName + " have " + str(len(metaData)) + " columns but " + str(
                    len(valueList)) + " values was given")
        else:
            i = 0
            dataRow = dict()
            for col in colList:
                col = col.lstrip().rstrip()
                if col in metaData.keys():
                    if type(self.getValueType(metaData[col])) != type("str"):
                        try:
                            dataInNeededFormat = self.getDataInRequiredFormat(valueList[i])
                        except:
                            self.locker.removeLock(tableName, self.databaseName)
                            raise Exception(
                                str(col) + " should be of type " + str(type(self.getValueType(metaData[col]))))
                        if (type(dataInNeededFormat) == type(self.getValueType(metaData[col]))):
                            dataRow[col] = dataInNeededFormat
                        else:
                            self.locker.removeLock(tableName, self.databaseName)
                            raise Exception(
                                str(col) + " should be of type " + str(type(self.getValueType(metaData[col]))))
                    else:
                        dataRow[col] = valueList[i]
                i = i + 1
            for key in metaData:
                if key in dataRow.keys():
                    pass
                else:
                    dataRow[key] = ""
            tableData.append(dataRow)
            print("data added")

        if self.isTransaction():
            self.tableDataList.append(tableData)
        else:
            tableDir = self.databaseDir + tableName + "/"
            tableDataFilePath = tableDir + tableName + "_data.json"
            with open(tableDataFilePath, 'w') as f:
                f.truncate()
                json.dump(tableData, f)
            self.locker.removeLock(tableName, self.databaseName)

        return metaData

    # method made just of Update query
    def updateRow(self, tableName,tableData, i, colList, metaData):
        for key in colList.keys():
            if key in tableData[i].keys():
                if type(self.getValueType(metaData[key])) != type("str"):
                    try:
                        dataInNeededFormat = self.getDataInRequiredFormat(colList[key])
                    except:
                        self.locker.removeLock(tableName, self.databaseName)
                        raise Exception(str(key) + " should be of type " + str(type(self.getValueType(metaData[key]))))
                    if (type(dataInNeededFormat) == type(self.getValueType(metaData[key]))):
                        if (type(colList[key]) == type(self.getValueType(metaData[key]))):
                            tableData[i][key] = dataInNeededFormat
                        else:
                            self.locker.removeLock(tableName, self.databaseName)
                            raise Exception(str(key) + " should be of type " + str(type(self.getValueType(metaData[key]))))
                else:
                # its in str format so just put it in
                    tableData[i][key] = colList[key]
            else:
                self.locker.removeLock(tableName, self.databaseName)
                raise Exception(key + " not present in table")

    def updateQuery(self, tableName, colList, condition):
        if self.isTransaction():
            if (tableName == self.tableName):
                tableData = self.tableDataList[-1].copy()
            else:
                raise Exception(
                    "transaction have lock on table " + self.tableName + " Commit or rollback to start new transaction")
        else:
            tableData = self.getDataFromTable(tableName)
            if (self.locker.checkLock(tableName, self.databaseName)):
                raise Exception("This table is locked by other user")
            else:
                self.locker.addLock(tableName, self.databaseName)

        tableDir = self.databaseDir + tableName + "/"
        tableDataFilePath = tableDir + tableName + "_metadata.json"
        metaData = ""
        with open(tableDataFilePath) as file:
            metaData = json.load(file)
        metaData = metaData["columns"]

        if (len(tableData) == 0):
            self.locker.removeLock(tableName, self.databaseName)
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
                        if operator == "=":
                            if (str(row[colToCheck]) == (valueToCheck)):
                                self.updateRow(tableData, i, tableName,colList, metaData)
                        elif operator == ">":
                            if (str(row[colToCheck]) > (valueToCheck)):
                                self.updateRow(tableData, i, tableName,colList, metaData)
                        elif (operator == "<"):
                            if (str(row[colToCheck]) < (valueToCheck)):
                                self.updateRow(tableData, i, tableName,colList, metaData)
                        elif (operator == ">="):
                            if (str(row[colToCheck]) >= (valueToCheck)):
                                self.updateRow(tableData, i, tableName,colList, metaData)
                        elif (operator == "<="):
                            if (str(row[colToCheck]) <= (valueToCheck)):
                                self.updateRow(tableData, i, tableName,colList, metaData)
                        elif (operator == "!="):
                            if (str(row[colToCheck]) != (valueToCheck)):
                                self.updateRow(tableData, i,tableName, colList, metaData)
                    i = i + 1
        if self.isTransaction():
            self.tableDataList.append(tableData)
        else:
            tableDir = self.databaseDir + tableName + "/"
            tableDataFilePath = tableDir + tableName + "_data.json"
            with open(tableDataFilePath, 'w') as f:
                f.truncate()
                json.dump(tableData, f)
            self.locker.removeLock(tableName, self.databaseName)
        print("Table Updated")

    def describeTable(self, tableName):
        if self.isTransaction():
            if (tableName == self.tableName):
                tableData = self.tableDataList[-1].copy()
            else:
                raise Exception(
                    "transaction have lock on table " + self.tableName + " Commit or rollback to start new transaction")
        else:
            tableDir = self.databaseDir + tableName + "/"
            if(not self.checkIfFilePathExist(tableDir)):
                raise Exception ("Table Name is not Valid")

            tableDataFilePath = tableDir + tableName + "_metadata.json"
            metaData = ""
            with open(tableDataFilePath) as file:
                metaData = json.load(file)

            print(colored("\n#####################################################", 'green'))
            print(colored("\n        STRUCTURE FOR TABLE: " + tableName, 'green'))
            print("{:<30}".format("Column Name"), end=' ')
            print("{:<30}".format("Data Type"), end=' ')
            print()
            for key in metaData["columns"]:
                print("{:<30}".format(key), end=' ')
                print("{:<30}".format(metaData["columns"][key]), end=' ')
                print()
            print(colored("\n#####################################################", 'green'))

    def describeDb(self, dbName):
        print(colored("\n#####################################################", 'green'))
        print(colored("\n        Tables in Databse: " + dbName, 'green'))
        self.databaseDir = "AllDatabase/" + dbName + "/"
        tableList = os.listdir("AllDatabase/" + dbName)
        for table in tableList:
            if table == "lock.json":
                continue
            self.describeTable(table)

