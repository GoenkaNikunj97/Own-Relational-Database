import os
import json

# User_defined Classes
import DataType
import Presentation


class QueryProcessos:

    def __init__(self, databaseName):
        self.databaseName = databaseName
        self.databaseDir = "AllDatabase/" + self.databaseName + "/"

    def checkIfFilePathExist(self, tableDir):
        if os.path.exists(tableDir):
            return True
        else:
            return False

    def createTable(self, tableName, columnDict, primaryKeyList, foreignKeyList):

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

    def readTable(self, tableName, columnListToDisplay=[], condition={}):
        '''
            condition = {
                "columnName" : "PersonID",
                "operator" : "=",
                "value" : "1"
            }
        '''
        tableDir = self.databaseDir + tableName + "/"
        
        if self.checkIfFilePathExist(tableDir):
            tableDataFilePath = tableDir + tableName + "_data.json"
            tableData = ""
            
            with open (tableDataFilePath) as file:
                tableData = json.load(file)
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

                tableData = data

        Presentation.displayJSONListData(tableData)


if __name__ == "__main__":

    queryProcessor = QueryProcessos("PersonDataBase")
    a = {
        'PersonID': 'int',
        'LastName': 'varchar(255)',
        'FirstName': 'varchar(255)',
        'Address': 'varchar(255)',
        'City': 'varchar(255)'
    }
    try:
        queryProcessor.createTable(
            "SecondTable", a, ['PersonID', 'Address'], [])
    except Exception as e:
        print(e)
    condition = {
                "columnName" : "PersonID",
                "operator" : "!=",
                "value" : "3"
            }
    queryProcessor.readTable( "Person", condition = condition )
