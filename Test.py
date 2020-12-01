import QueryProcessor as qp
import time

if __name__ == "__main__":

    #Make Object of Query Processor
    queryProcessor = qp.QueryProcessor()

    #---------CREATE DATABASE COMMAND-----------
    '''
    try:
        #this will create DB
        queryProcessor.createDB("PersonDataBase")

        #Now since DB is made it will give error
        queryProcessor.createDB("PersonDataBase")
    except Exception as e:
        print(e)
    '''
    
    #---------USE COMMAND-----------
    '''
    try:
        #this will select DB
        queryProcessor.useDb("PersonDataBase")

        #this will give error since this DB dont exist
        queryProcessor.useDb("wrongNameOfDB")
    except Exception as e:
        print(e)
    '''

    #---------CREATE TABLE COMMAND-----------
    '''
    try:
        #selecting DB, (run USE DB CODE FIRST)
        queryProcessor.useDb("PersonDataBase")

        #Data And its format needed -- For MALAV
        tableName = "PERSON2"
        columnDict = {
            'PersonID': 'int',
            'LastName': 'varchar(255)',
            'FirstName': 'varchar(255)',
            'Address': 'varchar(255)',
            'City': 'varchar(255)'
        }
        primaryKeyList = ['PersonID', 'Address']
        foreignKeyList = [] #not donw this part yet

        print("command to make Table")
        queryProcessor.createTable(tableName, columnDict, primaryKeyList, foreignKeyList)

        print("now error since its already created")
        queryProcessor.createTable(tableName, columnDict, primaryKeyList, foreignKeyList)
        
    except Exception as e:
        print(e)
    '''
    
    #---------DELETE TABLE COMMAND-----------
    '''
    try:
        #selecting DB, (run CREATE DB CODE FIRST)
        queryProcessor.useDb("PersonDataBase")

        tableName = "PERSON"

        condition = {
                "columnName" : "PersonID",
                "operator" : ">",
                "value" : "2"
            }

        print("DELETE FROM table_name WHERE condition")
        queryProcessor.deleteQuery(tableName, condition)
    
        print("DELETE FROM table_name")
        queryProcessor.deleteQuery(tableName)

    except Exception as e:
        print(e)
    '''

    #---------INSERT TABLE COMMAND-----------
    
    try:
        #selecting DB, (run CREATE DB CODE FIRST)
        queryProcessor.useDb("testdb")

        tableName = "testtable"

        valueList = ["999" , "sd", "3.1"]
        print(" INSERT INTO table_name VALUES (value1, value2, value3, ...);")
        queryProcessor.insertQuery( tableName, valueList = valueList)
        queryProcessor.selectQuery( tableName, columnListToDisplay=["*"] )

        valueList1 = ["999" , "sd"]
        colList = ["col1" , "col3"]
        print("INSERT INTO table_name (column1, column2, column3, ...) VALUES (value1, value2, value3, ...);")
        queryProcessor.insertQuery( tableName, valueList=valueList1 ,colList=colList)
        queryProcessor.selectQuery( tableName, columnListToDisplay=["*"] )
    except Exception as e:
        print(e)
    

    #---------SELECT TABLE COMMAND-----------
    '''
    try:
        #selecting DB, (run CREATE DB CODE FIRST)
        queryProcessor.useDb("PersonDataBase")

        #Data And its format needed -- For MALAV
        tableName = "PERSON"
        cols = ["PersonID", "FirstName", "City"]
        condition = {
                "columnName" : "PersonID",
                "operator" : "<",
                "value" : "999"
            }
        #print(" SELECT * FROM TABLE ")
        #queryProcessor.selectQuery( tableName, columnListToDisplay = ["*"])
        
        #print(" SELECT * FROM TABLE WHERE CONDITION ")
        #queryProcessor.selectQuery( tableName, condition = condition , columnListToDisplay = ["*"])

        #print(" SELECT <cols> FROM TABLE")
        #queryProcessor.selectQuery( tableName, columnListToDisplay = cols)

        print(" SELECT <cols> FROM TABLE WHERE CONDITION ")
        queryProcessor.selectQuery( tableName, condition = condition, columnListToDisplay = cols)
    except Exception as e:
        print(e)
    '''

    #-------------------------DROP TABLE COMMAND-----------------------
    '''
    try:
        #selecting DB, (run CREATE DB CODE FIRST)
        queryProcessor.useDb("PersonDataBase")

        tableName = "PERSON"
        print("DROP TABLE table_name;")
        queryProcessor.dropTable(tableName)
    except Exception as e:
        print(e)

    '''

    #--------------------------UPDATE TABLE COMMAND-----------------------
    '''
    try:
        tableName = "PERSON2"
        condition = {
                "columnName" : "PersonID",
                "operator" : "=",
                "value" : "12"
            }
        colList = {
            "PersonID":999,
            "FirstName":"Nikunj",
            "City": "Delhi"
            }

        #selecting DB, (run CREATE DB CODE FIRST)
        queryProcessor.useDb("PersonDataBase")
        
        print(" UPDATE Customers SET ContactName='Juan' WHERE Country='Mexico';")
        queryProcessor.updateQuery(tableName,colList,condition )

        queryProcessor.selectQuery( "PERSON2", columnListToDisplay = ["*"])
    except Exception as e:
        print(e)
    ''' 
    
    #-------------------------DROP DB COMMAND-----------------------
    '''
    try:
        databasename = "PersonDataBase"
        print("Create DATABASE PersonDataBase;")
        #queryProcessor.createDB("PersonDataBase")

        print("DROP DATABASE databasename;")
        queryProcessor.dropDb(databasename)
    except Exception as e:
        print(e)
    '''
    #-------------------------TRANSACTION COMMAND-----------------------
    '''
    try:
        dbname = "abcd"
        table = "nikunj"
        
        queryProcessor.startTransaction(dbname, table)
        queryProcessor.selectQuery( table, columnListToDisplay = ["*"])

        time.sleep(3)
        valueList = ["995439" , "sDFSGSd", "3.1"]
        queryProcessor.insertQuery( table, valueList = valueList)
        queryProcessor.selectQuery( table, columnListToDisplay = ["*"])
      
        valueList = ["954399" , "sd", "3.641"]
        queryProcessor.insertQuery( table, valueList = valueList)
        queryProcessor.selectQuery( table, columnListToDisplay = ["*"])
        
        time.sleep(2)
        queryProcessor.setSavePoint("sav")
        time.sleep(2)
        valueList = ["9634299" , "sdRTE", "53.1"]
        queryProcessor.insertQuery( table, valueList = valueList)
        queryProcessor.selectQuery( table, columnListToDisplay = ["*"])

        time.sleep(2)
        print("")
        print("")
        queryProcessor.rollback("sav")
        print("")
        print("")
        print("")
        print("")
        time.sleep(2)
        queryProcessor.selectQuery( table, columnListToDisplay = ["*"])

        print("")
        print("")

        valueList = ["954399" , "sd", "3.641"]
        queryProcessor.insertQuery( table, valueList = valueList)
        queryProcessor.selectQuery( table, columnListToDisplay = ["*"])
        table = "nikunj2"
        time.sleep(2)
        queryProcessor.setSavePoint("sav2")
        time.sleep(2)
        valueList = ["9634299" , "sdRTE", "53.1"]
        queryProcessor.insertQuery( table, valueList = valueList)
        queryProcessor.selectQuery( table, columnListToDisplay = ["*"])

        time.sleep(2)
        print("")
        print("")
        queryProcessor.rollback("sav2")
        print("")
        print("")
        print("")
        print("")
        time.sleep(2)
        
        table = "nikunj2"
        queryProcessor.selectQuery( table, columnListToDisplay = ["*"])

        print("")
        print("")
        
        time.sleep(2)
        #queryProcessor.commit()
    
    except Exception as e:
        print(e)
    '''


    queryProcessor.useDb('abcd')
    queryProcessor.describeTable("nikunj2")

    time.sleep(20)
    queryProcessor.describeDb('abcd')
