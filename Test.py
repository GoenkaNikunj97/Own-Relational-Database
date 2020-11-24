import QueryProcessor as qp
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
        #selecting DB, (run CREATE DB CODE FIRST)
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
                "operator" : "=",
                "value" : "2"
            }
        print(" SELECT * FROM TABLE ")
        queryProcessor.selectQuery( tableName )
        
        print(" SELECT * FROM TABLE WHERE CONDITION ")
        queryProcessor.selectQuery( tableName, condition = condition )

        print(" SELECT <cols> FROM TABLE")
        queryProcessor.selectQuery( tableName, columnListToDisplay = cols)

        print(" SELECT <cols> FROM TABLE WHERE CONDITION ")
        queryProcessor.selectQuery( tableName, condition = condition, columnListToDisplay = cols)

        
        
    except Exception as e:
        print(e)

    '''
    #---------SELECT TABLE COMMAND-----------
    

   
'''
    
    condition = {
                "columnName" : "PersonID",
                "operator" : "=",
                "value" : "3"
            }
    queryProcessor.readTable( "Person", condition = condition )
    
    #queryProcessor.createDB("RAJA")
'''
