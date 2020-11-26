import QueryProcessor as qp
if __name__ == "__main__":

    #Make Object of Query Processor
    queryProcessor = qp.QueryProcessor()  

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
