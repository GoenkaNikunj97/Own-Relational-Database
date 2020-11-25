from tabulate import tabulate


def displayJSONListData(fileData):


    for key in fileData[0]:
        print( "{:<10}".format(key)  , end = ' ' )
    print()    
    for key in fileData[0]:
        print( "{:<10}".format("-----")  , end = ' ' )
    print()
    keylist = fileData[0].keys()
    for row in fileData:
        for key in keylist:
            print( "{:<10}".format( row[key] )  , end = ' ' )
        print()
    
    '''
    tableContent = list()
    for row in fileData:
        tableContent.append(row.values())

    print ( tabulate(tableContent, headers= row.keys()))
    ''' 
        
