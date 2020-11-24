from tabulate import tabulate


def displayJSONListData(fileData):
    tableContent = list()
    for row in fileData:
        tableContent.append(row.values())

    print ( tabulate(tableContent, headers= row.keys()))
        
        
data = [[1, 'Liquid', 24, 12],
[2, 'Virtus.pro', 19, 14],
[3, 'PSG.LGD', 15, 19],
[4,'Team Secret', 10, 20]]

