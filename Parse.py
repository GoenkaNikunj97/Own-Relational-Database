import constants
import re
import QueryProcessor as qp
import Logging as logging
import Transaction as trns
from termcolor import colored


class Parse:
    transcationFlag=False
    newDB=True
    count=0
    log=logging.Logging()    
    def __init__(self,database,query,queryProcessor):
        self.queryProcessor=queryProcessor
        database=database.replace(";","")
        database=database.strip()
        self.database=database.lower()
        query=query.replace(";","")
        query=query.strip()
        self.query = query.lower()
        
    def check_query(self):
        if constants.select in self.query.lower():
            if self.database == "":
                self.log.pushLog(self.query,constants.db_missing)
                print(constants.db_missing)
                return
            self.select()
        elif constants.delete in self.query.lower():
            if self.database == "":
                self.log.pushLog(self.query,constants.db_missing)
                print("Database not selected")
                return
            self.delete()
        elif constants.insert in self.query.lower():
            if self.database == "":
                self.log.pushLog(self.query,constants.db_missing)
                print("Database not selected")
                return
            self.insert()
        elif constants.create in self.query.lower():
            self.create()
        elif constants.update in self.query.lower():
            if self.database == "":
                self.log.pushLog(self.query,constants.db_missing)
                print("Database not selected")
                return
            self.update()
        elif constants.drop in self.query.lower():
            self.drop()
        elif "describe" in self.query.lower() or "desc" in self.query.lower():
            self.describe()
        elif "erd" in self.query.lower():
            self.showERD()
        elif "begin transaction" in self.query.lower():
            if self.database == "":
                self.log.pushLog(self.query,constants.db_missing)
                print("Database not selected")
                return
            Parse.transcationFlag=True
            self.beginTransaction()
            return
        elif "dump" in self.query.lower():
            self.dump()
        elif "quit" in self.query.lower():
            if Parse.transcationFlag:
                Parse.transcationFlag=False
                self.queryProcessor.rollback()
            self.count=0
            return 0
        elif "use" in self.query.lower():
            Parse.newDB=True
            return
        elif "commit" in self.query.lower():
            if Parse.transcationFlag:
                self.queryProcessor.commit()
                print("commited")
                Parse.transcationFlag=False
                self.count=0
                return 0
            else:
                print("No transaction")
                return
        elif "rollback" in self.query.lower():
            if Parse.transcationFlag:
                check=self.rollback()
                if check==0:
                    Parse.transcationFlag=False 
                    self.count=0
                    return 0
                elif check==1:
                    return
            else:
                print("No transaction")
                return
        elif "savepoint" in self.query.lower():
            try:
                if Parse.transcationFlag:
                    savepoint=re.compile(r'savepoint\s*(.*).*',re.IGNORECASE).findall(self.query)
                    savePointName=savepoint[0]
                    savePointName=savePointName.rstrip().lstrip()
                    self.queryProcessor.setSavePoint(savePointName)
                else:
                    print("No transaction")
                    return
            except IndexError as e:
                print("Error in Query Syntax")   
            except Exception as e:
                print(e)
 
        else:
            return -1

    def showERD(self):
        try:
            dbName = re.compile(r'ERD\s*(.*).*', re.IGNORECASE).findall(self.query)
            self.queryProcessor.showErd(dbName[0])
        except Exception as e:
            print(e)

    def select(self):
        try:
            if Parse.newDB:
                self.queryProcessor.useDb(self.database)
                Parse.newDB=False
            condition_column = ''
            condition_value = ''

            # with condition
            if constants.where_clause in self.query:
                raw_columns = re.compile(constants.select_column_RE, re.IGNORECASE).findall(self.query)
                columns = self.format(raw_columns[0])
                tableName = re.compile(constants.select_table_condition_RE, re.IGNORECASE).findall(self.query)

                condition_column = re.compile(constants.select_condition_column_RE, re.IGNORECASE).findall(self.query)
                condition_value = re.compile(constants.select_condition_value_RE, re.IGNORECASE).findall(self.query)

                condition = self.formatCondition(condition_column, condition_value)
                table=tableName[0]
                table=table.lstrip().rstrip()
                if Parse.transcationFlag:
                    if self.count==0:
                        self.count+=1
                        self.queryProcessor.startTransaction(self.database,table)
                self.queryProcessor.selectQuery( table, condition = condition, columnListToDisplay = columns)
                self.log.pushLog(self.query,"Selected "+table+" from "+" database " +self.database)
            # without condition
            else:
                raw_columns = re.compile(
                    constants.select_column_RE, re.IGNORECASE).findall(self.query)
                columns = self.format(raw_columns[0])

                tableName = re.compile(
                    constants.select_table_no_condition_RE, re.IGNORECASE).findall(self.query)
                table=tableName[0]
                table=table.lstrip().rstrip()
                if Parse.transcationFlag:
                    if self.count==0:
                        self.count+=1
                        self.queryProcessor.startTransaction(self.database,table)
                self.queryProcessor.selectQuery( table, columnListToDisplay = columns)
                self.log.pushLog(self.query,"Selected "+table+" from "+" database "+self.database)

        except IndexError as e:
            self.log.pushLog(self.query,"Error in select query syntax")
            print("Error in Query Syntax")
        except Exception as e:
            self.log.pushLog(self.query,str(e))
            print(e)

    def delete(self):
        try:
            if Parse.newDB:
                self.queryProcessor.useDb(self.database)
                Parse.newDB=False
            
            if constants.where_clause in self.query:
                tableName = re.compile(constants.delete_table_RE,re.IGNORECASE).findall(self.query)
                condition_column = re.compile(
                    constants.delete_condition_column_RE, re.IGNORECASE).findall(self.query)
                condition_value = re.compile(
                    constants.delete_condition_value_RE, re.IGNORECASE).findall(self.query)
                condition = self.formatCondition(condition_column, condition_value)
                table=tableName[0]
                table=table.lstrip().rstrip()
                if Parse.transcationFlag:
                    if self.count==0:
                        self.count+=1
                        self.queryProcessor.startTransaction(self.database,table)
                self.queryProcessor.deleteQuery(table, condition)
                self.log.pushLog(self.query,"Data deleted from table "+table+" of database " +self.database)
            elif constants.where_clause not in self.query:
                tableName= re.compile(r'from\s(.*)\s*',re.IGNORECASE).findall(self.query)
                table=tableName[0]
                table=table.lstrip().rstrip()
                if Parse.transcationFlag:
                    if self.count==0:
                        self.count+=1
                        self.queryProcessor.startTransaction(self.database,table)
                self.queryProcessor.deleteQuery(table)
                self.log.pushLog(self.query,"Data successfully deleted from table "+table+" of database " +self.database)
        except IndexError as e:
            self.log.pushLog(self.query,"Error in select query syntax")
            print("Error in Query Syntax")
        except Exception as e:
            self.log.pushLog(self.query,str(e))
            print(e)

    def insert(self):
        try:
            if Parse.newDB:
                self.queryProcessor.useDb(self.database)
                Parse.newDB=False
            table = re.compile(constants.insert_table_RE,re.IGNORECASE).findall(self.query)
            table = table[0]
            table = table[:table.find('(')]
            table=table.lstrip().rstrip()
            raw_columns = re.compile(constants.insert_columns_RE,re.IGNORECASE).findall(self.query)
            raw_values = re.compile(constants.insert_values_RE,re.IGNORECASE).findall(self.query)
            values=raw_values[0].split(",")
            columns=raw_columns[0].split(",")
            if Parse.transcationFlag:
                if self.count==0:
                    self.count+=1
                    self.queryProcessor.startTransaction(self.database,table)
            self.queryProcessor.insertQuery(table,valueList=values,colList=columns)
            self.log.pushLog(self.query,"Data successfully inserted into table " +table+" of database "+ self.database )
        except IndexError as e:
            self.log.pushLog(self.query,"Error in select query syntax")
            print("Error in Query Syntax") 
        except Exception as e:
            self.log.pushLog(self.query,str(e))
            print(e)

    def create(self):
        try:
            if "database" in self.query:
                database = re.compile(r'create database\s(.*)\s*',re.IGNORECASE).findall(self.query)
                self.queryProcessor.createDB(database[0])
                self.log.pushLog(self.query,"Successfully created database "+self.database)
            elif "table" in self.query:
                if Parse.newDB:
                    self.queryProcessor.useDb(self.database)
                    Parse.newDB=False 
                raw_values=re.compile(r'\((.*)\)',re.IGNORECASE).findall(self.query)
                foreignKeyList=[]                
                primaryKeyList=[]
                if "primary" in raw_values[0].lower():
                    tableName = re.compile(r'create table\s*(.*)\s*\(+?',re.IGNORECASE).findall(self.query)
                    tableName = re.compile(r'\s*(.*)\s\(',re.IGNORECASE).findall(str(tableName[0]))
                    foreignKeyList=[]                
                    primaryKeyList=re.compile(r'primary key \((.*)\)',re.IGNORECASE).findall(raw_values[0])

                    raw_values[0]=re.sub(r'primary key \((.*)\)',"",raw_values[0],re.IGNORECASE)
                    raw_values[0]=re.sub(r'foreign key (.*)',"",raw_values[0],re.IGNORECASE)
                
                    value_list=raw_values[0].split(",")
                    value_list.remove('')
                else:
                    tableName = re.compile(r'create table\s*(.*)\s*\(+?',re.IGNORECASE).findall(self.query)
                    value_list=raw_values[0].split(",")
                columnDict={}
                for val in value_list:
                    temp=val.split()
                    column=str(temp[0])
                    clm_type=str(temp[1])
                    columnDict[column]=clm_type
                table=tableName[0]
                table=table.lstrip().rstrip()
                self.queryProcessor.createTable(table, columnDict, primaryKeyList, foreignKeyList)
                self.log.pushLog(self.query,"Successfully created table "+table+" from database " +self.database)
        except IndexError as e:
            self.log.pushLog(self.query,"Error in select query syntax")
            print("Error in Query Syntax")
        except Exception as e:
            self.log.pushLog(self.query,str(e))
            print(e)

    def update(self):
        try:
            if Parse.newDB:
                self.queryProcessor.useDb(self.database)
                Parse.newDB=False
            tableName = re.compile(r'update\s(.*)\sset',re.IGNORECASE).findall(self.query)
            raw_values=re.compile(r'set\s(.*)\swhere',re.IGNORECASE).findall(self.query)
            val=format(raw_values[0])
            val_list=re.split('=|,',val)
            update_values={}
            for x in range(0,len(val_list),2):
                column=val_list[x].lstrip().rstrip()
                clm_value=val_list[x+1].lstrip().rstrip()
                update_values[column]=clm_value.lstrip().rstrip()          
            #condition  
            condition_str=self.query[self.query.find('where'):]
            condition_column=re.compile(r'where\s*(.*)[=|>|<|<=|>=]\s*',re.IGNORECASE).findall(condition_str)
            condition_value=re.compile(r'\s*(=|>|<|<=|>=)\s*(.*).*',re.IGNORECASE).findall(condition_str)
            condition=self.formatCondition(condition_column,condition_value)

            table=tableName[0]
            table=table.lstrip().rstrip()
            if Parse.transcationFlag:
                if self.count==0:
                    self.count+=1
                    self.queryProcessor.startTransaction(self.database,table)
            self.queryProcessor.updateQuery(table,update_values,condition)
            self.log.pushLog(self.query,"Data successfully update in table "+table+" from database "+self.database)
        except IndexError as e:
            self.log.pushLog(self.query,"Error in select query syntax")
            print("Error in Query Syntax")   
        except Exception as e:
            self.log.pushLog(self.query,str(e))
            print(e)

    def drop(self):
        try:
            if "database" in self.query:
                database_drop = re.compile(r'drop database\s*(.*)\s*',re.IGNORECASE).findall(self.query)
                database=database_drop[0]
                database=database.lstrip().rstrip()
                self.queryProcessor.dropDb(database)
                self.log.pushLog(self.query,"Successfully dropped database "+self.database)
            elif "table" in self.query:
                if Parse.newDB:
                    self.queryProcessor.useDb(self.database)
                    Parse.newDB=False
                if self.database=="":
                    self.log.pushLog(self.query,"Database Not Selected") 
                    print("Database Not Selected")
                    return
                tableName=re.compile(r'drop table\s*(.*)\s*',re.IGNORECASE).findall(self.query)
                table=tableName[0]
                table=table.lstrip().rstrip()
                self.queryProcessor.useDb(self.database)
                self.queryProcessor.dropTable(table)
                self.log.pushLog(self.query,"Successfully dropped table "+table+" from database "+self.database)
        except IndexError as e:
            self.log.pushLog(self.query,"Error in select query syntax")
            print("Error in Query Syntax")
        except Exception as e:
            self.log.pushLog(self.query,str(e))
            print(e)

    def describe(self):
        try:
            if Parse.newDB:
                self.queryProcessor.useDb(self.database)
                Parse.newDB=False
            if "describe" in self.query.lower():
                tableName=re.compile(r'describe\s*(.*).*',re.IGNORECASE).findall(self.query)
            elif "desc" in self.query.lower():
                tableName=re.compile(r'desc\s*(.*).*',re.IGNORECASE).findall(self.query)
            table=tableName[0]
            table=table.lstrip().rstrip()
            self.queryProcessor.describeTable(table)
            self.log.pushLog(self.query,"Describe table "+table+" from database "+self.database)
        except IndexError as e:
            print(e)

    def format(self,raw_columns):
        columns = raw_columns.split(",")
        return columns

    # returns condition dictionary
    def formatCondition(self, column, condition_data):
        condition_op = condition_data[0][0]
        condition_val = condition_data[0][1]
        return {"columnName": column[0].lstrip().rstrip(), "operator": condition_op.lstrip().rstrip(), "value": condition_val.lstrip().rstrip()}

    def beginTransaction(self):

        print("\n--------------------------------------------------------")
        print("tranasction started")
        query = ""
        while not query.lower() == "quit":
            query=input()
            self.query=query
            val = self.check_query()
            if val == -1:
                print(colored("Incorrect Query",'red'))
            elif val == 0:
                break
        print("transaction ended")
        print("\n--------------------------------------------------------")

    def rollback(self):
        try:
            self.query=self.query.replace(";","")
            self.query=self.query.strip()
            check=self.query.split()
            if len(check)>2:
                savepoint=re.compile(r'rollback to\s(.*).*',re.IGNORECASE).findall(self.query)
                savePointName=savepoint[0]
                savePointName=savePointName.lstrip().rstrip()
                self.queryProcessor.rollback(savePointName)
                return 1
            elif len(check)==1:
                if check[0].lower()=='rollback':
                    self.queryProcessor.rollback()
                    return 0
                else:
                    raise Exception("Invalid Query")
        except IndexError as e:
            print("Error in Query Syntax")   
        except Exception as e:
            print(e)
    def dump(self):
        try:
            db=re.compile(r'dump\s(.*).*',re.IGNORECASE).findall(self.query)
            database=db[0]
            # print(database)
            database=database.lstrip().rstrip()
            self.queryProcessor.dump(database)
        except IndexError as e:
            print("Error in Query Syntax")   
        except Exception as e:
            print(e)
