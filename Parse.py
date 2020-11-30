import constants
import re
import QueryProcessor as qp
import Logging as logging

class Parse:
    log=logging.Logging()
    queryProcessor = qp.QueryProcessor()    
    def __init__(self,database, query):
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
        elif "quit" in self.query.lower():
            return 0
        elif "use" in self.query.lower():
            return
        else:
            return -1

    def select(self):
        try:
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
                self.queryProcessor.useDb(self.database)
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
                self.queryProcessor.useDb(self.database)
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
            self.queryProcessor.useDb(self.database)
            if constants.where_clause in self.query:
                tableName = re.compile(constants.delete_table_RE,re.IGNORECASE).findall(self.query)
                condition_column = re.compile(
                    constants.delete_condition_column_RE, re.IGNORECASE).findall(self.query)
                condition_value = re.compile(
                    constants.delete_condition_value_RE, re.IGNORECASE).findall(self.query)
                condition = self.formatCondition(condition_column, condition_value)
                table=tableName[0]
                table=table.lstrip().rstrip()
                self.queryProcessor.deleteQuery(table, condition)
                self.log.pushLog(self.query,"Data deleted from table "+table+" of database " +self.database)
            elif constants.where_clause not in self.query:
                tableName= re.compile(r'from\s(.*)\s*',re.IGNORECASE).findall(self.query)
                table=tableName[0]
                table=table.lstrip().rstrip()
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
            table = re.compile(constants.insert_table_RE,re.IGNORECASE).findall(self.query)
            table = table[0]
            table = table[:table.find('(')]
            table=table.lstrip().rstrip()
            raw_columns = re.compile(constants.insert_columns_RE,re.IGNORECASE).findall(self.query)
            raw_values = re.compile(constants.insert_values_RE,re.IGNORECASE).findall(self.query)
            values=raw_values[0].split(",")
            columns=raw_columns[0].split(",")
            self.queryProcessor.useDb(self.database)
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
                self.queryProcessor.useDb(self.database)
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
            tableName = re.compile(r'update\s(.*)\sset',re.IGNORECASE).findall(self.query)
            raw_values=re.compile(r'set\s(.*)\swhere',re.IGNORECASE).findall(self.query)
            val=format(raw_values[0])
            val_list=re.split('=|,',val)
            update_values={}
            for x in range(0,len(val_list),2):
                column=val_list[x]
                clm_value=val_list[x+1]
                update_values[column]=clm_value
            print(update_values)
            
            #condition  
            condition_str=self.query[self.query.find('where'):]
            condition_column=re.compile(r'where\s*(.*)[=|>|<|<=|>=]\s*',re.IGNORECASE).findall(condition_str)
            condition_value=re.compile(r'\s*(=|>|<|<=|>=)\s*(.*).*',re.IGNORECASE).findall(condition_str)
            condition=self.formatCondition(condition_column,condition_value)

            print(condition)
            table=tableName[0]
            table=table.lstrip().rstrip()
            self.queryProcessor.useDb(self.database)
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
                self.log.pushLog(self.query,"Successfully dropped database "+self.database)
            elif "table" in self.query:
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
    def format(self,raw_columns):
        columns = raw_columns.split(",")
        return columns

    # returns condition dictionary
    def formatCondition(self, column, condition_data):
        condition_op = condition_data[0][0]
        condition_val = condition_data[0][1]
        return {"columnName": column[0], "operator": condition_op, "value": condition_val}
