import constants
import re
import collections


class Parse:
    def __init__(self, query):
        self.query = query.lower()

    def check_query(self):
        if constants.select in self.query.lower():
            self.select()
        elif constants.delete in self.query.lower():
            self.delete()
        elif constants.insert in self.query.lower():
            self.insert()
        elif constants.create in self.query.lower():
            self.create()
        elif constants.update in self.query.lower():
            self.update()
        elif constants.drop in self.query.lower():
            self.drop()
        elif "quit" in self.query.lower():
            return
        else:
            return -1

    def select(self):
        try:
            condition_column = ''
            condition_value = ''
            # with condition

            if constants.where_clause in self.query:
                raw_columns = re.compile(
                    constants.select_column_RE, re.DOTALL).findall(self.query)
                columns = self.format(raw_columns[0])

                table = re.compile(
                    constants.select_table_condition_RE, re.DOTALL).findall(self.query)

                condition_column = re.compile(
                    constants.select_condition_column_RE, re.DOTALL).findall(self.query)
                condition_value = re.compile(
                    constants.select_condition_value_RE, re.DOTALL).findall(self.query)

                condition = self.formatCondition(
                    condition_column, condition_value)

            #########
            # return according to need NIKUNJ
            #########

            # without condition
            else:
                raw_columns = re.compile(
                    constants.select_column_RE, re.DOTALL).findall(self.query)
                columns = self.format(raw_columns[0])

                table = re.compile(
                    constants.select_table_no_condition_RE, re.DOTALL).findall(self.query)

            #########
            # return according to need NIKUNJ
            #########
        except:
            # to write
            print("incorrect query")

    def delete(self):
        try:
            table = re.compile(constants.delete_table_RE).findall(self.query)
            condition_column = re.compile(
                constants.delete_condition_column_RE, re.DOTALL).findall(self.query)
            condition_value = re.compile(
                constants.delete_condition_value_RE, re.DOTALL).findall(self.query)
            print(table)
            condition = self.formatCondition(condition_column, condition_value)
            print(condition)
            #########
            # return according to need NIKUNJ
            #########
        except:
            # to write
            print("incorrect query")

    def insert(self):

        table = re.compile(constants.insert_table_RE).findall(self.query)
        table = table[0]
        table = table[:table.find('(')]
        raw_columns = re.compile(
            constants.insert_columns_RE).findall(self.query)
        raw_values = re.compile(constants.insert_values_RE).findall(self.query)
        values=raw_values[0].split(",")
        columns=raw_columns[0].split(",")
        print(values)
        print(columns)
        print(table)
        #########
        # return according to need NIKUNJ
        #########

    def create(self):
        if "database" in self.query:
            database = re.compile(r'create database\s(.*)\s*').findall(self.query)
            print(database)
        elif "table" in self.query:
            table = re.compile(r'create table\s(.*)\s*').findall(self.query)
            print(table)
            raw_values=re.compile(r'\((.*)\)').findall(self.query)
            value_list=raw_values[0].split(",")
            insert_values={}
            for val in value_list:
                temp=val.split()
                column=str(temp[0])
                clm_type=str(temp[1])
                insert_values[column]=clm_type
            print(insert_values)
            #########
            #return according to need NIKUNJ
            #########

    def update(self):
        table = re.compile(r'update\s(.*)\sset').findall(self.query)
        print(table)

        raw_values=re.compile(r'set\s(.*)\swhere').findall(self.query)
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
        condition_column=re.compile(r'where\s(.*)[=|>|<|<=|>=]\s').findall(condition_str)
        condition_value=re.compile(r'\s(=|>|<|<=|>=)\s(.*)').findall(condition_str)
        condition=self.formatCondition(condition_column,condition_value)
        print(condition)
        #########
        # return according to need NIKUNJ
        #########
    def drop(self):
        if "database" in self.query:
            database = re.compile(r'drop database\s(.*)\s*').findall(self.query)
            print(database)
        elif "table" in self.query:
            table=re.compile(r'drop table\s(.*)\s*').findall(self.query)
            print(table)

    def format(self,raw_columns):
        columns = raw_columns.split(",")
        return columns

    # returns condition dictionary
    def formatCondition(self, column, condition_data):
        condition_op = condition_data[0][0]
        condition_val = condition_data[0][1]
        return {'condition_col': column[0], 'condition_op': condition_op, 'condition_val': condition_val}
