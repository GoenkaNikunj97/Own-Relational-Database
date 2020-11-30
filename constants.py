InputQuery="Enter the SQL Query\n"
select="select"
update="update"
delete="delete"
create="create"
insert="insert"
drop="drop"

#select
all_columns="*"
where_clause="where"
select_column_RE=r'select\s(.*)\sfrom'
select_table_condition_RE=r'from\s(.*)\swhere'
select_table_no_condition_RE=r'from\s(.*)\s*'
select_condition_column_RE=r'where\s(.*)[=|>|<|<=|>=]'
select_condition_value_RE=r'(=|>|<|<=|>=)(.*)'

#delete
delete_table_RE=r'from\s(.*)\swhere'
delete_condition_column_RE=r'where\s(.*)[=|>|<|<>|<=|>=]'
delete_condition_value_RE=r'(=|>|<|<=|>=)(.*)'

#insert
insert_table_RE=r'insert into\s*(.*)\s*\('
insert_columns_RE=r'\((.*?)\)\svalues'
insert_values_RE=r'values\s\((.*)\).*'

#errors
db_missing ="Database not selected"