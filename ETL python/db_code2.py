import sqlite3
import pandas as pd 

conn = sqlite3.connect('DEPARMENTS.db')


table_name = "Departments"
attribute_list = ['DEPT_ID','DEP_NAME','MANAGER_ID','LOC_ID']


file_path = '/home/project/practice/Departments.csv'
df = pd.read_csv(file_path, names = attribute_list)

df.to_sql(table_name, conn, if_exists = 'replace', index = False)
print("Table is ready")


query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT DEPT_ID FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

data_dict = {
    "DEPT_ID": [9],
    "DEP_NAME" : ['Quality Assurance'],
    'MANAGER_ID' : ['30010'],
    'LOC_ID' : ['L0010'],
}

data_append = pd.DataFrame(data_dict)

data_append.to_sql(table_name, conn, if_exists = 'append', index = False)
print("Data appended Successfully")

query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

conn.close()
