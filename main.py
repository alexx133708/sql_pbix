import pandas as pd
import pyodbc
import sqlalchemy as sqlalchemy
root = "E:\\bigdata\\original\\FExport2017_v.csv"

# engine = sqlalchemy.create_engine('mssql://GAME\\SQLEXPRESS/new_db?trusted_connection=yes')
engine = sqlalchemy.create_engine('mssql+pyodbc://GAME\\SQLEXPRESS/new_db?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')
conn = pyodbc.connect('Driver=SQL Server;'
                      'Server=GAME\SQLEXPRESS;'
                      'Database=new_db;'
                      'Trusted_Connection=yes;')

data = pd.read_csv(root, delimiter=';')
data.to_sql("FExport_2017", engine, if_exists='replace', index=False)
