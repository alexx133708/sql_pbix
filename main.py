import pandas as pd
import pyodbc
import sqlalchemy as sqlalchemy
root = "E:\\bigdata\\original\\DExportSKUs_index.csv"

connection = sqlalchemy.create_engine(url=f"mysql+mysqldb://alex3@192.168.1.75/sales", echo=False)
data = pd.read_csv(root, delimiter=';', on_bad_lines='skip', low_memory=False)

# data.loc[data['WeightUnit'] == ' ', 'WeightUnit'] = '0'
# data.loc[data['Weight'] == ' ', 'Weight'] = '0'
# data[['WeightUnit', 'Weight']] = data[['WeightUnit', 'Weight']].fillna(method="ffill").astype(float)
# m = ''
# for s in data['SKU'].tolist():
#     if len(s) > len(m):
#         m = s
# print(m)

data.to_sql("Products", connection, if_exists='replace', index=False)
