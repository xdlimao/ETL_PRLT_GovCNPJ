import dask.dataframe as dd
import pandas as pd
import pyodbc
import json
import os

with open('config.json', encoding="utf-8") as f: 
    data = json.load(f)

table_name = data["table_name"]
path = data["path"]
SERVER = data["server_address"]
DATABASE = data["database"]
fieldquote = data["fieldquote"]
fieldterminator = data["fieldterminator"]
batch_activate= data["batch_activate"]
batchsize = data["batch_size"]

# print(table_name)
# print(path)
# print(SERVER)
# print(DATABASE)
# print(fieldquote)
# print(rowterminator)

# USERNAME = 'peralta'
# PASSWORD = '123456'

# #Read csv
print("reading columns...")
dfp = pd.read_csv(path, header=None, sep=";", encoding='latin-1' ,nrows=1)
columnlength = dfp.shape[1]


"""
print(dfp.to_string())
print(dfp)

dfd = dd.read_csv('datatest.csv', header=None, sep=";", encoding='latin-1', dtype={8:'object'})
print(dfd.compute())
"""
print("read succefully")

#Credentials and connection
print("connecting database...")
connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;TrustServerCertificate=yes;'
conn = pyodbc.connect(connectionString)

#Operate DB
print("connected succefully")
cursor = conn.cursor()

#Create a temp-table
print(f"creating temporary table on the name: {table_name}...")
i = 1
temp_table_querymaker = f"CREATE TABLE {table_name} ("
while i <= columnlength:
    if i != columnlength:
        temp_table_querymaker = temp_table_querymaker + f"column{i} varchar(max),"
    else:
        temp_table_querymaker = temp_table_querymaker + f"column{i} varchar(max))"
    i = i + 1
cursor.execute(temp_table_querymaker)
# conn.commit()
print(f"created the table {table_name} succefully")

#Bulk Insert
print("inserting all data...")

def bulk_insert_batch(initrows, batchsizes):
    cursor.execute(f"""
        BULK INSERT {table_name}
        FROM '{path}'
        WITH (
            FORMAT='CSV',
            FIELDQUOTE = '{fieldquote}',
            ROWTERMINATOR = '0x0A',       
            FIELDTERMINATOR = '{fieldterminator}',
            FIRSTROW = {initrows},
            LASTROW = {initrows+batchsizes-1}
        )
        """)
    conn.commit()

def has_rows(index) :
    try:
        dataframe = pd.read_csv(path, header=None, sep=";", encoding='latin-1' , nrows=batchsize, skiprows= batchsize * index)
        return 1
    except:
        return 0

index = 0
initrow = 1
if batch_activate == True:
    print("(batch mode activated)")
    while has_rows(index) == 1:
        bulk_insert_batch(initrow, batchsize)
        index = index + 1
        initrow = initrow + batchsize
        print(f"inserted {index} loot")
else:
    cursor.execute(f"""
    BULK INSERT {table_name}
    FROM '{path}'
    WITH (
        FORMAT='CSV',
        FIELDQUOTE = '{fieldquote}',
        ROWTERMINATOR = '0x0A',       
        FIELDTERMINATOR = '{fieldterminator}'
    )
    """)
    conn.commit()

print("inserted all data")





