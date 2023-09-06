import pyodbc
import pandas as pd
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import time

server = 'MDSERVER' 
database = 'Basic' 
username = 'sa' 
password = 'Motordivision@123'
conn_string = 'DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_string})
engine = create_engine(connection_url)

curx = cnxn.cursor()
start_time = time.time()
t = 1
with engine.begin() as conn:
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-01-01' and '2023-01-02'", conn, chunksize = 50000):

        chunk_dataframe.to_csv('2023_Jan_Data.csv', mode = 'a', index = True)
        print("Time to execute " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
