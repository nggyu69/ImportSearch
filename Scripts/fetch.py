##To use only when on IFB network to access the server database

import pyodbc
import pandas as pd
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import time
import sys

server = 'MDSERVER' 
database = 'Basic' 
username = 'sa' 
password = 'Motordivision@123'
connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
# conn_string = 'DRIVER={/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.0.so.1.1};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password
# cnxn = pyodbc.connect('DRIVER={DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
conn = pyodbc.connect(connectionString) 
sys.exit()
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_string})
engine = create_engine(connection_url)

curx = cnxn.cursor()
start_time = time.time()

with engine.begin() as conn:
    # t = 1
    # for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-01-01' and '2023-01-31'", conn, chunksize = 50000):
    #     chunk_dataframe.to_csv('2023/2023_Jan_Data.csv', mode = 'a', index = False)
    #     print("Time to execute 2023 Jan " + str(t) +" : ", time.time() - start_time)
    #     t+=1
    #     start_time = time.time()
        
    # t = 1
    # for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-02-01' and '2023-02-28'", conn, chunksize = 50000):
    #     chunk_dataframe.to_csv('2023/2023_Feb_Data.csv', mode = 'a', index = False)
    #     print("Time to execute 2023 Feb " + str(t) +" : ", time.time() - start_time)
    #     t+=1
    #     start_time = time.time()

    # t = 1
    # for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-03-01' and '2023-03-31'", conn, chunksize = 50000):
    #     chunk_dataframe.to_csv('2023/2023_Mar_Data.csv', mode = 'a', index = False)
    #     print("Time to execute 2023 Mar " + str(t) +" : ", time.time() - start_time)
    #     t+=1
    #     start_time = time.time()

    # t = 1
    # for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-04-01' and '2023-04-30'", conn, chunksize = 50000):
    #     chunk_dataframe.to_csv('2023/2023_Apr_Data.csv', mode = 'a', index = False)
    #     print("Time to execute 2023 Apr " + str(t) +" : ", time.time() - start_time)
    #     t+=1
    #     start_time = time.time()
        
    # t = 1
    # for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-05-01' and '2023-05-31'", conn, chunksize = 50000):
    #     chunk_dataframe.to_csv('2023/2023_May_Data.csv', mode = 'a', index = False)
    #     print("Time to execute 2023 May " + str(t) +" : ", time.time() - start_time)
    #     t+=1
    #     start_time = time.time()
        
    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-06-01' and '2023-06-30'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2023/2023_Jun_Data.csv', mode = 'a', index = False)
        print("Time to execute 2023 Jun " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
        
    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-07-01' and '2023-07-31'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2023/2023_Jul_Data.csv', mode = 'a', index = False)
        print("Time to execute 2023 Jul " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
    
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-08-01' and '2023-08-31'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2023/2023_Aug_Data.csv', mode = 'a', index = False)
        print("Time to execute 2023 Aug " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
    
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-09-01' and '2023-09-30'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2023/2023_Sep_Data.csv', mode = 'a', index = False)
        print("Time to execute 2023 Sep " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()

    # t = 1
    # for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-10-01' and '2023-10-31'", conn, chunksize = 50000):
    #     chunk_dataframe.to_csv('2023/2023_Oct_Data.csv', mode = 'a', index = False)
    #     print("Time to execute 2023 Oct " + str(t) +" : ", time.time() - start_time)
    #     t+=1
    #     start_time = time.time()

    # t = 1
    # for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-11-01' and '2023-11-30'", conn, chunksize = 50000):
    #     chunk_dataframe.to_csv('2023/2023_Nov_Data.csv', mode = 'a', index = False)
    #     print("Time to execute 2023 Nov " + str(t) +" : ", time.time() - start_time)
    #     t+=1
    #     start_time = time.time()

    # t = 1
    # for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-12-01' and '2023-12-31'", conn, chunksize = 50000):
    #     chunk_dataframe.to_csv('2023/2023_Dec_Data.csv', mode = 'a', index = False)
    #     print("Time to execute 2023 Dec " + str(t) +" : ", time.time() - start_time)
    #     t+=1
    #     start_time = time.time()
