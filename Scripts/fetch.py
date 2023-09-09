##To use only when on IFB network to access the server database

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

with engine.begin() as conn:
    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2022-01-01' and '2022-01-31'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2022/2022_Jan_Data.csv', mode = 'a', index = False)
        print("Time to execute 2022 Jan " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
        
    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2022-02-01' and '2022-02-28'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2022/2022_Feb_Data.csv', mode = 'a', index = False)
        print("Time to execute 2022 Feb " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()

    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2022-03-01' and '2022-03-31'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2022/2022_Mar_Data.csv', mode = 'a', index = False)
        print("Time to execute 2022 Mar " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()

    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2022-04-01' and '2022-04-30'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2022/2022_Apr_Data.csv', mode = 'a', index = False)
        print("Time to execute 2022 Apr " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
        
    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2022-05-01' and '2022-05-31'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2022/2022_May_Data.csv', mode = 'a', index = False)
        print("Time to execute 2022 May " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
        
    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2022-06-01' and '2022-06-30'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2022/2022_Jun_Data.csv', mode = 'a', index = False)
        print("Time to execute 2022 Jun " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
        
    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2022-07-01' and '2022-07-31'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2022/2022_Jul_Data.csv', mode = 'a', index = False)
        print("Time to execute 2022 Jul " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
    
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2022-08-01' and '2022-08-31'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2022/2022_Aug_Data.csv', mode = 'a', index = False)
        print("Time to execute 2022 Aug " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
    
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2022-09-01' and '2022-09-30'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2022/2022_Sep_Data.csv', mode = 'a', index = False)
        print("Time to execute 2022 Sep " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()

    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2022-10-01' and '2022-10-31'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2022/2022_Oct_Data.csv', mode = 'a', index = False)
        print("Time to execute 2022 Oct " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()

    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2022-11-01' and '2022-11-30'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2022/2022_Nov_Data.csv', mode = 'a', index = False)
        print("Time to execute 2022 Nov " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()

    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2022-12-01' and '2022-12-31'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2022/2022_Dec_Data.csv', mode = 'a', index = False)
        print("Time to execute 2022 Dec " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
