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
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2020-01-01' and '2020-01-31'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2020/2020_Jan_Data.csv', mode = 'a', index = False)
        print("Time to execute 2020 Jan " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
        
    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2020-02-01' and '2020-02-28'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2020/2020_Feb_Data.csv', mode = 'a', index = False)
        print("Time to execute 2020 Feb " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()

    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2020-03-01' and '2020-03-31'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2020/2020_Mar_Data.csv', mode = 'a', index = False)
        print("Time to execute 2020 Mar " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()

    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2020-04-01' and '2020-04-30'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2020/2020_Apr_Data.csv', mode = 'a', index = False)
        print("Time to execute 2020 Apr " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
        
    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2020-05-01' and '2020-05-31'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2020/2020_May_Data.csv', mode = 'a', index = False)
        print("Time to execute 2020 May " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
        
    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2020-06-01' and '2020-06-30'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2020/2020_Jun_Data.csv', mode = 'a', index = False)
        print("Time to execute 2020 Jun " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
        
    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2020-07-01' and '2020-07-31'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2020/2020_Jul_Data.csv', mode = 'a', index = False)
        print("Time to execute 2020 Jul " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
    
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2020-08-01' and '2020-08-31'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2020/2020_Aug_Data.csv', mode = 'a', index = False)
        print("Time to execute 2020 Aug " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
    
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2020-09-01' and '2020-09-30'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2020/2020_Sep_Data.csv', mode = 'a', index = False)
        print("Time to execute 2020 Sep " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()

    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2020-10-01' and '2020-10-31'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2020/2020_Oct_Data.csv', mode = 'a', index = False)
        print("Time to execute 2020 Oct " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()

    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2020-11-01' and '2020-11-30'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2020/2020_Nov_Data.csv', mode = 'a', index = False)
        print("Time to execute 2020 Nov " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()

    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2020-12-01' and '2020-12-31'", conn, chunksize = 50000):
        chunk_dataframe.to_csv('2020/2020_Dec_Data.csv', mode = 'a', index = False)
        print("Time to execute 2020 Dec " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
