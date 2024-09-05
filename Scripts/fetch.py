##To use only when on IFB network to access the server database

import pyodbc
import pandas as pd
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import time
import os
import sys

server = 'MDSERVER' 
database = 'Basic' 
username = 'sa' 
password = 'Motordivision@123'
conn_string = 'DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_string})
engine = create_engine(connection_url)
curx = cnxn.cursor()
curx.execute("select count(BEDATE) from importdata_A where BEDATE between '2023-08-01' and '2023-08-30'")
print(curx.fetchall())
start_time = time.time()

os.chdir("C:\ImportData\ImportSearch\Data\Excel_Files")
with engine.begin() as conn:
    print("Starting engine")
    # t = 1
    # for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-01-01' and '2023-01-31'", conn, chunksize = 50000):
    #     os.makedirs("2023/2023-01", exist_ok=True)
    #     chunk_dataframe.to_csv('2023/2023-01/2023-01_0.csv', mode = 'a', index = False)
    #     print("Time to execute 2023 Jan " + str(t) +" : ", time.time() - start_time)
    #     t+=1
    #     start_time = time.time()
        
    # t = 1
    # for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-02-01' and '2023-02-28'", conn, chunksize = 50000):
    #     os.makedirs("2023/2023-02", exist_ok=True)
    #     chunk_dataframe.to_csv('2023/2023-02/2023-02_0.csv', mode = 'a', index = False)        print("Time to execute 2023 Feb " + str(t) +" : ", time.time() - start_time)
    #     t+=1
    #     start_time = time.time()

    # t = 1
    # for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-03-01' and '2023-03-31'", conn, chunksize = 50000):
    #     os.makedirs("2023/2023-03", exist_ok=True)
    #     chunk_dataframe.to_csv('2023/2023-03/2023-03_0.csv', mode = 'a', index = False)
    #     print("Time to execute 2023 Mar " + str(t) +" : ", time.time() - start_time)
    #     t+=1
    #     start_time = time.time()

    # t = 1
    # for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-04-01' and '2023-04-30'", conn, chunksize = 50000):
    #     os.makedirs("2023/2023-04", exist_ok=True)
    #     chunk_dataframe.to_csv('2023/2023-04/2023-04_0.csv', mode = 'a', index = False)
    #     print("Time to execute 2023 Apr " + str(t) +" : ", time.time() - start_time)
    #     t+=1
    #     start_time = time.time()
        
    # t = 1
    # for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-05-01' and '2023-05-31'", conn, chunksize = 50000):
    #     os.makedirs("2023/2023-05", exist_ok=True)
    #     chunk_dataframe.to_csv('2023/2023-05/2023-05_0.csv', mode = 'a', index = False)
    #     print("Time to execute 2023 May " + str(t) +" : ", time.time() - start_time)
    #     t+=1
    #     start_time = time.time()
        
    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-06-01' and '2023-06-30'", conn, chunksize = 50000):
        os.makedirs("2023/2023-06", exist_ok=True)
        chunk_dataframe.to_csv('2023/2023-06/2023-06_0.csv', mode = 'a', index = False)
        print("Time to execute 2023 Jun " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
        
    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-07-01' and '2023-07-31'", conn, chunksize = 50000):
        os.makedirs("2023/2023-07", exist_ok=True)
        chunk_dataframe.to_csv('2023/2023-07/2023-07_0.csv', mode = 'a', index = False)
        print("Time to execute 2023 Jul " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
    
    t = 1
    for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-08-01' and '2023-08-31'", conn, chunksize = 50000):
        os.makedirs("2023/2023-08", exist_ok=True)
        chunk_dataframe.to_csv('2023/2023-08/2023-08_0.csv', mode = 'a', index = False)
        print("Time to execute 2023 Aug " + str(t) +" : ", time.time() - start_time)
        t+=1
        start_time = time.time()
    
    # t = 1
    # for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-09-01' and '2023-09-30'", conn, chunksize = 50000):
    #     os.makedirs("2023/2023-09", exist_ok=True)
    #     chunk_dataframe.to_csv('2023/2023-09/2023-09_0.csv', mode = 'a', index = False)
    #     print("Time to execute 2023 Sep " + str(t) +" : ", time.time() - start_time)
    #     t+=1
    #     start_time = time.time()

    # t = 1
    # for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-10-01' and '2023-10-31'", conn, chunksize = 50000):
    #     os.makedirs("2023/2023-10", exist_ok=True)
    #     chunk_dataframe.to_csv('2023/2023-10/2023-10_0.csv', mode = 'a', index = False)
    #     print("Time to execute 2023 Oct " + str(t) +" : ", time.time() - start_time)
    #     t+=1
    #     start_time = time.time()

    # t = 1
    # for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-11-01' and '2023-11-30'", conn, chunksize = 50000):
    #     os.makedirs("2023/2023-11", exist_ok=True)
    #     chunk_dataframe.to_csv('2023/2023-11/2023-11_0.csv', mode = 'a', index = False)
    #     print("Time to execute 2023 Nov " + str(t) +" : ", time.time() - start_time)
    #     t+=1
    #     start_time = time.time()

    # t = 1
    # for chunk_dataframe in pd.read_sql_query("select * from importdata_A where BEDATE between '2023-12-01' and '2023-12-31'", conn, chunksize = 50000):
    #     os.makedirs("2023/2023-12", exist_ok=True)
    #     chunk_dataframe.to_csv('2023/2023-12/2023-12_0.csv', mode = 'a', index = False)
    #     print("Time to execute 2023 Dec " + str(t) +" : ", time.time() - start_time)
    #     t+=1
    #     start_time = time.time()
