import pandas as pd
import numpy as np
import sqlite3
import time
import threading
import sys

conn = sqlite3.connect("Data/Data.sqlite3")
cur = conn.cursor()
conn_full = sqlite3.connect("Data/Data_Full.sqlite3")
cur_full = conn_full.cursor()

count_lis = []
def run_query(query):
    conn_full = sqlite3.connect("Data/Data_Full.sqlite3")
    df = pd.read_sql(query, conn_full)
    # print(df.size)
    count_lis.append(df.size)

count_list = []
def start_process(year):
    p = threading.Thread(target=run_query, args=(r"SELECT * FROM Data_"+year+" indexed by imp_index_"+year+" where IMPORTER_NAME like '%mahle%'",))
    p.start()
    return p
start_time = time.time()

processes = []
for year in years:
    processes.append(start_process(year))
    print(year, "done")

for process in processes:
    process.join()

print("Time taken: ", time.time() - start_time)
print(sum(count_lis))



# p1 = threading.Thread(target=run_query, args=(r"SELECT * FROM Data_2021_Jan indexed by prod_index_2021_Jan where PRODUCT_DESCRIPTION like '%blower%'",))
# p2 = threading.Thread(target=run_query, args=(r"SELECT * FROM Data_2021_Feb indexed by prod_index_2021_Feb where PRODUCT_DESCRIPTION like '%blower%'",))
# p4 = threading.Thread(target=run_query, args=(r"SELECT * FROM Data_2021_Mar indexed by prod_index_2021_Mar where PRODUCT_DESCRIPTION like '%blower%'",))
# p3 = threading.Thread(target=run_query, args=(r"SELECT * FROM Data_2021_Apr indexed by prod_index_2021_Apr where PRODUCT_DESCRIPTION like '%blower%'",))
# p5 = threading.Thread(target=run_query, args=(r"SELECT * FROM Data_2021_May indexed by prod_index_2021_May where PRODUCT_DESCRIPTION like '%blower%'",))
# p6 = threading.Thread(target=run_query, args=(r"SELECT * FROM Data_2021_Jun indexed by prod_index_2021_Jun where PRODUCT_DESCRIPTION like '%blower%'",))
# p7 = threading.Thread(target=run_query, args=(r"SELECT * FROM Data_2021_Jul indexed by prod_index_2021_Jul where PRODUCT_DESCRIPTION like '%blower%'",))
# p8 = threading.Thread(target=run_query, args=(r"SELECT * FROM Data_2021_Aug indexed by prod_index_2021_Aug where PRODUCT_DESCRIPTION like '%blower%'",))
# p9 = threading.Thread(target=run_query, args=(r"SELECT * FROM Data_2021_Sep indexed by prod_index_2021_Sep where PRODUCT_DESCRIPTION like '%blower%'",))
# p10 = threading.Thread(target=run_query, args=(r"SELECT * FROM Data_2021_Oct indexed by prod_index_2021_Oct where PRODUCT_DESCRIPTION like '%blower%'",))
# p11 = threading.Thread(target=run_query, args=(r"SELECT * FROM Data_2021_Nov indexed by prod_index_2021_Nov where PRODUCT_DESCRIPTION like '%blower%'",))
# p12 = threading.Thread(target=run_query, args=(r"SELECT * FROM Data_2021_Dec indexed by prod_index_2021_Dec where PRODUCT_DESCRIPTION like '%blower%'",))

# p1.start()
# p2.start()
# p3.start()
# p4.start()
# p5.start()
# p6.start()
# p7.start()
# p8.start()
# p9.start()
# p10.start()
# p11.start()
# p12.start()

# start_time = time.time()

# p1.join()
# p2.join()
# p3.join()
# p4.join()
# p5.join()
# p6.join()
# p7.join()
# p8.join()
# p9.join()
# p10.join()
# p11.join()
# p12.join()

# print("Time taken: ", time.time() - start_time)

# print(sum(count_lis))