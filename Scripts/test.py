import pandas as pd
import sqlite3
import time

conn = sqlite3.connect(f"Data/Databases/Data2.sqlite3")

start_time = time.time()
df = pd.read_sql("select * from Data_2019 where ROWID in ( select ROWID from Data_2019_virt_searcher where PRODUCT_DESCRIPTION MATCH 'CONDUCTOR') ", conn)
print(time.time() - start_time)