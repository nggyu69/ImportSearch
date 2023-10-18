import sqlite3
import time
import pandas as pd
import os
import sys


conn = sqlite3.connect('Data/Databases/Data2.sqlite3')
cur = conn.cursor()


cols = "BE_NO, BEDATE, HS_CODE, QUANTITY, UNIT, ASSESS_VALUE_INR, UNIT_PRICE_INR, ASSESS_VALUE_USD, UNIT_PRICE_USD, TOTAL_DUTY, TOTAL_DUTY_BE_WISE, APPLICABLE_DUTY_INR, EXCHANGE_RATE_USD, ITEM_RATE_INV_CURR, VALUE_INV_CURR, INVOICE_CURRENCY, ASSESS_GROUP, IMPORTER_CODE, IMPORTER_ADDRESS, IMPORTER_CITY, IMPORTER_PIN, IMPORTER_STATE, SUPPLIER_CODE, SUPPLIER_ADDRESS, SUPPLIER_COUNTRY, FOREIGN_PORT, FOREIGN_COUNTRY, FOREIGN_REGIONS, CHA_NAME, CHA_PAN, IEC, IEC_CODE, INVOICE_NUMBER, INVOICE_SR_NO, ITEM_NUMBER, HSCODE_2DIGIT, HSCODE_4DIGIT, TYPE, INDIAN_PORT, SHIPMENT_MODE, INDIAN_REGIONS, SHIPMENT_PORT, HSCODE_6DIGIT, BCD_NOTN, BCD_RATE, BCD_AMOUNT_INR, CVD_NOTN, CVD_RATE, CVD_AMOUNT_INR, IGST_AMOUNT_INR, GST_CESS_AMOUNT_INR, REMARK, INCOTERMS, TOTAL_FREIGHT_VALUE_FORGN_CUR, FREIGHT_CURRENCY, TOTAL_INSU_VALUE_FORGN_CUR, INSURANCE_CURRENCY, TOTAL_INVOICE_VALUE_INR, INSURANCE_VALUE_INR, TOTAL_GROSS_WEIGHT, TOTAL_FREIGHT_VALUE_INR, GROSS_WEIGHT_UNIT, CUSTOM_NOTIFICATION, STANDARD_QUANTITY, STANDARD_QUANTITY_UNIT, INDEX"

# cur.execute(f"create virtual table Data_2019_virt_joiner fts5({cols})")
# cur.execute(f"create virtual table Data_2019_virt_full using fts5({cols}, tokenize = 'trigram')")
# cur.execute("""insert into virt_table select PRODUCT_DESCRIPTION, SUPPLIER_NAME, IMPORTER_NAME, sl_no from Data_2019_index""")
# conn.commit()

start_time = time.time()
df = pd.read_sql("select * from Data_2021 where ROWID in (select ROWID from Data_2021_virt_searcher where PRODUCT_DESCRIPTION MATCH 'steel' and IMPORTER_NAME match 'electric')", conn)
# print(cur.execute("select count(*) from Data_2019_virt_searcher where PRODUCT_DESCRIPTION match 'blower'").fetchall())
# df = pd.read_sql("select * from Data_2019 indexed by prod_index_2019 where PRODUCT_DESCRIPTION like '%steel%' and IMPORTER_NAME like '%electric%'", conn)
# print(len(cur.execute("select * from Data_2019_index where PRODUCT_DESCRIPTION like '%controller%'").fetchall()))
print(df.columns)
print(len(df))
print("--- %s seconds ---" % (time.time() - start_time))
# cur.execute("drop table if exists Data_2019_virt_searcher")
# cur.execute(f"create virtual table if not exists Data_2019_virt_searcher using fts5(PRODUCT_DESCRIPTION, IMPORTER_NAME, SUPPLIER_NAME, tokenize = 'trigram')")
# conn.commit()
# # cur.execute("insert into Data_2019_virt_searcher select PRODUCT_DESCRIPTION, IMPORTER_NAME, SUPPLIER_NAME from Data_2019 limit 100000")
# count = 1
# for chunk in pd.read_sql("select PRODUCT_DESCRIPTION, IMPORTER_NAME, SUPPLIER_NAME from Data_2019", conn, chunksize=50000):
#     # cur.execute(f"insert into Data_2019_virt_searcher values {chunk}")
#     # insert into Data_2019_virt_searcher using values of chunk
#     chunk.to_sql("Data_2019_virt_searcher", conn, if_exists="append", index=False)
#     print("Done chunk", count)
#     count += 1
#     conn.commit()
