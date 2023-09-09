import pandas as pd
from elasticsearch import Elasticsearch
import numpy as np
import sqlite3
import time
import threading
import sys

es = Elasticsearch("http://localhost:9200")
conn = sqlite3.connect("Data/Data.sqlite3")
cur = conn.cursor()
conn_full = sqlite3.connect("Data/Data_Full.sqlite3")
cur_full = conn_full.cursor()
# cur_full.execute("Create index if not exists prod_index on Data_2021(PRODUCT_DESCRIPTION)")


# dtypes = {  "BE_NO" : np.float64, 
#             "BEDATE" : str, 
#             "HS_CODE" : np.float64, 
#             "PRODUCT_DESCRIPTION" : str,
#             "QUANTITY" : np.float64,
#             "UNIT" : str, 
#             "ASSESS_VALUE_INR" : np.float64,
#             "UNIT_PRICE_INR" : np.float64,
#             "ASSESS_VALUE_USD" : np.float64,
#             "UNIT_PRICE_USD" : np.float64,
#             "TOTAL_DUTY" : np.float64,
#             "TOTAL_DUTY_BE_WISE" : str,
#             "APPLICABLE_DUTY_INR" : np.float64,
#             "EXCHANGE_RATE_USD" : np.float64,
#             "ITEM_RATE_INV_CURR" : np.float64,
#             "VALUE_INV_CURR" : np.float64,
#             "INVOICE_CURRENCY" : str,
#             "ASSESS_GROUP" : str,
#             "IMPORTER_CODE" : np.float64,
#             "IMPORTER_NAME" : str,
#             "IMPORTER_ADDRESS" : str,
#             "IMPORTER_CITY" : str,
#             "IMPORTER_PIN" : np.float64,
#             "IMPORTER_STATE" : str,
#             "SUPPLIER_CODE" : str,
#             "SUPPLIER_NAME" : str,
#             "SUPPLIER_ADDRESS" : str,
#             "SUPPLIER_COUNTRY" : str,
#             "FOREIGN_PORT" : str,
#             "FOREIGN_COUNTRY" : str,
#             "FOREIGN_REGIONS" : str,
#             "CHA_NAME" : str,
#             "CHA_PAN" : str,
#             "IEC" : str,
#             "IEC_CODE" : np.float64,
#             "INVOICE_NUMBER" : str,
#             "INVOICE_SR_NO" : np.float64,
#             "ITEM_NUMBER" : np.float64,
#             "HSCODE_2DIGIT" : np.float64,
#             "HSCODE_4DIGIT" : np.float64,
#             "TYPE" : str,
#             "INDIAN_PORT" : str,
#             "SHIPMENT_MODE" : str,
#             "INDIAN_REGIONS" : str,
#             "SHIPMENT_PORT" : str,
#             "HSCODE_6DIGIT" : np.float64,
#             "BCD_NOTN" : str,
#             "BCD_RATE" : str,
#             "BCD_AMOUNT_INR" : str,
#             "CVD_NOTN" : str,
#             "CVD_RATE" : str,
#             "CVD_AMOUNT_INR" : str,
#             "IGST_AMOUNT_INR" : str,
#             "GST_CESS_AMOUNT_INR" : str,
#             "REMARK" : str,
#             "INCOTERMS" : str,
#             "TOTAL_FREIGHT_VALUE_FORGN_CUR" : str,
#             "FREIGHT_CURRENCY" : str,
#             "TOTAL_INSU_VALUE_ FORGN_CUR" : np.float64,
#             "INSURANCE_CURRENCY" : str,
#             "TOTAL_INVOICE_VALUE_INR" : np.float64,
#             "INSURANCE_VALUE_INR" : np.float64,
#             "TOTAL_GROSS_WEIGHT" : np.float64,
#             "TOTAL_FREIGHT_VALUE_INR" : np.float64,
#             "GROSS_WEIGHT_UNIT" : str,
#             "CUSTOM_NOTIFICATION" : str,
#             "STANDARD_QUANTITY" : np.float64,
#             "STANDARD_QUANTITY_UNIT" : str}

def check_num(val):
    try:
        if float(val):
            return np.float64(val)
    except ValueError as e:
        # string cannot be parsed as a number, return nan
        return np.nan
    
dtypes = {  "BEDATE" : str, 
            "PRODUCT_DESCRIPTION" : str,
            "UNIT" : str, 
            "TOTAL_DUTY_BE_WISE" : str,
            "INVOICE_CURRENCY" : str,
            "ASSESS_GROUP" : str,
            "IMPORTER_NAME" : str,
            "IMPORTER_ADDRESS" : str,
            "IMPORTER_CITY" : str,
            "IMPORTER_STATE" : str,
            "SUPPLIER_CODE" : str,
            "SUPPLIER_NAME" : str,
            "SUPPLIER_ADDRESS" : str,
            "SUPPLIER_COUNTRY" : str,
            "FOREIGN_PORT" : str,
            "FOREIGN_COUNTRY" : str,
            "FOREIGN_REGIONS" : str,
            "CHA_NAME" : str,
            "CHA_PAN" : str,
            "IEC" : str,
            "INVOICE_NUMBER" : str,
            "TYPE" : str,
            "INDIAN_PORT" : str,
            "SHIPMENT_MODE" : str,
            "INDIAN_REGIONS" : str,
            "SHIPMENT_PORT" : str,
            "BCD_NOTN" : str,
            "BCD_RATE" : str,
            "BCD_AMOUNT_INR" : str,
            "CVD_NOTN" : str,
            "CVD_RATE" : str,
            "CVD_AMOUNT_INR" : str,
            "IGST_AMOUNT_INR" : str,
            "GST_CESS_AMOUNT_INR" : str,
            "REMARK" : str,
            "INCOTERMS" : str,
            "TOTAL_FREIGHT_VALUE_FORGN_CUR" : str,
            "FREIGHT_CURRENCY" : str,
            "INSURANCE_CURRENCY" : str,
            "GROSS_WEIGHT_UNIT" : str,
            "CUSTOM_NOTIFICATION" : str,
            "STANDARD_QUANTITY_UNIT" : str}


converters={"BE_NO":check_num,
            "HS_CODE":check_num, 
            "QUANTITY":check_num, 
            "ASSESS_VALUE_INR":check_num,
            "UNIT_PRICE_INR":check_num,
            "ASSESS_VALUE_USD":check_num,
            "UNIT_PRICE_USD":check_num,
            "TOTAL_DUTY":check_num,
            "APPLICABLE_DUTY_INR":check_num,
            "EXCHANGE_RATE_USD":check_num,
            "ITEM_RATE_INV_CURR":check_num,
            "VALUE_INV_CURR":check_num,
            "IMPORTER_PIN":check_num,
            "IMPORTER_CODE":check_num,
            "IEC_CODE":check_num,
            "INVOICE_SR_NO":check_num,
            "ITEM_NUMBER":check_num,
            "HSCODE_2DIGIT":check_num,
            "HSCODE_4DIGIT":check_num,
            "HSCODE_6DIGIT":check_num,
            "TOTAL_INSU_VALUE_ FORGN_CUR":check_num,
            "TOTAL_INVOICE_VALUE_INR":check_num,
            "INSURANCE_VALUE_INR":check_num,
            "TOTAL_GROSS_WEIGHT":check_num,
            "TOTAL_FREIGHT_VALUE_INR":check_num,
            "STANDARD_QUANTITY":check_num}
count_lis = []
def run_query(query):
    conn_full = sqlite3.connect("Data/Data_Full.sqlite3")
    df = pd.read_sql(query, conn_full)
    # print(df.size)
    count_lis.append(df.size)
# start_time = time.time()
# months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
years = ["2018", "2019", "2020", "2021", "2022"]
# for year in years:
#     for month in months:
#         t = 1
#         for chunk in pd.read_csv("Data/"+year+"/"+year+"_"+month+"_Data.csv",dtype=dtypes,converters=converters, chunksize=50000):
#             # chunk.drop(chunk.columns[0], axis=1, inplace=True)
#             # for j,i in enumerate(chunk.dtypes):
#             #     print(chunk.columns[j], " : ", i)
#             # break 

#             chunk.to_sql("Data_"+year, conn_full, if_exists="append", index=False)
#             print(year,":", month, ":", t, "done")
#             t += 1

# print("Time taken: ", time.time() - start_time)
# df = pd.read_sql("SELECT * FROM Data_2023 where BEDATE between '2023-01-01' and '2023-01-31'", conn)
# df.to_sql("Data_2023_Jan", conn, if_exists="replace", index=False)

# df = pd.read_sql("SELECT * FROM Data_2023 where BEDATE between '2023-02-01' and '2023-02-31'", conn)
# df.to_sql("Data_2023_Feb", conn, if_exists="replace", index=False)

# df = pd.read_sql("SELECT * FROM Data_2023 where BEDATE between '2023-03-01' and '2023-03-31'", conn)
# df.to_sql("Data_2023_Mar", conn, if_exists="replace", index=False)

# df = pd.read_sql("SELECT * FROM Data_2023 where BEDATE between '2023-04-01' and '2023-04-31'", conn)
# df.to_sql("Data_2023_Apr", conn, if_exists="replace", index=False)

# df = pd.read_sql("SELECT * FROM Data_2023 where BEDATE between '2023-05-01' and '2023-05-31'", conn)
# df.to_sql("Data_2023_May", conn, if_exists="replace", index=False)

# start_time = time.time()
# print(pd.read_sql(r"SELECT count(*) FROM Data_2023 where BEDATE between '2023-03-01' and '2023-04-31'", conn))
# print("Time taken: ", time.time() - start_time)

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