# import sqlite3
# import time
# import pandas as pd
# import os
# import sys
# from multiprocessing import Process

# # years = os.listdir("Data/Excel_Files")
# # for i in years:
# #     for j in os.listdir(f"Data/Excel_Files/{i}"):
# #         for k in os.listdir(f"Data/Excel_Files/{i}/{j}/"):
# #             if k.endswith(".csv"):
# #                 os.remove(f"Data/Excel_Files/{i}/{j}/{k}")
# # sys.exit()
# conn = sqlite3.connect('Data/Databases/Data.sqlite3')
# cur = conn.cursor()


# cols = "BE_NO, BEDATE, HS_CODE, QUANTITY, UNIT, ASSESS_VALUE_INR, UNIT_PRICE_INR, ASSESS_VALUE_USD, UNIT_PRICE_USD, TOTAL_DUTY, TOTAL_DUTY_BE_WISE, APPLICABLE_DUTY_INR, EXCHANGE_RATE_USD, ITEM_RATE_INV_CURR, VALUE_INV_CURR, INVOICE_CURRENCY, ASSESS_GROUP, IMPORTER_CODE, IMPORTER_ADDRESS, IMPORTER_CITY, IMPORTER_PIN, IMPORTER_STATE, SUPPLIER_CODE, SUPPLIER_ADDRESS, SUPPLIER_COUNTRY, FOREIGN_PORT, FOREIGN_COUNTRY, FOREIGN_REGIONS, CHA_NAME, CHA_PAN, IEC, IEC_CODE, INVOICE_NUMBER, INVOICE_SR_NO, ITEM_NUMBER, HSCODE_2DIGIT, HSCODE_4DIGIT, TYPE, INDIAN_PORT, SHIPMENT_MODE, INDIAN_REGIONS, SHIPMENT_PORT, HSCODE_6DIGIT, BCD_NOTN, BCD_RATE, BCD_AMOUNT_INR, CVD_NOTN, CVD_RATE, CVD_AMOUNT_INR, IGST_AMOUNT_INR, GST_CESS_AMOUNT_INR, REMARK, INCOTERMS, TOTAL_FREIGHT_VALUE_FORGN_CUR, FREIGHT_CURRENCY, TOTAL_INSU_VALUE_FORGN_CUR, INSURANCE_CURRENCY, TOTAL_INVOICE_VALUE_INR, INSURANCE_VALUE_INR, TOTAL_GROSS_WEIGHT, TOTAL_FREIGHT_VALUE_INR, GROSS_WEIGHT_UNIT, CUSTOM_NOTIFICATION, STANDARD_QUANTITY, STANDARD_QUANTITY_UNIT, INDEX"

# # cur.execute(f"create virtual table Data_2019_virt_joiner fts5({cols})")
# # cur.execute(f"create virtual table Data_2019_virt_full using fts5({cols}, tokenize = 'trigram')")
# # cur.execute("""insert into virt_table select PRODUCT_DESCRIPTION, SUPPLIER_NAME, IMPORTER_NAME, sl_no from Data_2019_index""")
# # conn.commit()

# # start_time = time.time()
# # df = pd.read_sql("select * from Data_2019 where ROWID in (select ROWID from Data_2019_virt_searcher where PRODUCT_DESCRIPTION MATCH 'steel')", conn)
# # print(cur.execute("select count(*) from Data_2019_virt_searcher where PRODUCT_DESCRIPTION match 'blower'").fetchall())
# # df = pd.read_sql("select * from Data_2019 indexed by prod_index_2019 where PRODUCT_DESCRIPTION like '%steel%'", conn)
# # print(len(cur.execute("select * from Data_2019_index where PRODUCT_DESCRIPTION like '%controller%'").fetchall()))
# # print(len(df))
# # print("--- %s seconds ---" % (time.time() - start_time))
# # cur.execute("drop table if exists Data_2019_virt_searcher")
# # cur.execute(f"create virtual table if not exists Data_2019_virt_searcher using fts5(PRODUCT_DESCRIPTION, IMPORTER_NAME, SUPPLIER_NAME, tokenize = 'trigram')")
# # conn.commit()
# # # cur.execute("insert into Data_2019_virt_searcher select PRODUCT_DESCRIPTION, IMPORTER_NAME, SUPPLIER_NAME from Data_2019 limit 100000")
# # count = 1
# # for chunk in pd.read_sql("select PRODUCT_DESCRIPTION, IMPORTER_NAME, SUPPLIER_NAME from Data_2019", conn, chunksize=50000):
# #     # cur.execute(f"insert into Data_2019_virt_searcher values {chunk}")
# #     # insert into Data_2019_virt_searcher using values of chunk
# #     chunk.to_sql("Data_2019_virt_searcher", conn, if_exists="append", index=False)
# #     print("Done chunk", count)
# #     count += 1
# #     conn.commit()

# def run_query(query, num):
#     conn = sqlite3.connect(f"Data/Databases/Data{num}.sqlite3")

#     start_time = time.time()
#     df = pd.read_sql(query, conn)
#     print(len(df))
#     print(num)
#     print("--- %s seconds ---" % (time.time() - start_time))

# def start_process(query, num):
#     p = Process(target=run_query, args=(query, num))
#     p.start()
#     return p

# start_process("select * from Data_2019 where ROWID in (select ROWID from Data_2019_virt_searcher where PRODUCT_DESCRIPTION MATCH 'blower')", "")
# start_process("select * from Data_2019 indexed by prod_index_2019 where PRODUCT_DESCRIPTION like '%blower%'", "")



#######################################################################
import pandas as pd
import numpy as np
import sqlite3
import time
import threading
from multiprocessing import Process, Manager
import sys
import os
import shutil


# os.chdir("..")
conn = sqlite3.connect("Data/Databases/Data.sqlite3")
cur = conn.cursor()

conn_progress_2018 = sqlite3.connect("Data/Databases/Progress_2018.sqlite3")
cur_progress_2018 = conn_progress_2018.cursor()

conn_progress_2019 = sqlite3.connect("Data/Databases/Progress_2019.sqlite3")
cur_progress_2019 = conn_progress_2019.cursor()

conn_progress_2020 = sqlite3.connect("Data/Databases/Progress_2020.sqlite3")
cur_progress_2020 = conn_progress_2020.cursor()

conn_progress_2021 = sqlite3.connect("Data/Databases/Progress_2021.sqlite3")
cur_progress_2021 = conn_progress_2021.cursor()

conn_progress_2022 = sqlite3.connect("Data/Databases/Progress_2022.sqlite3")
cur_progress_2022 = conn_progress_2022.cursor()

conn_progress_2023 = sqlite3.connect("Data/Databases/Progress_2023.sqlite3")
cur_progress_2023 = conn_progress_2023.cursor()

conn_progress_dict = {"2018" : conn_progress_2018, "2019" : conn_progress_2019, "2020" : conn_progress_2020, "2021" : conn_progress_2021, "2022" : conn_progress_2022, "2023" : conn_progress_2023}
cur_progress_dict = {"2018" : cur_progress_2018, "2019" : cur_progress_2019, "2020" : cur_progress_2020, "2021" : cur_progress_2021, "2022" : cur_progress_2022, "2023" : cur_progress_2023}

count_lis = []
df_dict = {}
process_dict = {}
def run_query(query, year, conn, result_name, dict1):
    # df_dict = SharedMemoryDict(name="df_dict", size=1000000000)
    conn = sqlite3.connect(f"Data/Databases/Data2.sqlite3")
    cur = conn.cursor()
    df1 = pd.read_sql_query(query, conn)
    
    # if year+".csv" not in os.listdir("Data/Results/"+result_name+"/"):
    #     df1.to_csv("Data/Results/"+result_name+"/"+year+".csv", index=False)
    if dict1[year] is None:
        dict1[year] = df1
    # df = pd.concat([df, df1])
    # conn = sqlite3.connect(f"Data/Databases/Progress_{year}.sqlite3")
    # cur = conn.cursor()
    # cur.execute("update progress set status = status + 1 where year = ?", (year,))
    # conn.commit()
    # print("Read batch", batch)
    conn.close()
    # for i in process_dict[year]:
    #     i.terminate()

count_list = []
def start_process(year, query, conn, result_name, dict):
    p = Process(target=run_query, args=(query, year, conn, result_name, dict))
    p.start()
    return p
start_time = time.time()

years = ["2018", "2019", "2020", "2021", "2022", "2023"]
processes = []


def create_exec(query_type, supplier, importer, product, start, end):

    lis = [supplier[:-4], importer[:-4], product[:-4]]
    string = ""
    for i in lis:
        if i.strip() != "":
            string += i + " and "
    string = string[:-5]
    print(string)
    # cols = "BE_NO, STRFTIME('%d-%m-%Y', BEDATE) as date, HS_CODE, PRODUCT_DESCRIPTION, QUANTITY, UNIT, ASSESS_VALUE_INR, UNIT_PRICE_INR, ASSESS_VALUE_USD, UNIT_PRICE_USD, TOTAL_DUTY, TOTAL_DUTY_BE_WISE, APPLICABLE_DUTY_INR, EXCHANGE_RATE_USD, ITEM_RATE_INV_CURR, VALUE_INV_CURR, INVOICE_CURRENCY, ASSESS_GROUP, IMPORTER_CODE, IMPORTER_NAME, IMPORTER_ADDRESS, IMPORTER_CITY, IMPORTER_PIN, IMPORTER_STATE, SUPPLIER_CODE, SUPPLIER_NAME, SUPPLIER_ADDRESS, SUPPLIER_COUNTRY, FOREIGN_PORT, FOREIGN_COUNTRY, FOREIGN_REGIONS, CHA_NAME, CHA_PAN, IEC, IEC_CODE, INVOICE_NUMBER, INVOICE_SR_NO, ITEM_NUMBER, HSCODE_2DIGIT, HSCODE_4DIGIT, TYPE, INDIAN_PORT, SHIPMENT_MODE, INDIAN_REGIONS, SHIPMENT_PORT, HSCODE_6DIGIT, BCD_NOTN, BCD_RATE, BCD_AMOUNT_INR, CVD_NOTN, CVD_RATE, CVD_AMOUNT_INR, IGST_AMOUNT_INR, GST_CESS_AMOUNT_INR, REMARK, INCOTERMS, TOTAL_FREIGHT_VALUE_FORGN_CUR, FREIGHT_CURRENCY, TOTAL_INSU_VALUE_FORGN_CUR, INSURANCE_CURRENCY, TOTAL_INVOICE_VALUE_INR, INSURANCE_VALUE_INR, TOTAL_GROSS_WEIGHT, TOTAL_FREIGHT_VALUE_INR, GROSS_WEIGHT_UNIT, CUSTOM_NOTIFICATION, STANDARD_QUANTITY, STANDARD_QUANTITY_UNIT"
    # cols = "STRFTIME('%d-%m-%Y', BEDATE) as date"
    cols = "*"
    # indexes = {"100" : "search_index_sup", "010" : "search_index_imp", "001" : "search_index_prod", "110" : "search_index_supimpproddate", "101" : "search_index_supimpproddate", "011" : "search_index_supimpproddate", "111" : "search_index_supimpproddate"}
    year = start[:4]

    if start[5:] == "01-01" and end[5:] == "12-31":
        bedate = ""
    else:
        bedate = "and BEDATE between '{}' and '{}'".format(start, end)
    # start_process("select * from Data_2019 where ROWID in (select ROWID from Data_2019_virt_searcher where PRODUCT_DESCRIPTION MATCH 'blower')", "")

    exec_str = "select {} from Data_{} where ROWID in ( select ROWID from Data_{}_virt_searcher where {}) {}".format(cols, year, year, string, bedate)
    
    # print(exec_str)
    # sys.exit()

    return exec_str

def create_batch(query_type, start_date, end_date, supplier, importer, product):
    indices = {0 : "sup_index_", 1 : "imp_index_", 2 : "prod_index_"}
    cur.execute("delete from progress")
    conn.commit()

    dict1 = Manager().dict()
    for j in range(int(start_date[:4]), int(end_date[:4])+1):
        df_dict[str(j)] = None
        dict1[str(j)] = None
        process_dict[str(j)] = []
        # cur_progress_dict[str(j)].execute("delete from progress")
        # cur_progress_dict[str(j)].execute("insert into progress values (?, ?)", (str(j), 0))
        # conn_progress_dict[str(j)].commit()
    conn.commit()
    # print(df_dict)

    result_name = (supplier[21:-6]+"_"+importer[21:-6]+"_"+product[27:-6]).rstrip("_").lstrip("_")
    try:
        shutil.rmtree("Data/Results/"+result_name+"/")
        print("Results folder already exists, deleting now")
    except Exception as E:
        print("Results folder does not exist, creating now")
    os.mkdir("Data/Results/"+result_name+"/")
        
    
    # for i in range(0, 3):
    #     if index_type[i] == "1":
    for year in df_dict.keys():
        if(year == start_date[:4]):
            if(year == end_date[:4]):
                end = end_date
            else:
                end = str(year)+"-12-31"
            start = start_date

        elif(year == end_date[:4]):
            start = str(year)+"-01-01"
            end = end_date
        else:
            start = str(year)+"-01-01"
            end = str(year)+"-12-31"
        print(start, end)
        query = create_exec(query_type, supplier, importer, product, start, end)
        
        process_dict[year].append(start_process(year, query, year, result_name, dict1))
        print(query)
        print()
    
    # for i in df_dict.keys():
    #     print(i, process_dict[i])
    flag = True
    while(flag):
        flag = False
        for year in df_dict.keys():
            # if(year+".csv" in os.listdir("Data/Results/"+result_name+"/") and cur_progress_dict[year].execute("select status from progress where year = ?", (year,)).fetchone()[0] and df_dict[year] is None):
            if(dict1[year] is not None and df_dict[year] is None):
                print("Done with", year)
                print(len(dict1[year]))
                df_dict[year] = dict1[year]
                # df_dict[year] = pd.read_csv("Data/Results/"+result_name+"/"+year+".csv")
                # print(df_dict[year]["PRODUCT_DESCRIPTION"])
        for year in df_dict.keys():
            if(df_dict[year] is None):
                flag = True
                break
    
    return dict1

start_date = "2018-02-01"
end_date = "2023-12-31"
supplier = "infineon" #infineon
importer = "" #millennium semiconductor #ifb industries
product = "circuit" #motor


query_type = ""

if supplier == "":
    supplier = " and"
    query_type += "0"
else:
    supplier = "SUPPLIER_NAME MATCH '" + supplier.upper() +"' and"
    query_type += "1"

if importer == "":
    importer = " and"
    query_type += "0"
else:
    importer = "IMPORTER_NAME MATCH '" + importer.upper() + "' and"
    query_type += "1"

if product == "":
    product = " and"
    query_type += "0"
else:
    product = "PRODUCT_DESCRIPTION MATCH '" + product.upper() + "' and"
    query_type += "1"


if start_date == "" or int(start_date[:4]) < 2018:
    start_date = "2018-01-01"

if end_date == "" or int(end_date[:4]) > 2023:
    end_date = "2023-12-31"

create_batch(query_type, start_date, end_date, supplier, importer, product)

print("Time taken: ", time.time() - start_time)
sys.exit()
