from django.shortcuts import render
from django.http import HttpResponse
import threading
import pandas as pd
import sqlite3
import time
from multiprocessing import Process, Manager
from xlsxwriter.workbook import Workbook
import csv
import os
import shutil
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning) 


conn_main = sqlite3.connect("Data/Databases/Data.sqlite3", check_same_thread=False)
cur_main = conn_main.cursor()

count_lis = []
df_dict = {}

process_dict = {}
def run_query(query, year, dict1):
    conn = sqlite3.connect(f"Data/Databases/Data2.sqlite3")
    df1 = pd.read_sql_query(query, conn)

    if dict1[year] is None:
        dict1[year] = df1
    
    conn.close()


count_list = []
def start_process(year, query, dict1):
    p = Process(target=run_query, args=(query, year, dict1))
    p.start()
    return p
start_time = time.time()

processes = []

def create_exec(supplier, importer, product, start, end):
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
    year = start[:4]

    if start[5:] == "01-01" and end[5:] == "12-31":
        bedate = ""
    else:
        bedate = "and BEDATE between '{}' and '{}'".format(start, end)

    exec_str = "select {} from Data_{} where ROWID in ( select ROWID from Data_{}_virt_searcher where {}) {}".format(cols, year, year, string, bedate)
 
    return exec_str


def create_batch(start_date, end_date, supplier, importer, product):

    dict1 = Manager().dict()
    for j in range(int(start_date[:4]), int(end_date[:4])+1):
        df_dict[str(j)] = None
        dict1[str(j)] = None
        process_dict[str(j)] = []

    
        
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
        query = create_exec(supplier, importer, product, start, end)
        
        process_dict[year].append(start_process(year, query, dict1))
        print(query)
        print()
    
    flag = True
    while(flag):
        flag = False
        for year in df_dict.keys():
            if(dict1[year] is not None and df_dict[year] is None):
                print("Done with", year)
                print(len(dict1[year]))
                df_dict[year] = dict1[year]
        for year in df_dict.keys():
            if(df_dict[year] is None):
                flag = True
                break    

    return dict1

def search(request):
    print(request.session.session_key)
    start_time = time.time()
    global dl
    # print(time.time() - start_time)


    if request.method == "POST":
        start_date = request.POST.get('from_date').upper()
        end_date = request.POST.get('to_date').upper()
        supplier = request.POST.get('SN').upper()
        importer = request.POST.get('IN').upper()
        product = request.POST.get('PD').upper()

        
        df = pd.DataFrame()
        query_type = ""

        if supplier == "":
            supplier = " and"
            query_type += "0"
        else:
            supplier = "SUPPLIER_NAME MATCH '" + supplier +"' and"
            query_type += "1"

        if importer == "":
            importer = " and"
            query_type += "0"
        else:
            importer = "IMPORTER_NAME MATCH '" + importer + "' and"
            query_type += "1"

        if product == "":
            product = " and"
            query_type += "0"
        else:
            product = "PRODUCT_DESCRIPTION MATCH '" + product + "' and"
            query_type += "1"


        if start_date == "" or int(start_date[:4]) < 2018:
            start_date = "2018-01-01"

        if end_date == "" or int(end_date[:4]) > 2023:
            end_date = "2023-12-31"


        result_name = start_date+"_"+((supplier[21:-5]+"_"+importer[21:-5]+"_"+product[27:-5]).rstrip("_").lstrip("_"))+"_"+end_date
        
        if os.path.exists("Data/Results/"+result_name+"/"):
            print("Results folder already exists")
            while(not os.path.exists(f"Data/Results/{result_name}/{result_name}.xlsx")):
                pass
            print("File exists")
            
        else:
            print("Results folder does not exist, creating now")
            os.mkdir("Data/Results/"+result_name+"/")

            dict1 = create_batch(start_date, end_date, supplier, importer, product)

            for year in dict1.keys():
                df = pd.concat([df, dict1[year]])

            print(df.shape)
            df.to_csv("Data/Results/"+result_name+"/"+f"{result_name}.csv", index=False)

            csvfile = f"Data/Results/{result_name}/{result_name}.csv"
            workbook = Workbook(f"Data/Results/{result_name}/{result_name}.xlsx")
            worksheet = workbook.add_worksheet()
            with open(csvfile, 'rt', encoding='utf8') as f:
                reader = csv.reader(f)
                for r, row in enumerate(reader):
                    for c, col in enumerate(row):
                        worksheet.write(r, c, col)
            worksheet.autofit()
            worksheet.autofilter('A1:BP1')
            worksheet.freeze_panes(1, 0)
            workbook.close()

        print("Time taken : ",time.time() - start_time)
        
        with open("Data/Results/"+result_name+"/"+f"{result_name}.xlsx", "rb") as f:
            response = HttpResponse(f.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = f'attachment; filename="{result_name}.xlsx"'
            return response

    context = {"today" : "2023-12-31"}
    return render(request, 'SearchApp/Search-page.html')