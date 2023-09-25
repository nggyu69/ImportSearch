from django.shortcuts import render
from django.http import HttpResponse
import threading
import pandas as pd
import sqlite3
import polars as pl
import time

import csv


# Create your views here.

years = ["2018", "2019", "2020", "2021", "2022", "2023"]
df_dict = {}
def create_exec(start, end, query_type, supplier, importer, product):

    cols = "BE_NO, STRFTIME('%d-%m-%Y', BEDATE) as date, HS_CODE, PRODUCT_DESCRIPTION, QUANTITY, UNIT, ASSESS_VALUE_INR, UNIT_PRICE_INR, ASSESS_VALUE_USD, UNIT_PRICE_USD, TOTAL_DUTY, TOTAL_DUTY_BE_WISE, APPLICABLE_DUTY_INR, EXCHANGE_RATE_USD, ITEM_RATE_INV_CURR, VALUE_INV_CURR, INVOICE_CURRENCY, ASSESS_GROUP, IMPORTER_CODE, IMPORTER_NAME, IMPORTER_ADDRESS, IMPORTER_CITY, IMPORTER_PIN, IMPORTER_STATE, SUPPLIER_CODE, SUPPLIER_NAME, SUPPLIER_ADDRESS, SUPPLIER_COUNTRY, FOREIGN_PORT, FOREIGN_COUNTRY, FOREIGN_REGIONS, CHA_NAME, CHA_PAN, IEC, IEC_CODE, INVOICE_NUMBER, INVOICE_SR_NO, ITEM_NUMBER, HSCODE_2DIGIT, HSCODE_4DIGIT, TYPE, INDIAN_PORT, SHIPMENT_MODE, INDIAN_REGIONS, SHIPMENT_PORT, HSCODE_6DIGIT, BCD_NOTN, BCD_RATE, BCD_AMOUNT_INR, CVD_NOTN, CVD_RATE, CVD_AMOUNT_INR, IGST_AMOUNT_INR, GST_CESS_AMOUNT_INR, REMARK, INCOTERMS, TOTAL_FREIGHT_VALUE_FORGN_CUR, FREIGHT_CURRENCY, TOTAL_INSU_VALUE_FORGN_CUR, INSURANCE_CURRENCY, TOTAL_INVOICE_VALUE_INR, INSURANCE_VALUE_INR, TOTAL_GROSS_WEIGHT, TOTAL_FREIGHT_VALUE_INR, GROSS_WEIGHT_UNIT, CUSTOM_NOTIFICATION, STANDARD_QUANTITY, STANDARD_QUANTITY_UNIT"
    # cols = "STRFTIME('%d-%m-%Y', BEDATE) as date"
    indexes = {"100" : "search_index_sup", "010" : "search_index_imp", "001" : "search_index_prod", "110" : "search_index_supimpproddate", "101" : "search_index_supimpproddate", "011" : "search_index_supimpproddate", "111" : "search_index_supimpproddate"}

    if query_type in ["111", "110", "101", "011"]:
        exec_str = "select {} from importdata_A indexed by {} where BEDATE between '{}' and '{}' and {} {} {}".format(cols, indexes[query_type], start, end, supplier, importer, product[:-4])
    else:
        focus = ""
        for d in [supplier, importer, product]:
                if(len(d) != 0):
                    focus = d[:-4]
                    break
        if(start[5:] == "01-01" and end[5:] == "12-31"):
            
                
            exec_str = "select {} from importdata_A indexed by {} where {}".format(cols, indexes[query_type],focus)
        else:
            exec_str = "select {} from importdata_A indexed by {} where BEDATE between '{}' and '{}' and {}".format(cols, indexes[query_type]+"date", start, end, focus)

    
    return exec_str

def run_query(query, year):
    conn = sqlite3.connect("Data/Data.sqlite3")
    df_dict[year] = pd.read_sql(query, conn)
    conn.close()

def start_process(query, year):
    p = threading.Thread(target=run_query, args=(query, year))
    p.start()
    return p


def search(request):
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
        dl = pl.DataFrame()
        query_type = ""

        if supplier == "":
            supplier = ""
            query_type += "0"
        else:
            supplier = "SUPPLIER_NAME like '%"+supplier+"%' and"
            query_type += "1"

        if importer == "":
            importer = ""
            query_type += "0"
        else:
            importer = "IMPORTER_NAME like '%"+importer+"%' and"
            query_type += "1"

        if product == "":
            product = ""
            query_type += "0"
        else:
            product = "PRODUCT_DESCRIPTION like '%"+product+"%' and"
            query_type += "1"

        
        if start_date == "" or int(start_date[:4]) < 2018:
            start_date = "2018-01-01"

        if end_date == "" or int(end_date[:4]) > 2023:
            end_date = "2023-12-31"
        # writer = pd.ExcelWriter('Databases/Results.xlsx', engine='xlsxwriter')

        proccesses = []
        for i in range(int(start_date[:4]), int(end_date[:4])+1):
            if(i == int(start_date[:4])):
                if(i == int(end_date[:4])):
                    end = end_date
                else:
                    end = str(i)+"-12-31"
                start = start_date

            elif(i == int(end_date[:4])):
                start = str(i)+"-01-01"
                end = end_date
            else:
                start = str(i)+"-01-01"
                end = str(i)+"-12-31"
            exec_string = create_exec(start, end, query_type, supplier, importer, product)
            proccesses.append(start_process(exec_string, str(i)))

        for process in proccesses:
            process.join()

        for year in years:
            df = pd.concat([df, df_dict[year]])

        # df.to_csv("Databases/Results.csv", index=False)
        dl.write_csv("Data/Results.csv")
        # df.to_excel(writer, sheet_name='Sheet1', index=False)
        # # worksheet = writer.sheets['Sheet1']

        
        
        
        # with Workbook('Databases/Results.xlsx') as workbook:
        #     dl.write_excel(workbook=workbook)

        # csvfile = "Databases/Results.csv"
        # workbook = Workbook(csvfile[:-4] + 'converted.xlsx')
        # worksheet = workbook.add_worksheet()
        # with open(csvfile, 'rt', encoding='utf8') as f:
        #     reader = csv.reader(f)
        #     for r, row in enumerate(reader):
        #         for c, col in enumerate(row):
        #             worksheet.write(r, c, col)
        # worksheet.autofit()
        # worksheet.autofilter('A1:BP1')
        # worksheet.freeze_panes(1, 0)
        # workbook.close()

        print("Time taken : ",time.time() - start_time)
        
        context = {'data': dl.iter_rows, "cols": dl.columns}
        # return render(request, 'login/Results.html', context)
        with open("Databases/Resultsconverted.xlsx", "rb") as f:
            response = HttpResponse(f.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = 'attachment; filename="Results.xlsx"'
            return response

    context = {"today" : "2023-12-31"}
    return render(request, 'SearchApp/Search-page.html')