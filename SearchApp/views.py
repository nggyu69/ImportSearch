from django.shortcuts import render
from django.http import HttpResponse
import threading
import pandas as pd
import sqlite3
import time
from multiprocessing import Process, Manager
import os
import shutil

# Create your views here.

conn_main = sqlite3.connect("Data/Databases/Data.sqlite3", check_same_thread=False)
cur_main = conn_main.cursor()

conn_progress_2018 = sqlite3.connect("Data/Databases/Progress_2018.sqlite3", check_same_thread=False)
cur_progress_2018 = conn_progress_2018.cursor()

conn_progress_2019 = sqlite3.connect("Data/Databases/Progress_2019.sqlite3", check_same_thread=False)
cur_progress_2019 = conn_progress_2019.cursor()

conn_progress_2020 = sqlite3.connect("Data/Databases/Progress_2020.sqlite3", check_same_thread=False)
cur_progress_2020 = conn_progress_2020.cursor()

conn_progress_2021 = sqlite3.connect("Data/Databases/Progress_2021.sqlite3", check_same_thread=False)
cur_progress_2021 = conn_progress_2021.cursor()

conn_progress_2022 = sqlite3.connect("Data/Databases/Progress_2022.sqlite3", check_same_thread=False)
cur_progress_2022 = conn_progress_2022.cursor()

conn_progress_2023 = sqlite3.connect("Data/Databases/Progress_2023.sqlite3", check_same_thread=False)
cur_progress_2023 = conn_progress_2023.cursor()

conn_progress_dict = {"2018" : conn_progress_2018, "2019" : conn_progress_2019, "2020" : conn_progress_2020, "2021" : conn_progress_2021, "2022" : conn_progress_2022, "2023" : conn_progress_2023}
cur_progress_dict = {"2018" : cur_progress_2018, "2019" : cur_progress_2019, "2020" : cur_progress_2020, "2021" : cur_progress_2021, "2022" : cur_progress_2022, "2023" : cur_progress_2023}

count_lis = []
df_dict = {}
def run_query(query, year, batch, conn, result_name):
    # df_dict = SharedMemoryDict(name="df_dict", size=1000000000)
    conn = sqlite3.connect(f"Data/Databases/Data.sqlite3")
    cur = conn.cursor()
    # connect = "sqlite:///home/fsociety/Code/Projects/ImportSearch/Data/Databases/Data.sqlite3"
    # df1 = pl.read_database_uri(query, connect)
    df1 = pd.read_sql_query(query, conn)
    
    if year+".csv" not in os.listdir("Data/Results/"+result_name+"/"):
        df1.to_csv("Data/Results/"+result_name+"/"+year+".csv", index=False)
    # df = pd.concat([df, df1])
    conn = sqlite3.connect(f"Data/Databases/Progress_{year}.sqlite3")
    cur = conn.cursor()
    cur.execute("update progress set status = status + 1 where year = ?", (year,))
    conn.commit()
    # print("Read batch", batch)
    conn.close()

def start_process(year, query, batch, conn, result_name):
    p = Process(target=run_query, args=(query, year, batch, conn, result_name))
    p.start()
    return p
start_time = time.time()


def create_exec(query_type, index_type, supplier, importer, product, start, end):

    # cols = "BE_NO, STRFTIME('%d-%m-%Y', BEDATE) as date, HS_CODE, PRODUCT_DESCRIPTION, QUANTITY, UNIT, ASSESS_VALUE_INR, UNIT_PRICE_INR, ASSESS_VALUE_USD, UNIT_PRICE_USD, TOTAL_DUTY, TOTAL_DUTY_BE_WISE, APPLICABLE_DUTY_INR, EXCHANGE_RATE_USD, ITEM_RATE_INV_CURR, VALUE_INV_CURR, INVOICE_CURRENCY, ASSESS_GROUP, IMPORTER_CODE, IMPORTER_NAME, IMPORTER_ADDRESS, IMPORTER_CITY, IMPORTER_PIN, IMPORTER_STATE, SUPPLIER_CODE, SUPPLIER_NAME, SUPPLIER_ADDRESS, SUPPLIER_COUNTRY, FOREIGN_PORT, FOREIGN_COUNTRY, FOREIGN_REGIONS, CHA_NAME, CHA_PAN, IEC, IEC_CODE, INVOICE_NUMBER, INVOICE_SR_NO, ITEM_NUMBER, HSCODE_2DIGIT, HSCODE_4DIGIT, TYPE, INDIAN_PORT, SHIPMENT_MODE, INDIAN_REGIONS, SHIPMENT_PORT, HSCODE_6DIGIT, BCD_NOTN, BCD_RATE, BCD_AMOUNT_INR, CVD_NOTN, CVD_RATE, CVD_AMOUNT_INR, IGST_AMOUNT_INR, GST_CESS_AMOUNT_INR, REMARK, INCOTERMS, TOTAL_FREIGHT_VALUE_FORGN_CUR, FREIGHT_CURRENCY, TOTAL_INSU_VALUE_FORGN_CUR, INSURANCE_CURRENCY, TOTAL_INVOICE_VALUE_INR, INSURANCE_VALUE_INR, TOTAL_GROSS_WEIGHT, TOTAL_FREIGHT_VALUE_INR, GROSS_WEIGHT_UNIT, CUSTOM_NOTIFICATION, STANDARD_QUANTITY, STANDARD_QUANTITY_UNIT"
    # cols = "STRFTIME('%d-%m-%Y', BEDATE) as date"
    cols = "*"
    indexes = {"100" : "search_index_sup", "010" : "search_index_imp", "001" : "search_index_prod", "110" : "search_index_supimpproddate", "101" : "search_index_supimpproddate", "011" : "search_index_supimpproddate", "111" : "search_index_supimpproddate"}
    year = start[:4]

    if start[5:] == "01-01" and end[5:] == "12-31":
        bedate = ""
    else:
        bedate = "BEDATE between '{}' and '{}' and".format(start, end)

    
    if query_type == "111":
        if index_type == "sup_index_":
            exec_str = "select {} from Data_{} indexed by {}{} where {} {} {} {}".format(cols, year, index_type, year, bedate, supplier, product, importer[:-4])
        elif index_type == "imp_index_":
            exec_str = "select {} from Data_{} indexed by {}{} where {} {} {} {}".format(cols, year, index_type, year, bedate, importer, supplier, product[:-4])
        elif index_type == "prod_index_":
            exec_str = "select {} from Data_{} indexed by {}{} where {} {} {} {}".format(cols, year, index_type, year, bedate, product, importer, supplier[:-4])
    
    elif query_type == "110":
        if index_type == "sup_index_":
            exec_str = "select {} from Data_{} indexed by {}{} where {} {} {}".format(cols, year, index_type, year, bedate, supplier, importer[:-4])
        elif index_type == "imp_index_":
            exec_str = "select {} from Data_{} indexed by {}{} where {} {} {}".format(cols, year, index_type, year, bedate, importer, supplier[:-4])
    
    elif query_type == "011":
        if index_type == "imp_index_":
            exec_str = "select {} from Data_{} indexed by {}{} where {} {} {}".format(cols, year, index_type, year, bedate, importer, product[:-4])
        elif index_type == "prod_index_":
            exec_str = "select {} from Data_{} indexed by {}{} where {} {} {}".format(cols, year, index_type, year, bedate, product, importer[:-4])
    
    elif query_type == "101":
        if index_type == "sup_index_":
            exec_str = "select {} from Data_{} indexed by {}{} where {} {} {}".format(cols, year, index_type, year, bedate, supplier, product[:-4])
        elif index_type == "prod_index_":
            exec_str = "select {} from Data_{} indexed by {}{} where {} {} {}".format(cols, year, index_type, year, bedate, product, supplier[:-4])
    else:
        focus = ""
        for d in [supplier, importer, product]:
                if(len(d) != 0):
                    focus = d[:-4]
                    break
        exec_str = "select {} from Data_{} indexed by {}{} where {} {}".format(cols, year, index_type, year, bedate, focus)

    
    return exec_str

def create_batch(index_type, start_date, end_date, supplier, importer, product):
    indices = {0 : "sup_index_", 1 : "imp_index_", 2 : "prod_index_"}
    # cur.execute("delete from progress")
    # conn.commit()
    for j in range(int(start_date[:4]), int(end_date[:4])+1):
        df_dict[str(j)] = None
        cur_progress_dict[str(j)].execute("delete from progress")
        cur_progress_dict[str(j)].execute("insert into progress values (?, ?)", (str(j), 0))
        conn_progress_dict[str(j)].commit()
    conn_main.commit()
    print(df_dict)

    result_name = (supplier[21:-6]+"_"+importer[21:-6]+"_"+product[27:-6]).rstrip("_").lstrip("_")
    try:
        shutil.rmtree("Data/Results/"+result_name+"/")
    except Exception as E:
        print("Results folder does not exist, creating now")
    os.mkdir("Data/Results/"+result_name+"/")
        
    
    for i in range(0, 3):
        if index_type[i] == "1":
            processes = []
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
                print(year,"Batch", indices[i]+year, "done")
                query = create_exec(index_type, indices[i], supplier, importer, product, start, end)
                
                start_process(year, query, indices[i]+year, year, result_name)
                print(query)
                print()
    
    flag = True
    while(flag):
        flag = False
        for year in df_dict.keys():
            if(year+".csv" in os.listdir("Data/Results/"+result_name+"/") and cur_progress_dict[year].execute("select status from progress where year = ?", (year,)).fetchone()[0] and df_dict[year] is None):
                print("Done with", year)
                df_dict[year] = pd.read_csv("Data/Results/"+result_name+"/"+year+".csv")
                print(df_dict[year].shape)
        for year in df_dict.keys():
            if(df_dict[year] is None):
                flag = True
                break
        # print("Waiting for all processes to finish")
    # for year in df_dict.keys():
    #     if(os.listdir("Data/Results/"+result_name+"/")):
    #         df_dict[year] = pd.read_csv("Data/Results/"+result_name+"/"+year+"/"+year+".csv")
    

    return result_name

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


        result_name = create_batch(query_type, start_date, end_date, supplier, importer, product)

        for year in df_dict.keys():
            df = pd.concat([df, df_dict[year]])
        
        print(df.shape)
        # df.to_csv("Databases/Results.csv", index=False)
        # dl.write_csv("Data/Results.csv")
        writer = pd.ExcelWriter("Data/Results/"+result_name+"/"+f"{result_name}.xlsx", engine='xlsxwriter')
        df.to_csv("Data/Results/"+result_name+"/"+f"{result_name}.csv", index=False)
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
        
        # context = {'data': dl.iter_rows, "cols": dl.columns}
        # return render(request, 'login/Results.html', context)
        # with open("Data/Results/"+result_name+"/"+f"{result_name}.xlsx", "rb") as f:
        #     response = HttpResponse(f.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        #     response['Content-Disposition'] = 'attachment; filename="Results.xlsx"'
        #     return response
        with open("Data/Results/"+result_name+"/"+f"{result_name}.csv", "rb") as f:
            response = HttpResponse(f.read(), content_type='text/csv')
            response['Content-Disposition'] = f'attachment; filename="{result_name}.csv"'
            return response

    context = {"today" : "2023-12-31"}
    return render(request, 'SearchApp/Search-page.html')