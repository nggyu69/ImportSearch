import pandas as pd
import numpy as np
import os
from xlsxwriter.workbook import Workbook
import csv



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

years = os.listdir("Data/Excel_Files")
years.sort()
years = years[1:]
for year in years:
    months = os.listdir("Data/Excel_Files/"+year)
    months.sort()

    for month in months:
        file_count = 0
        count = 0
    #     try:
    #         os.mkdir(f"Data/Excel_Files/{year}/{month[0:7]}")
    #         os.rename(f"Data/Excel_Files/{year}/{month}", f"Data/Excel_Files/{year}/{month[0:7]}/{month}")
    #     except:
    #         pass

        print("Reading", year, month)
        # df = pd.read_csv(f"Data/Excel_Files/{year}/{month}", dtype=dtypes, converters=converters)
        # for col in df.columns:
        #     if col not in dtypes and col not in converters:
        #         print(col)
        #         df.drop(col, axis=1, inplace=True)
        # if(len(df) > 1):
        #     writer = pd.ExcelWriter(f"Data/Excel_Files_2/{year}/{month[0:7]}.xlsx")
        #     print(len(df))
        #     df.to_excel(writer, index=False)
        # df.to_excel("Data/Excel_Files_2/"+year+"/"+year+"-"+month[5:7]+".xlsx", index=False)
        # print("Done", year, month)


        csvfile = f"Data/Excel_Files/{year}/{month[0:7]}/{month[0:7]}.csv"
        workbook = Workbook(f"Data/Excel_Files/{year}/{month[0:7]}/{month[0:7]}_{file_count}.xlsx")
        worksheet = workbook.add_worksheet()
        with open(csvfile, 'rt', encoding='utf8') as f:
            reader = csv.reader(f)
            for r, row in enumerate(reader):
                for c, col in enumerate(row):
                    worksheet.write(r, c, col)
                count += 1

                if count > 250000:
                    print(count)
                    count = 0 
                    file_count += 1
                    worksheet.autofit()
                    workbook.close()
                    workbook = Workbook(f"Data/Excel_Files/{year}/{month[0:7]}/{month[0:7]}_{file_count}.xlsx")
                    worksheet = workbook.add_worksheet()

        worksheet.autofit()
        workbook.close()
