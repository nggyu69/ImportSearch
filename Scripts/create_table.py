import pandas as pd
import numpy as np
import sqlite3
import time
import os
import sys

# os.chdir("..")

cols = "BE_NO, BEDATE, HS_CODE, PRODUCT_DESCRIPTION, QUANTITY, UNIT, ASSESS_VALUE_INR, UNIT_PRICE_INR, ASSESS_VALUE_USD, UNIT_PRICE_USD, TOTAL_DUTY, TOTAL_DUTY_BE_WISE, APPLICABLE_DUTY_INR, EXCHANGE_RATE_USD, ITEM_RATE_INV_CURR, VALUE_INV_CURR, INVOICE_CURRENCY, ASSESS_GROUP, IMPORTER_CODE, IMPORTER_NAME, IMPORTER_ADDRESS, IMPORTER_CITY, IMPORTER_PIN, IMPORTER_STATE, SUPPLIER_CODE, SUPPLIER_NAME, SUPPLIER_ADDRESS, SUPPLIER_COUNTRY, FOREIGN_PORT, FOREIGN_COUNTRY, FOREIGN_REGIONS, CHA_NAME, CHA_PAN, IEC, IEC_CODE, INVOICE_NUMBER, INVOICE_SR_NO, ITEM_NUMBER, HSCODE_2DIGIT, HSCODE_4DIGIT, TYPE, INDIAN_PORT, SHIPMENT_MODE, INDIAN_REGIONS, SHIPMENT_PORT, HSCODE_6DIGIT, BCD_NOTN, BCD_RATE, BCD_AMOUNT_INR, CVD_NOTN, CVD_RATE, CVD_AMOUNT_INR, IGST_AMOUNT_INR, GST_CESS_AMOUNT_INR, REMARK, INCOTERMS, TOTAL_FREIGHT_VALUE_FORGN_CUR, FREIGHT_CURRENCY, TOTAL_INSU_VALUE_FORGN_CUR, INSURANCE_CURRENCY, TOTAL_INVOICE_VALUE_INR, INSURANCE_VALUE_INR, TOTAL_GROSS_WEIGHT, TOTAL_FREIGHT_VALUE_INR, GROSS_WEIGHT_UNIT, CUSTOM_NOTIFICATION, STANDARD_QUANTITY, STANDARD_QUANTITY_UNIT"

conn = sqlite3.connect("Data/Databases/Data.sqlite3", check_same_thread=False)
cur = conn.cursor()
cur.execute("""CREATE TABLE if not exists "master" (
	"sl_no"	INTEGER,
	"stored_months"	TEXT,
	PRIMARY KEY("sl_no")
)""")

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
            "STANDARD_QUANTITY":check_num,
            "TOTAL_INSU_VALUE_FORGN_CUR":check_num}

column_map = {
    "BE_NO": ["BE_NO"],
    "BEDATE": ["BEDATE", "DATE"],
    "HS_CODE": ["HS_CODE", "HS CODE"],
    "PRODUCT_DESCRIPTION": ["PRODUCT_DESCRIPTION", "PRODUCT DESCRIPTION"],
    "QUANTITY": ["QUANTITY"],
    "UNIT": ["UNIT"],
    "ASSESS_VALUE_INR": ["ASSESS_VALUE_INR"],
    "UNIT_PRICE_INR": ["UNIT_PRICE_INR"],
    "ASSESS_VALUE_USD": ["ASSESS_VALUE_USD", "CIF VALUE (USD)"],
    "UNIT_PRICE_USD": ["UNIT_PRICE_USD", "UNIT PRICE (USD)"],
    "TOTAL_DUTY": ["TOTAL_DUTY", "ESTIMATED TARIFF(USD)"],
    "TOTAL_DUTY_BE_WISE": ["TOTAL_DUTY_BE_WISE"],
    "APPLICABLE_DUTY_INR": ["APPLICABLE_DUTY_INR"],
    "EXCHANGE_RATE_USD": ["EXCHANGE_RATE_USD", "EXCHANGE RATE(USD)"],
    "ITEM_RATE_INV_CURR": ["ITEM_RATE_INV_CURR", "ITEM RATE"],
    "VALUE_INV_CURR": ["VALUE_INV_CURR"],
    "INVOICE_CURRENCY": ["INVOICE_CURRENCY", "CURRENCY"],
    "ASSESS_GROUP": ["ASSESS_GROUP"],
    "IMPORTER_CODE": ["IMPORTER_CODE", "IMPORTER CODE"],
    "IMPORTER_NAME": ["IMPORTER_NAME", "IMPORTER NAME"],
    "IMPORTER_ADDRESS": ["IMPORTER_ADDRESS", "IMPORTER ADDRESS"],
    "IMPORTER_CITY": ["IMPORTER_CITY", "IMPORTER CITY"],
    "IMPORTER_PIN": ["IMPORTER_PIN", "IMPORTER PIN"],
    "IMPORTER_STATE": ["IMPORTER_STATE", "IMPORTER STATE"],
    "SUPPLIER_CODE": ["SUPPLIER_CODE", "SUPPLIER CODE"],
    "SUPPLIER_NAME": ["SUPPLIER_NAME", "SUPPLIER NAME"],
    "SUPPLIER_ADDRESS": ["SUPPLIER_ADDRESS", "SUPPLIER ADDRESS"],
    "SUPPLIER_COUNTRY": ["SUPPLIER_COUNTRY", "SUPPLIER COUNTRY"],
    "FOREIGN_PORT": ["FOREIGN_PORT", "FOREIGN PORT"],
    "FOREIGN_COUNTRY": ["FOREIGN_COUNTRY", "FOREIGN COUNTRY"],
    "FOREIGN_REGIONS": ["FOREIGN_REGIONS", "FOREIGN REGIONS"],
    "CHA_NAME": ["CHA_NAME"],
    "CHA_PAN": ["CHA_PAN"],
    "IEC": ["IEC"],
    "IEC_CODE": ["IEC_CODE"],
    "INVOICE_NUMBER": ["INVOICE_NUMBER"],
    "INVOICE_SR_NO": ["INVOICE_SR_NO"],
    "ITEM_NUMBER": ["ITEM_NUMBER"],
    "HSCODE_2DIGIT": ["HSCODE_2DIGIT", "HSCODE(2 DIGIT)"],
    "HSCODE_4DIGIT": ["HSCODE_4DIGIT", "HSCODE(4 DIGIT)"],
    "TYPE": ["TYPE"],
    "INDIAN_PORT": ["INDIAN_PORT", "INDIAN PORT"],
    "SHIPMENT_MODE": ["SHIPMENT_MODE", "SHIPMENT MODE"],
    "INDIAN_REGIONS": ["INDIAN_REGIONS", "INDIAN REGIONS"],
    "SHIPMENT_PORT": ["SHIPMENT_PORT", "SHIPMENT PORT"],
    "HSCODE_6DIGIT": ["HSCODE_6DIGIT", "HSCODE(6 DIGIT)"],
    "BCD_NOTN": ["BCD_NOTN"],
    "BCD_RATE": ["BCD_RATE"],
    "BCD_AMOUNT_INR": ["BCD_AMOUNT_INR", "BCD AMOUNT(INR)"],
    "CVD_NOTN": ["CVD_NOTN"],
    "CVD_RATE": ["CVD_RATE"],
    "CVD_AMOUNT_INR": ["CVD_AMOUNT_INR"],
    "IGST_AMOUNT_INR": ["IGST_AMOUNT_INR"],
    "GST_CESS_AMOUNT_INR": ["GST_CESS_AMOUNT_INR"],
    "REMARK": ["REMARK"],
    "INCOTERMS": ["INCOTERMS"],
    "TOTAL_FREIGHT_VALUE_FORGN_CUR": ["TOTAL_FREIGHT_VALUE_FORGN_CUR", "TOTAL FREIGHT VALUE(FORGN CUR)"],
    "FREIGHT_CURRENCY": ["FREIGHT_CURRENCY"],
    "TOTAL_INSU_VALUE_FORGN_CUR": ["TOTAL_INSU_VALUE_FORGN_CUR", "TOTAL INSU VALUE(FORGN CUR)"],
    "INSURANCE_CURRENCY": ["INSURANCE_CURRENCY"],
    "TOTAL_INVOICE_VALUE_INR": ["TOTAL_INVOICE_VALUE_INR"],
    "INSURANCE_VALUE_INR": ["INSURANCE_VALUE_INR"],
    "TOTAL_GROSS_WEIGHT": ["TOTAL_GROSS_WEIGHT", "TOTAL GROSS WEIGHT"],
    "TOTAL_FREIGHT_VALUE_INR": ["TOTAL_FREIGHT_VALUE_INR"],
    "GROSS_WEIGHT_UNIT": ["GROSS_WEIGHT_UNIT", "GROSS WEIGHT UNIT"],
    "CUSTOM_NOTIFICATION": ["CUSTOM_NOTIFICATION"],
    "STANDARD_QUANTITY": ["STANDARD_QUANTITY", "STANDARD QUANTITY"],
    "STANDARD_QUANTITY_UNIT": ["STANDARD_QUANTITY_UNIT", "STANDARD QUANTITY UNIT"]
}

def cols_string():
    cols_string = ""
    for i in cols.split(", "):
        if i in dtypes.keys():
            cols_string += f"{i} TEXT, "
        else:
            cols_string += f"{i} REAL, "
    cols_string = cols_string[:-2]
    return cols_string

def create_table(year):
    if((f"Data_{year}",) in list(cur.execute("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name").fetchall())):
        return
    else:
        cur.execute(f"CREATE TABLE if not exists Data_{year} ({cols_string()})")
        cur.execute(f"create virtual table if not exists Data_{year}_virt_searcher using fts5(PRODUCT_DESCRIPTION, IMPORTER_NAME, SUPPLIER_NAME, content = 'Data_{year}', tokenize = 'trigram')")
        conn.commit()
        cur.execute(f"""create trigger Data_{year}_virt_searcher_insert after insert on Data_{year} 
                        begin 
                        insert into Data_{year}_virt_searcher(rowid, PRODUCT_DESCRIPTION, IMPORTER_NAME, SUPPLIER_NAME) values (new.rowid, new.PRODUCT_DESCRIPTION, new.IMPORTER_NAME, new.SUPPLIER_NAME); 
                        end;""")
        cur.execute(f"""create trigger Data_{year}_virt_searcher_delete after delete on Data_{year}
                        begin
                        delete from Data_{year}_virt_searcher where rowid = old.rowid;
                        end;""")
        cur.execute(f"""create trigger Data_{year}_virt_searcher_update after update on Data_{year}
                        begin
                        update Data_{year}_virt_searcher set PRODUCT_DESCRIPTION = new.PRODUCT_DESCRIPTION, IMPORTER_NAME = new.IMPORTER_NAME, SUPPLIER_NAME = new.SUPPLIER_NAME where rowid = old.rowid;
                        end;""")
        conn.commit()
def check_new_file():
    cur_dict = {}
    current_years = os.listdir("Data/Excel_Files")
    current_years.sort()
    for year in current_years:
        if len(year) == 4:
            create_table(year)
            months = []
            current_months = os.listdir("Data/Excel_Files/"+year+"/")
            current_months.sort()
            for month in current_months:
                if month[0:4] == year:
                    months.append(month[5:7])      
            cur_dict[year] = months
    df = pd.read_sql("SELECT * FROM master", conn)
    prev_dict = {}
    sl_no = 1
    for i, j in zip(df["sl_no"], df["stored_months"]):
        try:
            prev_dict[j[0:4]] += [j[5:7]]
        except Exception as E:
            prev_dict[j[0:4]] = [j[5:7]]
        sl_no = i
    print(prev_dict)
    for i in prev_dict:
        for j in prev_dict[i]:
            cur_dict[i].remove(j)
    print(cur_dict)
    for i in cur_dict:
        for j in cur_dict[i]:
            t = 1
            files = os.listdir(f"Data/Excel_Files/{i}/{i}-{j}")
            files.sort()
            
            for k in files:
                print(f"\nProccessing : Data/Excel_files/{i}/{i}-{j}/{k}")
                if k.endswith(".xlsx"):
                    if k[:-5]+".csv" in files:
                        print("CSV exists, not converting")
                        continue
                    df = pd.read_excel(f"Data/Excel_Files/{i}/{i}-{j}/{k}", header = 0, engine="openpyxl")
                    df_new = pd.DataFrame(columns=cols.split(", "))
                    
                    if len(df.columns) < 60:
                        print("Non-standard format file, converting.....")
                        for old_col in column_map:
                            exist = False
                            for map_col in column_map[old_col]:
                                if map_col in df.columns:
                                    # df.rename(columns={j:i}, inplace=True)
                                    df_new[old_col] = df[map_col]
                                    # print(old_col, map_col)
                                    exist = True
                            if not exist:
                                df_new[old_col] = np.nan
                                # print(old_col, "Empty")
                        df_new["PRODUCT_DESCRIPTION"] = df_new["PRODUCT_DESCRIPTION"].str.replace(";", " ", regex=False)
                        print("Converted")
                        # df_new.to_csv(f"Data/Excel_Files/{i}/{i}-{j}/{k[:-5]}.csv", sep=",", index=False, header=True)
                        os.remove(f"Data/Excel_Files/{i}/{i}-{j}/{k}")
                        print("Deleted")
                        df_new.to_excel(f"Data/Excel_Files/{i}/{i}-{j}/{k}", index=False, header=True)
                        print("Saved in standard format")
                    
                    df = pd.read_excel(f"Data/Excel_Files/{i}/{i}-{j}/{k}", names = cols.split(", "), dtype=dtypes, converters=converters, engine="openpyxl")
                    if(df.shape[0] < 3):
                        continue
                    print("File shape : ", df.shape)
                    
                    df.to_csv(f"Data/Excel_Files/{i}/{i}-{j}/{k[:-5]}.csv", index=False, header=True)
                    # os.remove(f"Data/Excel_Files/{i}/{i}-{j}/{k}")
                    
                    k = k[:-5]+".csv"
                
                for chunk in pd.read_csv(f"Data/Excel_Files/{i}/{i}-{j}/{k}",dtype=dtypes,converters=converters, chunksize=50000):
                    chunk.rename(columns={"TOTAL_INSU_VALUE_ FORGN_CUR":"TOTAL_INSU_VALUE_FORGN_CUR"}, inplace=True)
                    for x in chunk.columns:
                        if x not in dtypes and x not in converters:
                            print("Deleting column: ", x)
                            chunk.drop(x, axis=1, inplace=True)
                    chunk.to_sql("Data_"+i, conn, if_exists="append", index=False)
                    print(i,":", j, ":", t, "done")
                    t += 1
                print()
            sl_no += 1
            print("Writing to master table : ", sl_no, i+"_"+j)
            cur.execute("INSERT INTO master VALUES (?,?)", (sl_no, i+"_"+j))
        
        conn.commit()
        # cur.execute("create index if not exists prod_index_"+i+" on Data_"+i+" (PRODUCT_DESCRIPTION)")
        # cur.execute("create index if not exists imp_index_"+i+" on Data_"+i+" (IMPORTER_NAME)")
        # cur.execute("create index if not exists sup_index_"+i+" on Data_"+i+" (SUPPLIER_NAME)")
        # conn.commit()


if __name__ == "__main__":
    check_new_file()

