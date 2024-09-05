import win32com.client
import os

os.chdir("C:\ImportData\ImportSearch\Scripts")
print(os.listdir())
xl=win32com.client.Dispatch("Excel.Application")
xl.DisplayAlerts = False
xl.Workbooks.Open(Filename="C:\ImportData\ImportSearch\Scripts\ALL_PORT_ALL_I_84_Dec23_Dec23_1.xlsx",ReadOnly=1)
wb = xl.Workbooks(1)
wb.SaveAs(Filename='C:\ImportData\ImportSearch\Scripts\ALL_PORT_ALL_I_84_Dec23_Dec23_1.csv', FileFormat='6') #6 means csv
wb.Close(False)
xl.Application.Quit()
wb=None 
xl=None
