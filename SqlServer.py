from flask import Flask
import pyodbc

def connection():
    s = '10.84.6.199' #Your server name
    d = 'DWH_SF'
    u = 'sa' #Your login
    p = '31zDM#OJ9f1g7h!&hsDR' #Your login password de AWS
    cstr = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+s+';DATABASE='+d+';UID='+u+';PWD='+ p
    conn = pyodbc.connect(cstr)
    return conn