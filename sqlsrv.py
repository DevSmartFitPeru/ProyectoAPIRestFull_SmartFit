import pymssql
conn = pymssql.connect(server='10.84.6.199', user='sa', password='31zDM#OJ9f1g7h!&hsDR', database='DWH_SF')
cursor = conn.cursor()
cursor.execute('SELECT CODIGO,UNIDAD,RUC FROM PROCESOS_SMARTFIT.SMARTFIT.UNIDADES_SMARTFIT_PERU')
row = cursor.fetchone()
while row:
        print(str(row[0]) + " " + str(row[1]) + " " + str(row[2]))
        row = cursor.fetchone()