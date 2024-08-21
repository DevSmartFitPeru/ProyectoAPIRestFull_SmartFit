import datetime as datetime
import openpyxl
from openpyxl import Workbook
import psycopg2
from flask import Flask, jsonify
from pyathena import connect
import requests
import csv
from datetime import datetime
import datetime
from datetime import date
app = Flask(__name__)

connposgresql = psycopg2.connect("postgresql://postgres:Pagamento2024$@dwh.ckioqeuxcht7.us-east-2.rds.amazonaws.com:5432/DWH")

@app.route('/location')
def location():
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute("SELECT * from prod_lake_modeled_refined.dim_locations where country ='Peru'")
        resultado = []
        for row in cursor:
            content = {
                'acronym':row[0] 
                 }
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
        cursor.close()

@app.route('/dimlocation')
def dimlocation():
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute(" select  acronym ,active ,opening_confirmed ,address ,built_area ,cnpj ,date_format(created_at, '%Y-%m-%d')created_at ,date_format(first_due_at, '%Y-%m-%d')first_due_at ,id ,latitude ,longitude ,name ,official_name ,date_format(real_opening_date, '%Y-%m-%d')real_opening_date ,unified_location_id ,state ,city ,district ,regional from prod_lake_modeled_refined.dim_locations where country = 'Peru' ")
        resultado = []
        for row in cursor:
            content = {
                'acronym':row[0],
                'active':row[1],
                'opening_confirmed':row[2],
                'address':row[3],
                'built_area':row[4],
                'cnpj':row[5],
                'created_at':row[6],
                'first_due_at':row[7],
                'id':row[8],
                'latitude':row[9],
                'longitude':row[10],
                'name':row[11],
                'official_name':row[12],
                'real_opening_date':row[13],
                'unified_location_id':row[14],
                'state':row[15],
                'city':row[16],
                'district':row[17],
                'regional':row[18]
                }
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
        cursor.close()

@app.route('/minifactu/<fecha_inicio>/<fecha_fin>')
def minifactu(fecha_inicio,fecha_fin):
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute("select id_payment,	status_pagamento,	date_format(payed_at, '%Y-%m-%d') payed_at,	amount_paid,	pag_elegivel,	propriedade,	forma_pagamento,	country,	acronym,	external_id,	minifactu_id,	errors,	validacao_erro,	retornou_minifactu,	error,	validacao_coerce,	retornou_front,	gross_value,	amount_pag_elegivel,	exportado_minifactu,	exportado_sistema_externo,	amount_validacao_coerce,	amount_retornou_minifactu,	amount_validacao_erro,	amount_retornou_front,	date_format(load_datetime, '%Y-%m-%d') load_datetime,	amount_exportado_sistema_externo,	amount_exportado_minifactu from prod_lake_modeled_refined.minifactu_otc where payed_at between cast('"+str(fecha_inicio)+"' as date) and  cast('"+str(fecha_fin)+"' as date) and country='Peru'")
        resultado = []
        for row in cursor:
            content = { 'id_payment':row[0],
            'status_pagamento':row[1],
            'payed_at':row[2],
            'amount_paid':row[3],
            'pag_elegivel':row[4],
            'propriedade':row[5],
            'forma_pagamento':row[6],
            'country':row[7],
            'acronym':row[8],
            'external_id':row[9],
            'minifactu_id':row[10],
            'errors':row[11],
            'validacao_erro':row[12],
            'retornou_minifactu':row[13],
            'error':row[14],
            'validacao_coerce':row[15],
            'retornou_front':row[16],
            'gross_value':row[17],
            'amount_pag_elegivel':row[18],
            'exportado_minifactu':row[19],
            'exportado_sistema_externo':row[20],
            'amount_validacao_coerce':row[21],
            'amount_retornou_minifactu':row[22],
            'amount_validacao_erro':row[23],
            'amount_retornou_front':row[24],
            'load_datetime':row[25],
            'amount_exportado_sistema_externo':row[26],
            'amount_exportado_minifactu':row[27] }
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
        cursor.close()

@app.route('/fin/<fecha_inicio>/<fecha_fin>')
def fin(fecha_inicio,fecha_fin):
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute("SELECT fin.id ,fin .external_id external_id_fin ,fin .origin_system ,fin .amount ,date_format(fin .due_on, '%Y-%m-%d')due_on ,fin.status_pagamento status_pagamento_fin ,fin .forma_pagamento ,fin .status_front , date_format(fin.data_sistema_front, '%Y-%m-%d %H:%i') data_sistema_front, fin.brand ,fin .operadora ,fin .acronym ,otc.country,otc .external_id ,otc.status_pagamento ,otc.errors,otc.error ,date_format(otc.payed_at, '%Y-%m-%d') payed_at, otc.id_payment FROM prod_lake_modeled_refined.fin_otc fin left join prod_lake_modeled_refined.minifactu_otc otc on fin .external_id = otc.id_payment where otc.payed_at between cast('"+str(fecha_inicio)+"' as date) and  cast('"+str(fecha_fin)+"' as date) and otc.country='Peru'")
        resultado = []
        for row in cursor:
            content = { 'id':row[0],
            'external_id_fin':row[1],
            'origin_system':row[2],
            'amount':row[3],
            'due_on':row[4],
            'status_pagamento_fin':row[5],
            'forma_pagamento':row[6],
            'status_front':row[7],
            'data_sistema_front':row[8],
            'brand':row[9],
            'operadora':row[10],
            'acronym':row[11],
            'country':row[12],
            'external_id':row[13],
            'status_pagamento':row[14],
            'errors':row[15],
            'error':row[16],
            'payed_at':row[17],
            'id_payment':row[18]}
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
             cursor.close()
@app.route('/monitor_otc/<fecha_inicio>/<fecha_fin>')
def monitor_otc(fecha_inicio,fecha_fin):
    try:

        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute("select date_format(payed_at, '%Y_%m_%d') payed_at,count(*) as Total_tx,sum(amount_paid) as Valorizado,country from prod_lake_modeled_refined.minifactu_otc where payed_at between cast('"+str(fecha_inicio) +"' as timestamp) and  cast('"+str(fecha_fin) +"' as timestamp) and country='Peru' group by payed_at,country order by payed_at  desc")
        resultado = []
        for row in cursor:
            content = { 'payed_at':row[0],'Total_tx':row[1],'Valorizado':row[2],'country':row[3] }

            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
        cursor.close()
@app.route('/error_minifactu/<fecha_inicio>/<fecha_fin>')
def error_minifactu(fecha_inicio,fecha_fin):
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute("select id_payment,	status_pagamento,	date_format(payed_at, '%Y-%m-%d') payed_at,	amount_paid,	pag_elegivel,	propriedade,	forma_pagamento,	country,	acronym,	external_id,	minifactu_id,	errors,	validacao_erro,	retornou_minifactu,	error,	validacao_coerce,	retornou_front,	gross_value,	amount_pag_elegivel,	exportado_minifactu,	exportado_sistema_externo,	amount_validacao_coerce,	amount_retornou_minifactu,	amount_validacao_erro,	amount_retornou_front,	date_format(load_datetime, '%Y-%m-%d') load_datetime,	amount_exportado_sistema_externo,	amount_exportado_minifactu from prod_lake_modeled_refined.minifactu_otc where date_format(payed_at, '%Y-%m-%d') between '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' and country='Peru' and errors  is not null")
        resultado = []
        for row in cursor:
            content = { 'id_payment':row[0],
            'status_pagamento':row[1],
            'payed_at':row[2],
            'amount_paid':row[3],
            'pag_elegivel':row[4],
            'propriedade':row[5],
            'forma_pagamento':row[6],
            'country':row[7],
            'acronym':row[8],
            'external_id':row[9],
            'minifactu_id':row[10],
            'errors':row[11],
            'validacao_erro':row[12],
            'retornou_minifactu':row[13],
            'error':row[14],
            'validacao_coerce':row[15],
            'retornou_front':row[16],
            'gross_value':row[17],
            'amount_pag_elegivel':row[18],
            'exportado_minifactu':row[19],
            'exportado_sistema_externo':row[20],
            'amount_validacao_coerce':row[21],
            'amount_retornou_minifactu':row[22],
            'amount_validacao_erro':row[23],
            'amount_retornou_front':row[24],
            'load_datetime':row[25],
            'amount_exportado_sistema_externo':row[26],
            'amount_exportado_minifactu':row[27] }
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
        cursor.close()
@app.route('/scheduled')
def scheduled():
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute("select id ,external_id ,origin_system ,amount ,date_format(due_on, '%Y-%m-%d') due_on ,status_pagamento ,forma_pagamento ,status_front ,brand ,operadora ,acronym ,country , amount_sent_operadora ,date_format(load_datetime, '%Y-%m-%d %H:%i:%s')  load_datetime from prod_lake_modeled_refined.fin_otc where status_pagamento = 'rejeitado' and operadora in ('peru_interbank','VisanetPERU','MCprocesosPERU') and data_sistema_front is null and status_front ='scheduled'")
        resultado = []
        for row in cursor:
            content = {
            'id':row[0],
            'external_id':row[1],
            'origin_system':row[2],
            'amount':row[3],
            'due_on':row[4],
            'status_pagamento':row[5],
            'forma_pagamento':row[6],
            'status_front':row[7],
            'brand':row[8],
            'operadora':row[9],
            'acronym':row[10],
            'country':row[11],
            'amount_sent_operadora':row[12],
            'load_datetime':row[13]}
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
             cursor.close()
@app.route('/oic/<fecha_inicio>')
def oic(fecha_inicio):

    try:
        url = "https://elal-test.fa.la1.oraclecloud.com/fscmRestApi/resources/11.13.18.05/receivablesInvoices?finder=invoiceSearch;TransactionDate="''+str(fecha_inicio)+''",BusinessUnit=PE03 - SUR"
        payload = {}
        headers = {
            'Authorization': 'Basic Zml0LmNvbnN1bHRpbmc6MFJAY2wzRiF0MjAyMg==',
            'Cookie': 'ak_bmsc=7C90400BB1678DBCDD3B5B686605B14F~000000000000000000000000000000~YAAQxQ/EFx6OlDiFAQAAElPyeBIvsAeR0ZGnc2PDBEEI8mQTsuxLKrwtxhjwCex/tE9xT5fusapfry4ekafWF9R1q3AGh2yfbC+xK5bkEtwQ8ViCQSZCcSYBu8RQI7uV2Szub2beSLFpLU9oQkLx9BQgmqCFlMtTZNHVRjmCc3mSHlEWuF0iTNBkIHWTrJjQFlXFTqBEIUNj8lhsc2H8mr0lPix4UXeH7Tr7Vp+RG4Jq7YyvtQ/2AEbfjCKEcDDAGaQz5gSPh0qRl72lag5ZD5app6gbYfdGT24M4mjp/uXgILdNdK11bf2DOiHDUeLLGn0ug8j2okBTaeaRok/0ICOAOVwb0mGu81YEgOn1eS3C8t1LnY93uKdg1cIj2BXMU7UzzLn2psU=; bm_sv=B4D4EA7B251ECCA892CA993D8F73B228~YAAQxQ/EF6yOlDiFAQAAB5ryeBIB01cq01kBkXcgiTkLofM1kqzIkJMi0o+YwSbnws+2Lu2ohurEMBlq6ZdXSxqm3avND1WnCd+nXKnUQZMvfVxhCQtYeTK7x8Au6/9PfnK0M+B4coq+EG0QWT3w3dZ9CaH5QJk+c5pNCH6g9dqTusV6shqjn3qBj/sD9Bfdlc6VnH9/+oSJ4ZY0e8NVSBNnGuIWqqG3elJX9IshcqNQdiru0KAvCsUq7rk3X2K+AT2rvyx5hBwB8MrF~1'
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        #print(response.text)
        return jsonify(response.json())

    except Exception as e:
        print(e)

@app.route('/invoice_latam')
def invoice_latam():
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute("select MONTH(payed_at) as month,year(payed_at) year,country,count(*) as Total_TX,sum(amount_paid) as Valorizado from prod_lake_modeled_refined.minifactu_otc where payed_at between cast('2023-01-01 00:00:00' as timestamp) and  cast('2023-12-31 00:00:00' as timestamp) and country in('Peru','Colômbia','México','Brasil','Chile') group by country,MONTH(payed_at) ,year(payed_at) order by Total_TX desc")
        resultado = []
        for row in cursor:
            content = {
            'month':row[0],
            'year':row[1],
            'country': row[2],
            'Total_TX':row[3],
            'Valorizado':row[4]}
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
             cursor.close()

@app.route('/promociones')
def promociones():
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute("select id indice ,code promocion_cod ,case when id = 11832 then 'Promo Cierre' when id = 11983 then 'Promo Enero Black' when id = 12198 then 'Promo Febrero Black' when id = 12401 then 'Promo Febrero FLASH' when id = 12448 then 'Promo Marzo' when id = 12543 then 'Fit Friend' when id = 12688 then 'Descuento Black' when id = 13071 then 'Maraton Adidas' when id = 13127 then 'Promo Mayo FLASH' when id = 13450 then 'Cierre semestre' when id = 13565 then 'Te exoneramos tu deuda' when id = 13600 then 'PromoJulio' when id = 14041 then 'Promo transversal' when id = 14233 then 'Promo Setiembre' else 'validar' end nombre_promocion ,case when id = 11832 then 'Promo Cierre 2022' when id = 11983 then 'Promo Enero Black 2023' when id = 12198 then 'Promo Febrero Black 2023' when id = 12401 then 'Promo Febrero FLASH 2023' when id = 12448 then 'Promo Marzo 2023' when id = 12543 then 'Fit Friend 2023' when id = 12688 then 'Descuento Black 2023' when id = 13071 then 'Maraton Adidas 2023' when id = 13127 then 'Promo Mayo FLASH 2023' when id = 13450 then 'Cierre semestre 2023' when id = 13565 then 'Te exoneramos tu deuda 2023' when id = 13600 then 'PromoJulio 2023' when id = 14041 then 'Promo transversal' when id = 14233 then 'Promo Setiembre' else 'validar' end nombre_promocion_short ,concat(date_format(starts_at, '%Y-%m-%d'),' ','al',' ',date_format(expires_at, '%Y-%m-%d')) vigencia ,date_format(starts_at, '%Y-%m-%d')fecha_inicio ,date_format(expires_at, '%Y-%m-%d')fecha_fin ,title descripcion ,'Todas'sedes ,'Unica'tipo ,''stock from prod_lake_modeled_refined.dim_promotions where id in (11832, 11983, 12198, 12401, 12448, 12543, 12688, 13071, 13127, 13450, 13600, 13565, 14041, 14233) 	")
        resultado = []
        for row in cursor:
            content = {
            'indice':row[0],
            'promocion_cod':row[1],
            'nombre_promocion':row[2],
            'nombre_promocion_short':row[3],
            'vigencia':row[4],
            'fecha_inicio':row[5],
            'fecha_fin':row[6],
            'descripcion':row[7],
            'sedes':row[8],
            'tipo':row[9],
            'stock':row[10]
            }
            resultado.append(content)
        return jsonify(resultado)
    except Exception as e:
        print(e)
    finally:
             cursor.close()
#ws_inadimplentes_analitico
@app.route('/inadimplentes_analitico/<fecha_inicio>/<fecha_fin>')
def inadimplentes_analitico(fecha_inicio,fecha_fin):
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute(" select id_pagamento CODIGO_PAGAMENTO ,matricula CODIGO_MATRICULA ,nome NOMBRE_CLIENTE ,email EMAIL ,pais PAIS ,estado REGION ,cidade CIUDAD ,sigla_unidade CODIGO_UNIDAD ,nome_unidade GIMNACIO ,plano PLAN_CLIENTE ,tipo_pagamento TIPO_PAGO ,tipo_pagamento_conceito TIPO_CONCEPTO ,valor VALOR_CONCEPTO ,valor VALOR_TOTAL_CONCEPTO ,status ESTADO_PAGO ,status_usuario ESTADO_USUARIO ,date_format (data_vencimento,'%Y-%m-%d') FECHA_VENCIMIENTO ,date_format(NOW(),'%Y-%m-%d') FECHA_PROCESAMIENTO  from prod_lake_modeled_refined.inadimplentes_analitico where pais='Peru' and date_format (data_vencimento,'%Y-%m-%d') between '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' ")
        resultado = []
        for row in cursor:
            content = {
                        'CODIGO_PAGAMENTO': row[0],
                        'CODIGO_MATRICULA': row[1],
                        'NOMBRE_CLIENTE': row[2],
                        'EMAIL': row[3],
                        'PAIS': row[4],
                        'REGION': row[5],
                        'CIUDAD': row[6],
                        'CODIGO_UNIDAD': row[7],
                        'GIMNACIO': row[8],
                        'PLAN_CLIENTE': row[9],
                        'TIPO_PAGO': row[10],
                        'TIPO_CONCEPTO': row[11],
                        'VALOR_CONCEPTO': row[12],
                        'VALOR_TOTAL_CONCEPTO': row[13],
                        'ESTADO_PAGO': row[14],
                        'ESTADO_USUARIO': row[15],
                        'FECHA_VENCIMIENTO': row[16],
                        'FECHA_PROCESAMIENTO': row[17]
                     }
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
             cursor.close()

#ws_cancelamientos
@app.route('/cancelamientos/<fecha_inicio>/<fecha_fin>')
def cancelamientos(fecha_inicio,fecha_fin):
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute(" select acronym CODIGO_UNIDAD ,propriedade PROPIEDAD ,matricula COD_MATRICULA ,nome NOMBRES ,document_cpf NRO_DNI ,date_format(birthday,'%Y-%m-%d') FECHA_NACIMIENTO ,age EDAD ,email EMAIL ,case 	when telefono is not null then 'CellPhone' 	else null end TIPO_TELEFONO ,telefono NRO_CELULAR ,tipo_cancelamento TIPO_CANCELAMIENTO ,motivo_cancelamento MOTIVO_CANCELAMIENTO ,replace(promocao,'|',' ') PROMOCION ,pais PAIS ,date_format(data_pedido_cancelamento,'%Y-%m-%d')  FECHA_SOLICITUD_CANCELAMIENTO ,date_format(data_cancelamento,'%Y-%m-%d') FECHA_CANCELAMIENTO ,plano_plan PLAN_CLIENTE ,date_format(data_compra_plano,'%Y-%m-%d') FECHA_COMPRA_PLAN ,date_format(load_datetime,'%Y-%m-%d') FECHA_PROCESAMIENTO from prod_lake_refined_modeled_latam_lgpd.cancelados where lower(pais) = 'peru' and date_format(data_cancelamento,'%Y-%m-%d') between '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"'  ")
        resultado = []
        for row in cursor:
            content = {
            'CODIGO_UNIDAD':row[0],
            'PROPIEDAD':row[1],
            'COD_MATRICULA':row[2],
            'NOMBRES':row[3],
            'NRO_DNI':row[4],
            'FECHA_NACIMIENTO':row[5],
            'EDAD':row[6],
            'EMAIL':row[7],
            'TIPO_TELEFONO':row[8],
            'NRO_CELULAR':row[9],
            'TIPO_CANCELAMIENTO':row[10],
            'MOTIVO_CANCELAMIENTO':row[11],
            'PROMOCION':row[12],
            'PAIS':row[13],
            'FECHA_SOLICITUD_CANCELAMIENTO':row[14],
            'FECHA_CANCELAMIENTO':row[15],
            'PLAN_CLIENTE':row[16],
            'FECHA_COMPRA_PLAN':row[17],
            'FECHA_PROCESAMIENTO': row[18]
            }
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
             cursor.close()

#ws_alumnoshistoricos
@app.route('/alumnoshistoricos/<plan>/<fecha_inicio>/<fecha_fin>')
def alumnoshistoricos(plan,fecha_inicio,fecha_fin):
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute(" select person_id CODIGO_MATRICULA ,name NOMBRE_CLIENTE ,acronym CODIGO_UNIDAD ,plan PLAN_ACTUAL ,date_format(purchase_date,'%Y-%m-%d') FECHA_COMPRA ,documento NRO_DOCUMENTO ,cached_status ESTADO_CLIENTE ,date_format(cancel_date,'%Y-%m-%d') FECHA_CANCELACION ,cancelling_reason MOTIVO_CANCELACION ,country PAIS ,kind KIND ,replace(promotion_title,'|',' ') PROMOCION_HIS ,email EMAIL_CLIENTE ,date_format(birthday,'%Y-%m-%d') FECHA_NACIMIENTO_CLIENTE ,gender SEXO ,state DEPARTAMENTO ,city PROVINCIA ,age EDAD ,contrato CONTRATO from prod_lake_refined_modeled_latam_lgpd.alunos_historico where lower(country)='peru' and plan = '"+str(plan)+"' and date_format (purchase_date,'%Y-%m-%d') between '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' ")
        resultado = []
        for row in cursor:
            content = {
            'CODIGO_MATRICULA':row[0],
            'NOMBRE_CLIENTE':row[1],
            'CODIGO_UNIDAD':row[2],
            'PLAN_ACTUAL':row[3],
            'FECHA_COMPRA':row[4],
            'NRO_DOCUMENTO':row[5],
            'ESTADO_CLIENTE':row[6],
            'FECHA_CANCELACION':row[7],
            'MOTIVO_CANCELACION':row[8],
            'PAIS':row[9],
            'KIND':row[10],
            'PROMOCION_HIS':row[11],
            'EMAIL_CLIENTE':row[12],
            'FECHA_NACIMIENTO_CLIENTE':row[13],
            'SEXO':row[14],
            'DEPARTAMENTO':row[15],
            'PROVINCIA':row[16],
            'EDAD':row[17],
            'CONTRATO':row[18],
            }
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
             cursor.close()

#ws_precios
@app.route('/pricing')
def pricing():
        try:
            cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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",s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/",region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
            cursor.execute("select prec.id ID_PRECIO,prec.location_id ID_UNIDAD,loc.acronym COD_UNIDAD,prec.plan_id PLAN_ID,pla.name NOMBRE_PLAN,prec.price PRECIO, date_format(prec.created_at, '%Y-%m-%d')  FECHA_CREACION,date_format(prec.updated_at, '%Y-%m-%d')  FECHA_ACTUALIZACION,prec.contract CONTRACT,prec.annual_prices PRECIO_ANUAL, date_format( prec.load_datetime, '%Y-%m-%d')  FECHA_CARGA,date_format(prec.reference_date, '%Y-%m-%d')  FECHA_REFERENCIA from prod_lake_modeled_refined.prices_empilhado prec left join prod_lake_modeled_refined.dim_locations loc on prec.location_id  = loc.id left join prod_lake_modeled_refined.dim_plans pla on prec.plan_id = pla.id where loc.country = 'Peru'and date(reference_date) = date_add('day',-1,current_date)")
            resultado = []
            for row in cursor:
                content = {
                        'ID_PRECIO': row[0],
                        'ID_UNIDAD': row[1],
                        'COD_UNIDAD': row[2],
                        'PLAN_ID': row[3],
                        'NOMBRE_PLAN': row[4],
                        'PRECIO': row[5],
                        'FECHA_CREACION': row[6],
                        'FECHA_ACTUALIZACION': row[7],
                        'CONTRACT': row[8],
                        'PRECIO_ANUAL': row[9],
                        'FECHA_CARGA': row[10],
                        'FECHA_REFERENCIA': row[11]}
                resultado.append(content)
            return jsonify(resultado)
        except Exception as e:
            print(e)
        finally:
            cursor.close()
#ws_alumnosactivos
@app.route('/alumnosactivos/<plan>')
def alumnosactivos(plan):
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute(" select matricula CODIGO_MATRICULA ,sigla CODIGO_UNIDAD ,unidad UNIDAD ,nombre NOMBRE_CLIENTE ,correo EMAIL ,documento NRO_DOCUMENTO ,date_format(fecha_nacimiento,'%Y-%m-%d') FECHA_NACIMIENTO ,celular CELULAR ,plan PLAN_ACTUAL ,date_format(fecha_compra,'%Y-%m-%d') FECHA_COMPRA ,date_format(fecha_mensualidad,'%Y-%m-%d') FECHA_EXPIRACION_MEMBERSHIP ,replace(promocion, '|',' ')PROMOCION ,status_trinquete ESTADO_CATRACA_TORNIQUETE ,date_format(vencimento_mensualidad,'%Y-%m-%d') FECHA_VENCIMIENTO_MENSUALIDAD ,precio_mensualidad PRECIO_MENSUALIDAD ,edad EDAD ,date_format(load_datetime,'%Y-%m-%d') FECHA_PROCESAMIENTO from prod_lake_refined_modeled_latam_lgpd.alunos_ativos where lower(pais) = 'peru' and plan = '"+str(plan)+"' ")
        resultado = []
        for row in cursor:
            content = {
            'CODIGO_MATRICULA':row[0],
            'CODIGO_UNIDAD':row[1],
            'UNIDAD':row[2],
            'NOMBRE_CLIENTE':row[3],
            'EMAIL':row[4],
            'NRO_DOCUMENTO':row[5],
            'FECHA_NACIMIENTO':row[6],
            'CELULAR':row[7],
            'PLAN_ACTUAL':row[8],
            'FECHA_COMPRA':row[9],
            'FECHA_EXPIRACION_MEMBERSHIP':row[10],
            'PROMOCION':row[11],
            'ESTADO_CATRACA_TORNIQUETE':row[12],
            'FECHA_VENCIMIENTO_MENSUALIDAD':row[13],
            'PRECIO_MENSUALIDAD':row[14],
            'EDAD':row[15],
            'FECHA_PROCESAMIENTO':row[16]}
            resultado.append(content)
        return jsonify(resultado)
    except Exception as e:
        print(e)
    finally:
             cursor.close()


#ws_accesos
@app.route('/accesos/<plan>/<fecha_inicio>/<fecha_fin>')
def accesos(plan,fecha_inicio,fecha_fin):
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute("select person_id CODIGO_CLIENTE ,purchase_id ID_COMPRA ,name NOMBRES ,cached_status ESTADO_CLIENTE ,acronym CODIGO_UNIDAD ,date_format(cancelled,'%Y-%m-%d')FECHA_CANCELAMIENTO ,date_format(purchase_date ,'%Y-%m-%d')FECHA_COMPRA ,plan PLAN_CLIENTE ,document NRO_DOCUMENTO ,date_format(due_at ,'%Y-%m-%d') DUE_AT  ,access_acronym  ACCESO_ACRONYM ,date_format(date,'%Y-%m-%d') FECHA ,date_format(NOW(),'%Y-%m-%d') FECHA_PROCESAMIENTO  from prod_lake_refined_modeled_latam_lgpd.acessos_detalhados where lower(pais) ='peru'and plan = '"+str(plan)+"' and date_format (date,'%Y-%m-%d') between '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"'")
        resultado = []
        for row in cursor:
            content = {
            'CODIGO_CLIENTE':row[0],
            'ID_COMPRA':row[1],
            'NOMBRES':row[2],
            'ESTADO_CLIENTE':row[3],
            'CODIGO_UNIDAD':row[4],
            'FECHA_CANCELAMIENTO':row[5],
            'FECHA_COMPRA':row[6],
            'PLAN_CLIENTE':row[7],
            'NRO_DOCUMENTO':row[8],
            'DUE_AT':row[9],
            'ACCESO_ACRONYM':row[10],
            'FECHA':row[11],
            'FECHA_PROCESAMIENTO': row[12]
            }
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
             cursor.close()
@app.route('/odatabacklog/<fecha_inicio>/<fecha_fin>')
def odatabacklog(fecha_inicio,fecha_fin):
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute(" select ano_mes ,ano_mes_criacao ,pais ,reference_date ,id_atualizacao ,date_format(data_atualizacao, '%Y-%m-%d')data_atualizacao ,incidentid ,date_format(responder_antes_de, '%Y-%m-%d')responder_antes_de ,date_format(data_alvo_sla, '%Y-%m-%d')data_alvo_sla ,date_format(respondido_em, '%Y-%m-%d')respondido_em ,date_format(resolvido_em, '%Y-%m-%d')resolvido_em ,date_format(fechado_em, '%Y-%m-%d')fechado_em ,custos ,avaliacao_atendimento ,escalonado ,desescalonado ,respondido ,fechado ,finalizado ,numero_chamado ,date_format(data_criacao_chamado, '%Y-%m-%d')data_criacao_chamado ,titulo_chamado ,nome_solicitante ,numero_externo ,categoria ,subcategoria ,prioridade ,servico ,duracao_sla ,tipo_chamado ,registro ,impacto ,urgencia ,gerencia ,departamento_grupo_operador ,grupo_operadores ,operador ,fornecedores ,status ,motivo_escalamento ,unidade ,regional_senior ,login_de_rede ,cargo ,departamento ,date_format(load_datetime, '%Y-%m-%d')load_datetime ,date_format(last_requisition_date, '%Y-%m-%d')last_requisition_date ,ultimo_status ,ultimo_grupo_operador ,ultima_gerencia ,date_format(ultima_data_atualizacao, '%Y-%m-%d')ultima_data_atualizacao ,status_encerrado_ou_resolvido ,contagem_unidade from prod_lake_modeled_refined.topdesk_odata_backlog where lower(pais) ='peru' and data_criacao_chamado between cast('"+str(fecha_inicio)+"' as date) and  cast('"+str(fecha_fin)+"' as date) ")
        resultado = []
        for row in cursor:
            content = { 'ano_mes':row[0],
            'ano_mes_criacao':row[1],
            'pais':row[2],
            'reference_date':row[3],
            'id_atualizacao':row[4],
            'data_atualizacao':row[5],
            'incidentid':row[6],
            'responder_antes_de':row[7],
            'data_alvo_sla':row[8],
            'respondido_em':row[9],
            'resolvido_em':row[10],
            'fechado_em':row[11],
            'custos':row[12],
            'avaliacao_atendimento':row[13],
            'escalonado':row[14],
            'desescalonado':row[15],
            'respondido':row[16],
            'fechado':row[17],
            'finalizado':row[18],
            'numero_chamado':row[19],
            'data_criacao_chamado':row[20],
            'titulo_chamado':row[21],
            'nome_solicitante':row[22],
            'numero_externo':row[23],
            'categoria':row[24],
            'subcategoria':row[25],
            'prioridade':row[26],
            'servico':row[27],
            'duracao_sla':row[28],
            'tipo_chamado':row[29],
            'registro':row[30],
            'impacto':row[31],
            'urgencia':row[32],
            'gerencia':row[33],
            'departamento_grupo_operador':row[34],
            'grupo_operadores':row[35],
            'operador':row[36],
            'fornecedores':row[37],
            'status':row[38],
            'motivo_escalamento':row[39],
            'unidade':row[40],
            'regional_senior':row[41],
            'login_de_rede':row[42],
            'cargo':row[43],
            'departamento':row[44],
            'load_datetime':row[45],
            'last_requisition_date':row[46],
            'ultimo_status':row[47],
            'ultimo_grupo_operador':row[48],
            'ultima_gerencia':row[49],
            'ultima_data_atualizacao':row[50],
            'status_encerrado_ou_resolvido':row[51],
            'contagem_unidade':row[52]}
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
        cursor.close()

#API KPIS
@app.route('/kpis/<fecha_inicio>/<fecha_fin>')
def kpis(fecha_inicio,fecha_fin):
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute(" select date_format(load_datetime ,'%Y-%m-%d')LOAD_DATE ,date_format(reference_date ,'%Y-%m-%d')REFERENCE_DATE ,acronym ACRONYM ,kpi KPI ,plan_name PLAN_NAME ,channel CHANNEL ,origin ORIGIN ,qtd QTD from prod_lake_modeled_refined.kpis_diarios where acronym in (select acronym from prod_lake_modeled_refined.dim_locations where country = 'Peru' ) and kpi in ('Acessos', 'Ativos', 'Inadimplentes', 'Visitas', 'Venda Mês','Evasão') and date_format (reference_date,'%Y-%m-%d') between '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' ")
        resultado = []
        for row in cursor:
            content = {
            'LOAD_DATE':row[0],
            'REFERENCE_DATE':row[1],
            'ACRONYM':row[2],
            'KPI':row[3],
            'PLAN_NAME':row[4],
            'CHANNEL':row[5],
            'ORIGIN':row[6],
            'QTD':row[7]
            }
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
             cursor.close()

#API KPIS_PROMOCIONES
@app.route('/kpipromociones/<fecha_inicio>/<fecha_fin>')
def kpipromociones(fecha_inicio,fecha_fin):
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute(" with payments as (select 	purc. id purchase_id 	,otc.id_payment 	,pay.payed_at 	,pay.due_at  	,pay.state payment_state 	,coalesce(prod.description,pay.payable_type)  product_name 	,pay.amount_paid 	,otc.forma_pagamento 	,loc.acronym acronym_payet from 	prod_lake_modeled_refined.minifactu_otc otc left join prod_lake_ss_refined.payments pay on 	otc.id_payment = pay.id left join prod_lake_ss_refined.memberships mem on 	pay.payable_id = mem.id and lower(pay.payable_type) = 'membership' left join prod_lake_modeled_refined.dim_locations loc on 	pay.location_id = loc.id 	and loc.country = 'Peru' left join prod_lake_ss_refined.services ser on 	pay.payable_id = ser.id and lower(pay.payable_type) = 'service' left join prod_lake_ss_refined.products prod on 	ser.product_id = prod.id left join prod_lake_ss_refined.purchases purc on 	purc.id = case when pay.payable_type = 'Membership' then mem.purchase_id else ser.purchase_id end ) select 	 purc.promotion_id id_promocion 	,date_format(prom.starts_at,'%Y-%m-%d') fecha_inicio_promo 	,date_format(prom.expires_at,'%Y-%m-%d') vigencia_promo 	,prom.active estado_promocion 	,replace(prom.title, '|', ' ')title 	,purc.id id_compra 	,date_format(purc.created_at,'%Y-%m-%d')fecha_de_compra 	,date_format(purc.confirmed_at,'%Y-%m-%d') fecha_de_confirm_compra 	,date_format(purc.cancelled,'%Y-%m-%d') fecha_cancel_compra 	,date_format(purc.expired_at,'%Y-%m-%d') fecha_exp_compra 	,purc.person_id cod_matricula 	,cli.cliente_nome nombre_cliente 	,cli.cliente_idade edad_cliente 	,peo.status_aluno 	,peo.status_catraca 	,plan.name nombre_plan 	,loc.acronym cod_unidad_compra 	,case 	 		when purc.created_at is not null and purc.expired_at is null then 'Activo' 	 		else 'Cancelado' 	end status_compra 	,p.id_payment payment_id 	,date_format(p.due_at,'%Y-%m-%d')due_at 	,date_format(p.payed_at,'%Y-%m-%d')payed_at 	,p.amount_paid 	,p.payment_state 	,date_format(purc.load_datetime,'%Y-%m-%d')load_datetime from prod_lake_ss_refined.purchases purc left join prod_lake_modeled_salesforce_latam.salesforce_dim_clientes_latam cli on 	cast(purc.person_id as varchar(30)) = cli.cliente_person_id and cli.cliente_pais = 'Peru' left join prod_lake_modeled_refined.dim_people peo on 	purc.person_id = peo.person_id left join prod_lake_modeled_refined.dim_locations loc on 	purc.original_location_id = loc.id left join prod_lake_modeled_refined.dim_promotions prom on 	purc.promotion_id = prom.id left join prod_lake_modeled_refined.dim_plans plan on 	purc.plan_id = plan.id left join payments p on 	purc.id = p.purchase_id where purc.original_location_id in (select id from prod_lake_modeled_refined.dim_locations where country='Peru') and purc.promotion_id  in (11832, 11983, 12198, 12401, 12448, 12543, 12688, 13071, 13127, 13450, 13600, 13565, 14041, 14233) and date_format(purc.created_at,'%Y-%m-%d') between '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' ")
        resultado = []
        for row in cursor:
            content = {
                        'id_promocion': row[0],
                        'fecha_inicio_promo': row[1],
                        'vigencia_promo': row[2],
                        'estado_promocion': row[3],
                        'title': row[4],
                        'id_compra': row[5],
                        'fecha_de_compra': row[6],
                        'fecha_de_confirm_compra': row[7],
                        'fecha_cancel_compra': row[8],
                        'fecha_exp_compra': row[9],
                        'cod_matricula': row[10],
                        'nombre_cliente': row[11],
                        'edad_cliente': row[12],
                        'status_aluno': row[13],
                        'status_catraca': row[14],
                        'nombre_plan': row[15],
                        'cod_unidad_compra': row[16],
                        'status_compra': row[17],
                        'payment_id': row[18],
                        'due_at': row[19],
                        'payed_at': row[20],
                        'amount_paid': row[21],
                        'payment_state': row[22],
                        'load_datetime': row[23]
                     }
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
             cursor.close()

@app.route('/salescoporate/<fecha_inicio>/<fecha_fin>')
def salescoporate(fecha_inicio,fecha_fin):
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute("select dpc.country PAIS ,dpc.state DEPARTAMENTO ,dpc.acronym COD_UNIDAD ,dpc.purchase_created_at FECHA_COMPRA,dpc.purchase_id  ID_COMPRA,dpc.person_id  COD_MATRICULA,dpc.count_purchases CONT_COMPRA,dpc.purchase_original_plan_id  ID_PLAN_ORIGINAL_COMPRA,dpc.membership_current_plan_id  ID_MEMBRESIA_PLAN_ACTUAL,dpc.purchase_current_plan_id ID_PLAN_COMPRA_ACTUAL,dpc.membership_current_start_at FECHA_INICIO_MEMBRESIA_ACTUAL,dpc.membership_current_end_at  FECHA_FIN_MEMBRESIA_ACTUAL,dpc.membership_payed_at FECHA_PAGO_MEMBRESIA,dpc.membership_payment_state ESTADO_PAGO_MEMBRESIA,dpc.membership_original_amount_paid MONTO_PAGADA,dpc.membership_price PRECIO_MEMBRESIA,dpc.purchase_kind TIPO_COMPRA,dpc.cancelling_reason_other RAZON_CANCELAMIENTO,dpc.purchase_promotion_id ID_PROMOCION,pr.starts_at FECHA_INICIO_PROMOCION,pr.expires_at FECHA_FIN_PROMCION,pr.code COD_PROMOCION,pr.available_codes CANT_COD_ASIGNADOS,replace(replace (pr.description,'|',''),'/',' ') DESC_PROMOCION, replace(replace (pr.title,'|',''),'/', ' ') TITULO_PROM, pr.active ESTADO_PROM,pr.active ESTADO_PROM from prod_lake_modeled_refined.daily_purchases_caches dpc left join prod_lake_modeled_refined.dim_promotions pr on dpc.purchase_promotion_id = pr.id where country ='Peru'and date_format (purchase_created_at,'%Y-%m-%d') between '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' and dpc.purchase_promotion_id is not null")
        resultado = []
        for row in cursor:
            content = {
                        'PAIS': row[0],
                        'DEPARTAMENTO': row[1],
                        'COD_UNIDAD': row[2],
                        'FECHA_COMPRA': row[3],
                        'ID_COMPRA': row[4],
                        'COD_MATRICULA': row[5],
                        'CONT_COMPRA': row[6],
                        'ID_PLAN_ORIGINAL_COMPRA': row[7],
                        'ID_MEMBRESIA_PLAN_ACTUAL': row[8],
                        'ID_PLAN_COMPRA_ACTUAL': row[9],
                        'FECHA_INICIO_MEMBRESIA_ACTUAL': row[10],
                        'FECHA_FIN_MEMBRESIA_ACTUAL': row[11],
                        'FECHA_PAGO_MEMBRESIA': row[12],
                        'ESTADO_PAGO_MEMBRESIA': row[13],
                        'MONTO_PAGADA': row[14],
                        'PRECIO_MEMBRESIA': row[15],
                        'TIPO_COMPRA': row[16],
                        'RAZON_CANCELAMIENTO': row[17],
                        'ID_PROMOCION': row[18],
                        'FECHA_INICIO_PROMOCION': row[19],
                        'FECHA_FIN_PROMCION': row[20],
                        'COD_PROMOCION': row[21],
                        'CANT_COD_ASIGNADOS': row[22],
                        'DESC_PROMOCION': row[23],
                        'TITULO_PROM': row[24],
                        'ESTADO_PROM': row[25]}
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
             cursor.close()
#resumen tx por dia
@app.route('/summary_tx/<fecha_inicio>/<fecha_fin>')
def summary_tx(fecha_inicio,fecha_fin):
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute("select date_format(payed_at,'%Y-%m-%d')payed_at ,count(id_payment) Total_TX ,sum(gross_value) valorizado ,country from prod_lake_modeled_refined.minifactu_otc where date_format (payed_at,'%Y-%m-%d') between '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' and lower(country)='peru' group by payed_at,country order by payed_at desc")
        resultado = []
        for row in cursor:
            content = {
            'payed_at':row[0],
            'Total_TX':row[1],
            'valorizado':row[2],
            'country':row[3]}
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
             cursor.close()

@app.route('/vta_corporativa/<fecha_inicio>/<fecha_fin>')
def vta_corporativa(fecha_inicio,fecha_fin):
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute("select date_format(pur.created_at,'%Y-%m-%d') fecha_de_compra ,date_format(pur.confirmed_at,'%Y-%m-%d')  fecha_de_confirm_compra ,date_format(pur.cancelled,'%Y-%m-%d')  fecha_cancel_compra ,pur.promotion_id id_promocion ,substring(prom.code,3,20)empresa ,prom.code  cod_promocion ,pur.person_id cod_matricula ,date_format(prom.starts_at,'%Y-%m-%d')  fecha_inicio_promocion ,date_format(prom.expires_at,'%Y-%m-%d') fecha_fin_promocion ,prom.available_codes codigos_asignados ,prom.description desc_promocion ,cli.cliente_nome nombre_cliente ,cli.cliente_idade  edad_cliente ,cli.cliente_genero genero_cliente ,cli.flag_status_cliente  estado_cliente ,cli.ultimo_acesso f_ultimo_acesso ,cli.plan_name nombre_plan ,cli.pgto_status estado_pago ,cli.sigla_unidade sigla_unidad ,cli.nome_unidade nombre_unidad ,'Compra Personal'tipo_compra from prod_lake_ss_refined.purchases pur left join prod_lake_modeled_refined.dim_promotions prom on pur.promotion_id = prom.id left join prod_lake_modeled_salesforce_latam.salesforce_dim_clientes_latam cli on cast(pur.person_id as varchar(50)) = cli.cliente_person_id where date_format (pur.confirmed_at,'%Y-%m-%d') between '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' and lower(prom.description)  like '%personal%' and lower(cli.cliente_pais) = 'peru' union all select date_format(pur.created_at,'%Y-%m-%d') fecha_de_compra1 ,date_format(pur.confirmed_at,'%Y-%m-%d')  fecha_de_confirm_compra1 ,date_format(pur.cancelled,'%Y-%m-%d')  fecha_cancel_compra1 ,pur.promotion_id id_promocion1 ,null empresa1 ,prom.code  cod_promocion1 ,pur.person_id cod_matricula1 ,date_format(prom.starts_at,'%Y-%m-%d')  fecha_inicio_promocion1 ,date_format(prom.expires_at,'%Y-%m-%d') fecha_fin_promocion1 ,prom.available_codes codigos_asignados1 ,prom.description desc_promocion1 ,cli.cliente_nome nombre_cliente1 ,cli.cliente_idade  edad_cliente1 ,cli.cliente_genero genero_cliente1 ,cli.flag_status_cliente  estado_cliente1 ,cli.ultimo_acesso f_ultimo_acesso1 ,cli.plan_name nombre_plan1 ,cli.pgto_status estado_pago1 ,cli.sigla_unidade sigla_unidad1 ,cli.nome_unidade nombre_unidad1 ,'Compra Anticipada'tipo_compra1 from prod_lake_ss_refined.purchases pur left join prod_lake_modeled_refined.dim_promotions prom on pur.promotion_id = prom.id left join prod_lake_modeled_salesforce_latam.salesforce_dim_clientes_latam cli on cast(pur.person_id as varchar(50)) = cli.cliente_person_id where date_format (pur.confirmed_at,'%Y-%m-%d') between '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' and lower(prom.description)  like '%anticipada%' and lower(cli.cliente_pais) = 'peru' ")
        resultado = []
        for row in cursor:
            content = {
            'fecha_de_compra':row[0],
            'fecha_de_confirm_compra':row[1],
            'fecha_cancel_compra':row[2],
            'id_promocion':row[3],
            'empresa':row[4],
            'cod_promocion':row[5],
            'cod_matricula':row[6],
            'fecha_inicio_promocion':row[7],
            'fecha_fin_promocion':row[8],
            'codigos_asignados':row[9],
            'desc_promocion':row[10],
            'nombre_cliente':row[11],
            'edad_cliente':row[12],
            'genero_cliente':row[13],
            'estado_cliente':row[14],
            'f_ultimo_acesso':row[15],
            'nombre_plan':row[16],
            'estado_pago':row[17],
            'sigla_unidad':row[18],
            'nombre_unidad':row[19],
            'tipo_compra':row[20]}
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
             cursor.close()
@app.route('/transaction_latam/<fecha_inicio>/<fecha_fin>')
def transaction_latam(fecha_inicio,fecha_fin):
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute("select country ,acronym ,id_payment,date_format (payed_at,'%Y-%m-%d') payed_at ,amount_paid ,minifactu_id,forma_pagamento  from prod_lake_modeled_refined.minifactu_otc where date_format(payed_at,'%Y-%m-%d') between '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' and country in ('Peru','México','Colômbia','Chile')")
        resultado = []
        for row in cursor:
            content = {
            'country':row[0],
            'acronym':row[1],
            'id_payment':row[2],
            'payed_at':row[3],
            'amount_paid':row[4],
            'minifactu_id':row[5],
            'forma_pagamento':row[6]}
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
             cursor.close()
@app.route('/dashboard_latam/<fecha_inicio>/<fecha_fin>')
def dashboard_latam(fecha_inicio,fecha_fin):
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute("select date_format(payed_at,'%Y-%m-%d') payed_at ,country,count(*) as Total_tx,sum(amount_paid) as valorizado from prod_lake_modeled_refined.minifactu_otc where date_format(payed_at,'%Y-%m-%d') between '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' group by payed_at ,country ")
        resultado = []
        for row in cursor:
            content = {
            'payed_at':row[0],
            'country':row[1],
            'Total_tx':row[2],
            'valorizado':row[3]}
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
             cursor.close()

@app.route('/oracle_sovos/<fecha_inicio>/<fecha_fin>', methods=["POST", "GET"])
def oracle_sovos(fecha_inicio,fecha_fin):
        try:

            cursor = sqldatawarehouse.cursor()
            cursor.execute("SELECT REPLACE(RUC_EMISOR,' ','') RUC_EMISOR,REPLACE(TIPO_DOCUMENT,' ','') TIPO_DOCUMENT,REPLACE(FOLIO,' ','') FOLIO,REPLACE(RUC_RECEPTOR,' ','') RUC_RECEPTOR,FECHA_EMISION,REPLACE(ESTADO_OSE,' ','') ESTADO_OSE, REPLACE(URL,' ','') URL FROM DWH_SF.SOVOS.TRANSACCIONES_SIN_ESTADO_ERP WHERE FECHA_EMISION BETWEEN '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' AND ESTADO_OSE='ACEPTADO_POR_LA_OSE-A-OSE'")
            records = cursor.fetchall()
            for row in records:
                ruc_sovos= row[0]
                tipo_doc_fiscal = row[1]
                nro_invoice = row[2]
                nro_ruc_receptor= row[3]
                fecha_emision= format_fecha(row[4])
                estado_ose = row[5]
                url_sovos= row[6]

                #Inicio de envios sa Oracle  Cloud ERP

                url = "https://oic-prod-1-grvdxyhij8gy-ia.integration.ocp.oraclecloud.com/ic/api/integration/v1/flows/rest/LACL_PE_UPDA_INVO_AP_FROM_SOVO/1.0/documents"

                payload = "<Evento>\r\n   <TipoEvento>"''+estado_ose+''"</TipoEvento>\r\n   <RucEmisor>20600597940</RucEmisor>\r\n   <RucReceptor>"''+nro_ruc_receptor+''"</RucReceptor>\r\n   <TipoCPE>"''+tipo_doc_fiscal+''"</TipoCPE>\r\n   <Folio>"''+nro_invoice+''"</Folio>\r\n   <FechaEmision>"''+fecha_emision+''"</FechaEmision>\r\n   <FechaEvento>"''+fecha_emision+''" 16:44:38</FechaEvento>\r\n   <URI>"''+url_sovos+''"</URI>\r\n   <Descripcion>Descripcion D</Descripcion>\r\n   <Observacion>Observacion O</Observacion>\r\n   <CDR>\r\n      <URICDR/>\r\n      <CDRB64/>\r\n   </CDR>\r\n</Evento>"
                headers = {
                    'Content-Type': 'text/xml',
                    'Authorization': 'Basic cHJvdmVlZG9yX2Zpc2NhbC5wZTpJbnRlZ3JhY29lcyMyMDIz'
                }

                response = requests.request("POST", url, headers=headers, data=payload, allow_redirects=False)

                print(response.text)



                #Fin de envios a Oracle Cloud ERP


                print("\n")

            cursor.close()

            return ruc_sovos

        except Exception as e:
            print(e)

@app.route('/auditoria_data_lake/')
def auditoria_data_lake():
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute("select date_format(max(load_datetime) , '%Y-%m-%d %H:%i:%s') HORA_SINCRONIZACION_UTC,'PROD_LAKE_MODELED_REFINED.MINIFACTU_OTC' as TABLA_DATA_LAKE from prod_lake_modeled_refined.minifactu_otc union all select date_format(max(load_datetime) , '%Y-%m-%d %H:%i:%s') HORA_SINCRONIZACION_UTC,'PROD_LAKE_MINIFACTU_REFINED.INVOICES' as TABLA_DATA_LAKE from prod_lake_minifactu_refined.invoices")
        resultado = []
        for row in cursor:
            content = { 'HORA_SINCRONIZACION_UTC':row[0],
                        'TABLA_DATA_LAKE':row[1]}
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
        cursor.close()
@app.route('/pagos_procesados_aws/<fecha_inicio>/<fecha_fin>')
def pagos_procesados_aws(fecha_inicio,fecha_fin):
    try:

        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY",
                         aws_secret_access_key="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",
                         s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1",
                         work_group="peru", schema_name="prod_lake_modeled_refined").cursor()

        cursor.execute("select id_payment,status_pagamento,date_format(payed_at , '%Y-%m-%d') payed_at ,amount_paid ,CASE WHEN forma_pagamento is null THEN 'Forma de Pago NO Identificada' ELSE forma_pagamento end as forma_pagamento ,country ,acronym,CASE WHEN minifactu_id is null THEN 0 ELSE minifactu_id end minifactu_id , CASE WHEN errors is null THEN 'Sin errores en SmartSystem' ELSE array_join(errors,',') end error,CASE WHEN error is null THEN 'Solicitar reenvio a MiniFactu' ELSE error end error_ss from prod_lake_modeled_refined.minifactu_otc where date_format(payed_at, '%Y-%m-%d') BETWEEN '" + str(fecha_inicio) + "' and '" + str(fecha_fin) + "' and country  in('Peru','Colômbia','México','Chile','Brasil') ")
        records = cursor.fetchall()

        for row in records:
            id_payment = str(row[0])
            status_pagamento = str(row[1])
            payed_at = str(row[2])
            amount_paid = str(row[3])
            forma_pagamento = str(row[4])
            country = str(row[5])
            acronym = str(row[6])
            minifactu_id = str(row[7])
            error = row[8]
            error_ss = row[9]
            cur = connposgresql.cursor()
            query_sql_insert = 'insert into "ATHENA"."PAGOS_PROCESADOS_SMARTSYSTEM_LATAM"  (ID_PAYMENT,STATUS_PAGAMENTO,PAYET_AT,AMOUNT_PAID,FORMA_PAGAMENTO,COUNTRY,ACRONYM,MINIFACTU_ID,ERROR,error_ss) ' \
                              "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            cur.execute(query_sql_insert,(id_payment,status_pagamento,payed_at,amount_paid,forma_pagamento,country,acronym,minifactu_id,error,error_ss))
        connposgresql.commit()
        cursor.close()
        return jsonify({'status': 'success', 'message': 'Sincronizacion Finalizada!'}), 200
    except Exception as e:
        print(str(e))
    finally:
        print('Se ejecuto pagos procesados.')

@app.route('/payment_ar/<fecha_inicio>/<fecha_fin>')
def payment_ar(fecha_inicio,fecha_fin):
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY",
                         aws_secret_access_key="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",
                         s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1",
                         work_group="peru", schema_name="prod_lake_ss_refined").cursor()
        sql = "WITH payments_services AS (SELECT date(p.payed_at) AS payed_at, p.payable_type, p.payable_id, p.plan_id, p.location_id, pm.kind AS payment_method_kind, p.state AS payment_state, st.name AS status_name, pc.name AS payment_company_name, p.id AS payment_id, w.person_id AS person_id, p.amount_paid AS amount_paid, p.authorization_number, w.card_number, otc.minifactu_id , p.invoice_code, p.load_datetime FROM prod_lake_ss_refined.payments p JOIN prod_lake_ss_refined.wallets w ON w.id = p.wallet_id JOIN prod_lake_ss_refined.payment_companies pc ON pc.id = w.payment_company_id JOIN prod_lake_ss_refined.payment_methods pm ON pm.id = pc.payment_method_id LEFT JOIN prod_lake_ss_refined.payment_statuses st ON st.id = p.payment_status_id left join prod_lake_modeled_refined.minifactu_otc otc on p.id = otc.id_payment WHERE p.state = 'payed' AND p.amount_paid > 0 AND p.payable_type = 'Service' AND year(p.payed_at) = year(CURRENT_DATE) AND p.location_id in (SELECT l.id FROM prod_lake_modeled_refined.dim_locations l WHERE l.country = 'Peru') ), services AS (SELECT s.id AS service_id, s.description AS service_description, p.id AS product_id, p.name AS product_name, p.description AS product_description, p.kind AS product_kind FROM prod_lake_ss_refined.services s JOIN prod_lake_ss_refined.products p ON p.id = s.product_id), payments_membership AS (SELECT date(p.payed_at) AS payed_at, p.payable_type, p.payable_id, p.plan_id, p.location_id, pm.kind AS payment_method_kind, p.state AS payment_state, st.name AS status_name, pc.name AS payment_company_name, p.id AS payment_id, w.person_id AS person_id, p.amount_paid AS amount_paid, 'Membresía ' AS product_description, p.authorization_number, w.card_number, otc.minifactu_id , p.invoice_code , p.load_datetime FROM prod_lake_ss_refined.payments p JOIN prod_lake_ss_refined.wallets w ON w.id = p.wallet_id JOIN prod_lake_ss_refined.payment_companies pc ON pc.id = w.payment_company_id JOIN prod_lake_ss_refined.payment_methods pm ON pm.id = pc.payment_method_id LEFT JOIN prod_lake_ss_refined.payment_statuses st ON st.id = p.payment_status_id left join prod_lake_modeled_refined.minifactu_otc otc on p.id = otc.id_payment WHERE p.state = 'payed' AND p.amount_paid > 0 AND p.payable_type = 'Membership' AND year(p.payed_at) = year(CURRENT_DATE) AND p.location_id in (SELECT l.id FROM prod_lake_modeled_refined.dim_locations l WHERE l.country = 'Peru') ), payments_union AS (SELECT payments_services.payed_at, payments_services.payable_type, payments_services.payable_id, payments_services.plan_id, payments_services.location_id, payments_services.payment_method_kind, payments_services.payment_state, payments_services.status_name, payments_services.payment_company_name, payments_services.payment_id, payments_services.person_id, payments_services.amount_paid, services.product_description, payments_services.authorization_number, payments_services.card_number, payments_services.minifactu_id, payments_services.invoice_code, payments_services.load_datetime FROM payments_services INNER JOIN services ON services.service_id = payable_id UNION ALL SELECT payments_membership.payed_at, payments_membership.payable_type, payments_membership.payable_id, payments_membership.plan_id, payments_membership.location_id, payments_membership.payment_method_kind, payments_membership.payment_state, payments_membership.status_name, payments_membership.payment_company_name, payments_membership.payment_id, payments_membership.person_id, payments_membership.amount_paid, payments_membership.product_description, payments_membership.authorization_number, payments_membership.card_number, payments_membership.minifactu_id, payments_membership.invoice_code, payments_membership.load_datetime FROM payments_membership), plans AS (SELECT id AS plan_id, name AS plan_name FROM prod_lake_modeled_refined.dim_plans), units_countries AS (SELECT id AS location_id, country, currency, acronym, name AS location_name FROM prod_lake_modeled_refined.dim_locations), payments_complete AS (SELECT payed_at AS fecha_pago, payment_id AS front_id, acronym AS sigla_unidad, location_name AS nombre_ubicacion, person_id AS matricula_usuario, amount_paid AS importe, payable_type AS tipo_pago, plan_name AS Plan, payment_method_kind AS metodo_pago, CASE WHEN payable_type = 'Service' THEN product_description ELSE concat(product_description, plan_name) END AS descripcion_mensualidad, payment_company_name AS financiero, authorization_number, card_number, minifactu_id, invoice_code FROM payments_union INNER JOIN plans ON plans.plan_id = payments_union.plan_id INNER JOIN units_countries ON units_countries.location_id = payments_union.location_id) SELECT * FROM payments_complete where fecha_pago between cast('" + str(fecha_inicio) + " 00:00:00' as timestamp) and cast('" + str(fecha_fin) + " 00:00:00' as timestamp)"

        cursor.execute(sql)
        records = cursor.fetchall()

        for row in records:
            front_id = row[1]
            fecha_pago = row[0]
            sigla_unidad = str(row[2])
            nombre_ubicacion = str(row[3])
            matricula_usuario = str(row[4])
            importe = row[5]
            tipo_pago = str(row[6])
            Plan = str(row[7])
            metodo_pago = row[8]
            descripcion_mensualidad = str(row[9])
            financiero = str(row[10])
            authorization_number = row[11]
            card_number = row[12]
            minifactu_id = row[13]
            invoice_code = row[14]
            cur = connposgresql.cursor()
            query_sql_insert = 'insert into "PAYMENT"."payment"(front_id, fecha_pago, sigla_unidad, nombre_ubicacion, matricula_usuario, importe, tipo_pago, Plan, metodo_pago, descripcion_mensualidad, financiero, authorization_number, card_number, minifactu_id, invoice_code)' \
                              " values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s) "
            cur.execute(query_sql_insert,(front_id, fecha_pago, sigla_unidad, nombre_ubicacion, matricula_usuario, importe, tipo_pago, Plan, metodo_pago, descripcion_mensualidad, financiero, authorization_number, card_number, minifactu_id, invoice_code))
        connposgresql.commit()
        return 'Sincronizacion Pagos Procesados Finalizada!'
    except Exception as e:
        print(e)
    finally:
        cursor.close()

@app.route('/unidades_peru')
def unidades_peru():
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY",
                         aws_secret_access_key="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",
                         s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1",
                         work_group="peru", schema_name="prod_lake_ss_refined").cursor()
        sql = "select acronym ACRONYM,upper(name) NAME_UNIDAD,CASE WHEN acronym ='HUAHUA1' THEN '6' WHEN acronym ='LIMPUL1' THEN '154' WHEN acronym ='CUSCUS1' THEN '31' WHEN acronym ='LIMVMT1' THEN '75' WHEN acronym ='PIUPIU1' THEN '10' WHEN acronym ='LIMATE1' THEN '59' WHEN acronym ='LIMSUR3' THEN '194' WHEN acronym ='LIMSUR1' THEN '202' WHEN acronym ='TRUTRU2' THEN '14' WHEN acronym ='CAJCAJ1' THEN '33' WHEN acronym ='CHICLA2' THEN '3' WHEN acronym ='AQPCAY2' THEN '1' WHEN acronym ='LIMSUR3' THEN '194' WHEN acronym ='LIMLIM2' THEN '114' WHEN acronym ='LIMLIM2' THEN '114' WHEN acronym ='LIMSMP1' THEN '92' WHEN acronym ='LIMCHO4' THEN '5' WHEN acronym ='PUCPUC1' THEN '34' WHEN acronym ='ICAICA1' THEN '122' ELSE '-' end as NUMERACION_REAL_PLAZA, CASE WHEN acronym ='HUAHUA1' THEN 'LE-02' WHEN acronym ='LIMPUL1' THEN 'TA-201' WHEN acronym ='CUSCUS1' THEN 'TI-04' WHEN acronym ='LIMVMT1' THEN 'LC-410' WHEN acronym ='PIUPIU1' THEN 'TI-04' WHEN acronym ='LIMATE1' THEN 'TI-206' WHEN acronym ='LIMSUR3' THEN 'TA-101' WHEN acronym ='LIMSUR1' THEN 'LCE-101' WHEN acronym ='TRUTRU2' THEN 'LC-201' WHEN acronym ='CAJCAJ1' THEN 'TI-01' WHEN acronym ='CHICLA2' THEN 'LC-203' WHEN acronym ='AQPCAY2' THEN 'LC-301' WHEN acronym ='LIMSUR3' THEN 'TA-101' WHEN acronym ='LIMLIM2' THEN 'TA-201' WHEN acronym ='LIMLIM2' THEN 'TA-201' WHEN acronym ='LIMSMP1' THEN 'LCE-201/LCE-301' WHEN acronym ='LIMCHO4' THEN 'TI-202' WHEN acronym ='PUCPUC1' THEN 'TI-201/202' WHEN acronym ='ICAICA1' THEN 'LCE-103' ELSE '-' end AS CODIGO_REAL_PLAZA, CASE WHEN acronym ='LIMCOR1' THEN 'PE010001' WHEN acronym ='CUSCUS1' THEN 'PE010002' WHEN acronym ='LIMMIR1' THEN 'PE010003' WHEN acronym ='LIMMIR2' THEN 'PE010004' WHEN acronym ='LIMVMT1' THEN 'PE010005' WHEN acronym ='LIMMIG1' THEN 'PE010006' WHEN acronym ='AQPCAY1' THEN 'PE010007' WHEN acronym ='LIMSJM1' THEN 'PE010008' WHEN acronym ='LIMMIR3' THEN 'PE010009' WHEN acronym ='LIMMOL1' THEN 'PE010010' WHEN acronym ='LIMIND1' THEN 'PE010011' WHEN acronym ='AQPCOL1' THEN 'PE010012' WHEN acronym ='LIMLIM1' THEN 'PE010013' WHEN acronym ='LIMPUL1' THEN 'PE010014' WHEN acronym ='CALBEL1' THEN 'PE010015' WHEN acronym ='LIMLVC1' THEN 'PE010016' WHEN acronym ='PIUPIU1' THEN 'PE010017' WHEN acronym ='HUAHUA1' THEN 'PE010018' WHEN acronym ='LIMLVC2' THEN 'PE010019' WHEN acronym ='PIUPIU2' THEN 'PE010020' WHEN acronym ='LIMATE1' THEN 'PE010021' WHEN acronym ='LIMMIR4' THEN 'PE010022' WHEN acronym ='LIMCHO2' THEN 'PE010023' WHEN acronym ='LIMSUR1' THEN 'PE010024' WHEN acronym ='LIMCHO4' THEN 'PE010025' WHEN acronym ='PIUPIU3' THEN 'PE010026' WHEN acronym ='PIUCAS1' THEN 'PE010027' WHEN acronym ='CHICLA2' THEN 'PE010028' WHEN acronym ='TRUTRU2' THEN 'PE010029' WHEN acronym ='AQPAQP1' THEN 'PE010030' WHEN acronym ='AQPPAU1' THEN 'PE010031' WHEN acronym ='AQPCAY2' THEN 'PE010032' WHEN acronym ='CAJCAJ1' THEN 'PE010033' WHEN acronym ='LIMSUR3' THEN 'PE010035' WHEN acronym ='LIMADM1' THEN 'PE010036' WHEN acronym ='CAJCAJ2' THEN 'PE010037' WHEN acronym ='ICAICA2' THEN 'PE010038' WHEN acronym ='LIMSMP1' THEN 'PE010039' WHEN acronym ='CHICLA4' THEN 'PE010040' WHEN acronym ='LIMSJL2' THEN 'PE010041' WHEN acronym ='LORIQT1' THEN 'PE010042' WHEN acronym ='LIMLIM2' THEN 'PE010043' WHEN acronym ='LIMCHO1' THEN 'PE010044' WHEN acronym ='LIMMIR5' THEN 'PE010045' WHEN acronym ='LIMBAR1' THEN 'PE010046' WHEN acronym ='CALBEL2' THEN 'PE010047' WHEN acronym ='LIMSUQ1' THEN 'PE010048' WHEN acronym ='LIMANI1' THEN 'PE010049' WHEN acronym ='LIMMGD1' THEN 'PE010050' WHEN acronym ='LIMSBO1' THEN 'PE010051' WHEN acronym ='ICAICA1' THEN 'PE010052' WHEN acronym ='LIMCOM1' THEN 'PE010053' WHEN acronym ='CHICLA1' THEN 'PE010054' WHEN acronym ='LIMCHO3' THEN 'PE010055' WHEN acronym ='LIMIND2' THEN 'PE010056' WHEN acronym ='LIMSUR5' THEN 'PE010057' WHEN acronym ='TRUTRU1' THEN 'PE010058' WHEN acronym ='LIMSUR4' THEN 'PE010059' WHEN acronym ='CALCAL1' THEN 'PE010060' WHEN acronym ='LIMSUR7' THEN 'PE010061' WHEN acronym ='LIMISI2' THEN 'PE010062' WHEN acronym ='CHICLA3' THEN 'PE010063' WHEN acronym ='HUAHUA2' THEN 'PE010064' WHEN acronym ='LIMATE2' THEN 'PE010065' WHEN acronym ='LIMBRE1' THEN 'PE010066' WHEN acronym ='LIMISI1' THEN 'PE010067' WHEN acronym ='LIMLIM3' THEN 'PE010068' WHEN acronym ='LIMLIM4' THEN 'PE010069' WHEN acronym ='LIMMOL2' THEN 'PE010070' WHEN acronym ='LIMSJL1' THEN 'PE010071' WHEN acronym ='LIMSUQ2' THEN 'PE010072' WHEN acronym ='LIMSUR2' THEN 'PE010073' WHEN acronym ='LIMSUR6' THEN 'PE010074' WHEN acronym ='PUCPUC1' THEN 'PE010075' WHEN acronym ='PUCPUC2' THEN 'PE010076' WHEN acronym ='LIMAGU1' THEN 'PE010077' WHEN acronym ='LIMSJM2' THEN 'PE010078' WHEN acronym ='LIMMOL3' THEN 'PE010079' WHEN acronym ='LIMMIG2' THEN 'PE010080' ELSE 'Aun no Apertura' end AS FILIAL,cnpj RUC ,'SMARTFIT PERU S.A.C.' RAZON_SOCIAL,unified_location_id ID_SMARTSYSTEM_OIC,upper(country) COUNTRY from prod_lake_modeled_refined.dim_locations where cnpj ='20600597940'"
        cursor.execute(sql)
        records = cursor.fetchall()
        for row in records:
            front_id = row[0]
            fecha_pago = row[1]
            sigla_unidad = str(row[2])
            nombre_ubicacion = str(row[3])
            matricula_usuario = str(row[4])
            importe = row[5]
            tipo_pago = str(row[6])
            Plan = str(row[7])
            metodo_pago = row[8]
            descripcion_mensualidad = str(row[9])
            financiero = str(row[10])
            authorization_number = row[11]
            card_number = row[12]
            minifactu_id = row[13]
            invoice_code = row[14]
            cur = connposgresql.cursor()
            query_sql_insert = 'insert into "PAYMENT"."payment"(front_id, fecha_pago, sigla_unidad, nombre_ubicacion, matricula_usuario, importe, tipo_pago, Plan, metodo_pago, descripcion_mensualidad, financiero, authorization_number, card_number, minifactu_id, invoice_code)' \
                              " values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s) "
            cur.execute(query_sql_insert,(front_id, fecha_pago, sigla_unidad, nombre_ubicacion, matricula_usuario, importe, tipo_pago, Plan, metodo_pago, descripcion_mensualidad, financiero, authorization_number, card_number, minifactu_id, invoice_code))
        connposgresql.commit()
        return 'Sincronizacion Pagos Procesados Finalizada!'
    except Exception as e:
        print(e)
    finally:
        cursor.close()

@app.route('/scheduled_pe')
def scheduled_pe():
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        sql = "select operadora ,brand ,date_format(due_on, '%Y-%m-%d') due_on ,acronym ,count(*) as total_tx,sum(amount) as valorizado  from prod_lake_modeled_refined.fin_otc where operadora  in ('MCprocesosPERU','peru_interbank','VisanetPERU') and status_front ='scheduled' group by operadora,brand,due_on  ,acronym"
        cursor.execute(sql)
        records = cursor.fetchall()
        for row in records:
            operadora = row[0]
            brand = row[1]
            due_on = row[2]
            acronym= row[3]
            total_tx =row[4]
            valorizado =row[5]
            cur = connposgresql.cursor()
            query_sql_insert = 'insert into "PAYMENT"."marcado_agenda"(operadora ,brand ,due_on ,acronym ,total_tx ,valorizado)' \
                               " values(%s, %s, %s, %s, %s, %s) "
            cur.execute(query_sql_insert, (operadora ,brand ,due_on ,acronym ,total_tx ,valorizado))
        connposgresql.commit()
        return 'Sincronización Marcados en Agenda...'
    except Exception as e:
        print(e)
    finally:
             cursor.close()


@app.route('/interfaz_ingenico/<fecha_inicio>/<fecha_fin>')
def interfaz_ingenico(fecha_inicio,fecha_fin):
    try:
        # ELIMINAR REGISTROS PARA NO DUPLICAR
        sqlDelete = f"DELETE FROM \"FIN\".interfaz_ingenico WHERE payed_at BETWEEN  '{fecha_inicio}' and '{fecha_fin}'"
        print(sqlDelete)
        curDel = connposgresql.cursor()
        curDel.execute(sqlDelete)
        connposgresql.commit()

        # EXTRAE DATOS A INSERTAR
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY",
                         aws_secret_access_key="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",
                         s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1",
                         work_group="peru", schema_name="prod_lake_ss_refined").cursor()
        sql = "select otc.id_payment,otc.payed_at,otc.minifactu_id,otc.amount_paid,payment.created_at, import.state,import.fin_id,import.brand_name from prod_lake_modeled_refined.minifactu_otc otc inner join prod_lake_ss_refined.payments payment on otc.id_payment = payment.id inner join prod_lake_ss_refined.imports_payments import on payment.id = import.payment_id where country ='Peru' and otc.payed_at between cast('" + str(fecha_inicio) + " 00:00:00' as timestamp) and  cast('" + str(fecha_fin) + " 00:00:00' as timestamp)"
        cursor.execute(sql)
        records = cursor.fetchall()
        for row in records:
            id_payment = row[0]
            payed_at = row[1]
            minifactu_id = row[2]
            amount_paid = row[3]
            created_at = row[4]
            state = str(row[5])
            fin_id = row[6]
            brand_name = str(row[7])

            cur = connposgresql.cursor()
            query_sql_insert = 'insert into "FIN".interfaz_ingenico(id_payment, payed_at, minifactu_id, amount_paid, fecha, state, fin_id, brand_name)' \
                              " values(%s, %s, %s, %s, %s, %s, %s, %s) "
            cur.execute(query_sql_insert,(id_payment, payed_at, minifactu_id, amount_paid, created_at, state, fin_id, brand_name))
        connposgresql.commit()

        # SINCRONIZAR PROCEDIMIENTO
        sqlProc = f"CALL \"TESORERIA_PE\".sync_payment('{fecha_inicio}','{fecha_fin}')"
        curProc = connposgresql.cursor()
        curProc.execute(sqlProc)
        connposgresql.commit()
        return jsonify({'status': 'success', 'message': 'Sincronizacion INTERFAZ_INGENICO Finalizada!'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'{str(e)}'}), 500
 
@app.route('/front_system/<fecha_inicio>/<fecha_fin>')
def front_system(fecha_inicio,fecha_fin):
    try:

        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY",
                         aws_secret_access_key="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",
                         s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1",
                         work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute("select distinct date_format(t1.paid_at , '%Y-%m-%d') paid_at,t2.created_at,t2.external_system  ,t2.minifactu_id,t2.country,t2.external_id,otc.gross_value,otc.operation,CASE WHEN otc.gross_value is null THEN 'Pendiente_Integrar_en_oic_db' else 'Integrado_en_oic_db' end as status_integration_oic_db  from prod_lake_minifactu_refined.invoices_data t1 inner join prod_lake_minifactu_refined.invoices t2 on t1.invoice_id = t2.id LEFT join  prod_lake_modeled_refined.oic_otc otc on t2.minifactu_id = otc.minifactu_id where date_format(t1.paid_at , '%Y-%m-%d') BETWEEN '" + str(fecha_inicio) + "' and '" + str(fecha_fin) + "' and t2.country='brazil';")
        records = cursor.fetchall()

        for row in records:
            paid_at = str(row[0])
            created_at = str(row[1])
            external_system = str(row[2])
            minifactu_id = str(row[3])
            country = str(row[4])
            external_id = str(row[5])
            gross_value = str(row[6])
            operation = str(row[7])
            status_integration_oic_db = str(row[8])
            cur = connposgresql.cursor()
            query_sql_insert = 'insert into "DATALAKE".front_system (paid_at,created_at,external_system,minifactu_id,country,external_id,gross_value,operation,status_integration_oic_db) ' \
                               "values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            cur.execute(query_sql_insert, (
            paid_at, created_at, external_system, minifactu_id, country, external_id, gross_value,operation,status_integration_oic_db))
        connposgresql.commit()
        cursor.close()
        return jsonify({'status': 'success', 'message': 'Sincronizacion Front System Finalizada con exito!!!'}), 200
    except Exception as e:
        print(str(e))
    finally:
        jsonify({'status': 'success', 'message': 'Sincronizacion Finalizada!'}), 200
@app.route('/load_csv_tunquiaws')
def load_csv_tunquiaws():
    try:#Leyendo datos para inserccion desde la cabecera nivel de lineas
       # with open('C:\\Users\\luis.azanero.BIO-RITMO\\Desktop\\FILES_CSV_ERP\\ucmfa202485425', newline='', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=',', quoting=csv.QUOTE_NONE)
            next(reader)  # Skip the header row (salto de la primera linea).
            format = '%Y-%m-%d'
            for row in reader:
               # Insertando datos dese la cabecera
                customertrxlineid = row[0]
                customertrxlineid_insert = str(customertrxlineid.replace('"', ""))
                TRANSACTIONHEADERCUSTOMERTRXID = str(row[3])
                TRANSACTIONHEADERCUSTOMERTRXID_insert = str(TRANSACTIONHEADERCUSTOMERTRXID.replace('"', ""))
                TRANSACTIONHEADERTRXDATE = str(row[4])
                TRANSACTIONHEADERTRXDATE_insert = TRANSACTIONHEADERTRXDATE.replace('"', "")
                fecha = datetime.datetime.strptime(TRANSACTIONHEADERTRXDATE_insert, format).date()
                dia = str(fecha.day)
                mes = str(fecha.month)
                anio = str(fecha.year)
                fecha_emision = str(anio+'-'+mes+'-'+dia)
                TRANSACTIONLINEINTERFACELINEATTRIBUTE1 = str(row[6])
                TRANSACTIONLINEINTERFACELINEATTRIBUTE1_insert = str(TRANSACTIONLINEINTERFACELINEATTRIBUTE1.replace('NA', "-")).replace('"', "")
                TRANSACTIONLINEINTERFACELINEATTRIBUTE2 = str(row[13])
                TRANSACTIONLINEINTERFACELINEATTRIBUTE2_insert = str(TRANSACTIONLINEINTERFACELINEATTRIBUTE2.replace('"', ""))
                TRANSACTIONLINEINTERFACELINEATTRIBUTE3 = str(row[14])
                TRANSACTIONLINEINTERFACELINEATTRIBUTE3_insert = str(TRANSACTIONLINEINTERFACELINEATTRIBUTE3.replace('"', ""))
                TRANSACTIONLINELINENUMBER = str(row[21])
                TRANSACTIONLINELINENUMBER_insert = str(TRANSACTIONLINELINENUMBER.replace('"', ""))
                TRANSACTIONLINELINETYPE = str(row[22])
                TRANSACTIONLINELINETYPE_insert = str(TRANSACTIONLINELINETYPE.replace('"', ""))
                TRANSACTIONLINEUNITSELLINGPRICE = str(row[23])
                TRANSACTIONLINEUNITSELLINGPRICE_insert = str(TRANSACTIONLINEUNITSELLINGPRICE.replace('"', ""))
                TRANSACTIONLINEUNITSTANDARDPRICE = str(row[24])
                TRANSACTIONLINEUNITSTANDARDPRICE_insert = str(TRANSACTIONLINEUNITSTANDARDPRICE.replace('"', ""))
                cur = connposgresql.cursor()
                query_sql_insert = 'INSERT INTO "ORACLE".invoice_erp_cabecera_bicc (customertrxlineid,transactionheadercustomertrxid,TRANSACTIONHEADERTRXDATE,TRANSACTIONLINEINTERFACELINEATTRIBUTE1,TRANSACTIONLINEINTERFACELINEATTRIBUTE2,TRANSACTIONLINELINENUMBER,TRANSACTIONLINELINETYPE,TRANSACTIONLINEUNITSELLINGPRICE,TRANSACTIONLINEUNITSTANDARDPRICE,TRANSACTIONLINEINTERFACELINEATTRIBUTE3) ' \
                               "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cur.execute(query_sql_insert, (
                customertrxlineid_insert, TRANSACTIONHEADERCUSTOMERTRXID_insert, fecha_emision,TRANSACTIONLINEINTERFACELINEATTRIBUTE1_insert,TRANSACTIONLINEINTERFACELINEATTRIBUTE2_insert,TRANSACTIONLINELINENUMBER_insert,TRANSACTIONLINELINETYPE_insert,TRANSACTIONLINEUNITSELLINGPRICE_insert,TRANSACTIONLINEUNITSTANDARDPRICE_insert,TRANSACTIONLINEINTERFACELINEATTRIBUTE3_insert))
                connposgresql.commit()
                print('Sincronizacion en proceso...'+TRANSACTIONLINEINTERFACELINEATTRIBUTE1_insert)
    except Exception as e:
        print(str(e))
    finally:
        print('Finalize')

server_name = app.config['SERVER_NAME']
if server_name and ':' in server_name:
    host, port = server_name.split(":")
    port = int(port)
else:
    port = 1247
    host = "0.0.0.0"

    app.run(debug=True,host=host, port=port)
