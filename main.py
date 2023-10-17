import requests as requests
from flask import Flask, jsonify,request
import pyodbc
import pymssql
#from flask_mysqldb import MySQL
from pyathena import connect


app = Flask(__name__)

app.config['MYSQL_HOST'] = 'oic-db-prod.cgzshounia8v.us-east-1.rds.amazonaws.com'
app.config['MYSQL_USER'] = 'luis.azanero'
app.config['MYSQL_PASSWORD'] = 'Lu1s0Ic2023'
app.config['MYSQL_DB'] = 'oic_db'
#mysql = MySQL(app)

conn = pymssql.connect(server='10.84.6.199', user='sa', password='31zDM#OJ9f1g7h!&hsDR', database='VOXIVA')

@app.route('/products')
def getAllProducts():
    try:
        cur = mysql.connection.cursor()
        cur.execute("SELECT id,country ,product_from_to_origin_system ,product_from_to_front_product_id ,front_product_name ,erp_item_ar_id ,erp_item_ar_name from oic_db.product_from_to_version where country='Peru'")
        data = cur.fetchall()
        resultado = []
        for row in data:
            content = {'id': row[0],'country': row[1],'product_from_to_origin_system': row[2],'product_from_to_front_product_id': row[3],'front_product_name': row[4],'erp_item_ar_id': row[5],'erp_item_ar_name': row[6]}
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
        cur.close()

@app.route('/customer')
def customer():
    try:
        cur = mysql.connection.cursor()
        cur.execute("SELECT id,erp_customer_id ,full_name ,type_person ,identification_financial_responsible,chargeback_acquirer_label  from oic_db.customer WHERE nationality_code ='PE'")
        data = cur.fetchall()
        resultado = []
        for row in data:
            content = { 'id':row[0], 'erp_customer_id': row[1],'full_name': row[2],'type_person': row[3],'identification_financial_responsible': row[4],'chargeback_acquirer_label': row[5] }
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
        cur.close()

@app.route('/cliente/<int:id>')
def cliente(id):
    try:
        cur = mysql.connection.cursor()
        cur.execute("SELECT id,erp_customer_id ,full_name ,type_person ,identification_financial_responsible,chargeback_acquirer_label from oic_db.customer WHERE nationality_code ='PE' and identification_financial_responsible ="+ str(id))
        data = cur.fetchall()
        resultado = []
        for row in data:
            content = { 'id':row[0], 'erp_customer_id': row[1],'full_name': row[2],'type_person': row[3],'identification_financial_responsible': row[4],'chargeback_acquirer_label': row[5] }
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
        cur.close()

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
@app.route('/oic_db/<fecha_inicio>/<fecha_fin>')
def oic_db(fecha_inicio, fecha_fin):
    try:
        cur = mysql.connection.cursor()
        cur.execute("select distinct otc.id ,date_format(otc.created_at, '%Y-%m-%d %H:%i') created_at ,otc.country ,otc.unity_identification ,otc.erp_business_unit ,otc.erp_legal_entity ,otc.erp_subsidiary ,otc.acronym ,otc.to_generate_invoice , otc.origin_system ,otc.minifactu_id ,otc.front_id ,otc.conciliator_id conciliator_id_otc ,date_format(otc.erp_invoice_customer_send_to_erp_at, '%Y-%m-%d %H:%i')erp_invoice_customer_send_to_erp_at , date_format(otc.erp_invoice_customer_returned_from_erp_at, '%Y-%m-%d %H:%i') erp_invoice_customer_returned_from_erp_at ,erp_invoice_customer_status_transaction, date_format(otc.erp_receivable_sent_to_erp_at, '%Y-%m-%d %H:%i') erp_receivable_sent_to_erp_at , date_format(otc.erp_receivable_returned_from_erp_at, '%Y-%m-%d %H:%i') erp_receivable_returned_from_erp_at ,erp_receivable_customer_identification,erp_receivable_status_transaction , date_format(otc.erp_invoice_send_to_erp_at, '%Y-%m-%d %H:%i')  erp_invoice_send_to_erp_at, date_format(otc.erp_invoice_returned_from_erp_at, '%Y-%m-%d %H:%i') erp_invoice_returned_from_erp_at , erp_invoice_status_transaction, date_format(otc.erp_receipt_send_to_erp_at, '%Y-%m-%d %H:%i') erp_receipt_send_to_erp_at , date_format(otc.erp_receipt_returned_from_erp_at, '%Y-%m-%d %H:%i') erp_receipt_returned_from_erp_at ,erp_receipt_status_transaction,r.erp_receivable_id,r.erp_clustered_receivable_id ,r.conciliator_id ,r.transaction_type ,r.contract_number ,r.credit_card_brand ,r.truncated_credit_card ,r.price_list_value ,r.gross_value ,r.net_value ,r.interest_value,r.administration_tax_percentage ,r.administration_tax_value , date_format(r.billing_date, '%Y-%m-%d') billing_date , date_format(r.credit_date, '%Y-%m-%d') credit_date ,r.registration_gym_student ,r.fullname_gym_student ,r.oic_ids,cliente.identification_financial_responsible , cliente.document_kind, Substring(UPPER(cliente.full_name), 1, 75) full_name, ii.front_product_id ,ii.front_plan_id ,ii.front_addon_id ,ii.erp_item_ar_id ,ii.erp_item_ar_name , url ,status_url from oic_db.order_to_cash otc inner join oic_db.receivable r on otc.id =r.order_to_cash_id inner join oic_db.invoice_customer cliente on otc.id = cliente.order_to_cash_id inner join oic_db.invoice i  on otc.id = i.order_to_cash_id left join oic_db.invoice_items ii  on i.id  = ii.id_invoice LEFT JOIN oic_db.invoice_erp_configurations iec ON iec.country = otc.country where DATE_FORMAT(otc.created_at,'%Y-%m-%d') between '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' and otc.country ='Peru' AND iec.erp_business_unit = otc.erp_business_unit AND iec.erp_legal_entity = otc.erp_legal_entity AND iec.erp_subsidiary = otc.erp_subsidiary  AND iec.origin_system = otc.origin_system  AND iec.operation = otc.operation")
        data = cur.fetchall()
        resultado = []
        for row in data:
            content = {'id': row[0],
            'created_at': row[1],
            'country': row[2],
            'unity_identification': row[3],
            'erp_business_unit': row[4],
            'erp_legal_entity': row[5],
            'erp_subsidiary': row[6],
            'acronym': row[7],
            'to_generate_invoice': row[8],
            'origin_system': row[9],
            'minifactu_id': row[10],
            'front_id': row[11],
            'conciliator_id_otc': row[12],
            'erp_invoice_customer_send_to_erp_at': row[13],
            'erp_invoice_customer_returned_from_erp_at': row[14],
            'erp_invoice_customer_status_transaction': row[15],
            'erp_receivable_sent_to_erp_at': row[16],
            'erp_receivable_returned_from_erp_at': row[17],
            'erp_receivable_customer_identification': row[18],
            'erp_receivable_status_transaction': row[19],
            'erp_invoice_send_to_erp_at': row[20],
            'erp_invoice_returned_from_erp_at': row[21],
            'erp_invoice_status_transaction': row[22],
            'erp_receipt_send_to_erp_at': row[23],
            'erp_receipt_returned_from_erp_at': row[24],
            'erp_receipt_status_transaction': row[25],
            'erp_receivable_id': row[26],
            'erp_clustered_receivable_id': row[27],
            'conciliator_id': row[28],
            'transaction_type': row[29],
            'contract_number': row[30],
            'credit_card_brand': row[31],
            'truncated_credit_card': row[32],
            'price_list_value': row[33],
            'gross_value': row[34],
            'net_value': row[35],
            'interest_value': row[36],
            'administration_tax_percentage': row[37],
            'administration_tax_value': row[38],
            'billing_date': row[39],
            'credit_date': row[40],
            'registration_gym_student': row[41],
            'fullname_gym_student': row[42],
            'oic_ids': row[43],
            'identification_financial_responsible': row[44],
            'document_kind': row[45],
            'full_name': row[46],
            'front_product_id': row[47],
            'front_plan_id': row[48],
            'front_addon_id': row[49],
            'erp_item_ar_id': row[50],
            'erp_item_ar_name': row[51],
            'url': row[52],
            'status_url': row[53]}
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
        cur.close()

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
@app.route('/dashboard_otc/<fecha_inicio>/<fecha_fin>')
def dashboard_otc(fecha_inicio, fecha_fin):
    try:
        cur = mysql.connection.cursor()
        cur.execute("SELECT date_format(otc.created_at, '%Y-%m-%d') as created_at ,erp_business_unit,erp_invoice_customer_status_transaction,erp_receivable_status_transaction,erp_invoice_status_transaction,erp_receipt_status_transaction,count(*) as total_tx,FORMAT(SUM(r.price_list_value), 2) as Valorizado from oic_db.order_to_cash otc inner join oic_db.receivable r on otc.id =r.order_to_cash_id  where date_format(otc.created_at, '%Y-%m-%d') between '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' and otc.country ='Peru' group by date_format(otc.created_at, '%Y-%m-%d'),erp_business_unit,erp_invoice_customer_status_transaction,erp_receivable_status_transaction;")
        data = cur.fetchall()
        resultado = []
        for row in data:
            content = {
            'created_at': row[0],
            'erp_business_unit': row[1],
            'erp_invoice_customer_status_transaction': row[2],
            'erp_receivable_status_transaction': row[3],
            'erp_invoice_status_transaction': row[4],
            'erp_receipt_status_transaction': row[5],
            'total_tx': row[6],
            'valorizado': row[7]}
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
        cur.close()
@app.route('/sovos/<fecha_inicio>/<fecha_fin>')
def sovos(fecha_inicio, fecha_fin):
    try:
        cur = mysql.connection.cursor()
        cur.execute("SELECT distinct otc.id ,date_format(otc.created_at, '%Y-%m-%d') created_at ,otc.country ,otc.unity_identification ,otc.erp_business_unit ,otc.erp_legal_entity ,otc.erp_subsidiary ,otc.acronym ,otc.to_generate_invoice , otc.origin_system ,otc.minifactu_id ,otc.front_id ,date_format(otc.erp_invoice_customer_send_to_erp_at, '%Y-%m-%d %H:%i') erp_invoice_customer_send_to_erp_at ,date_format(erp_invoice_customer_returned_from_erp_at, '%Y-%m-%d %H:%i')  erp_invoice_customer_returned_from_erp_at ,erp_invoice_customer_status_transaction,date_format(erp_receivable_sent_to_erp_at, '%Y-%m-%d %H:%i') erp_receivable_sent_to_erp_at , date_format(erp_receivable_returned_from_erp_at, '%Y-%m-%d %H:%i') erp_receivable_returned_from_erp_at ,erp_receivable_customer_identification,erp_receivable_status_transaction ,date_format(erp_invoice_send_to_erp_at, '%Y-%m-%d %H:%i') erp_invoice_send_to_erp_at, date_format(erp_invoice_returned_from_erp_at, '%Y-%m-%d %H:%i') erp_invoice_returned_from_erp_at ,erp_invoice_status_transaction,date_format(erp_receipt_send_to_erp_at, '%Y-%m-%d %H:%i') erp_receipt_send_to_erp_at , date_format(erp_receipt_returned_from_erp_at, '%Y-%m-%d %H:%i') erp_receipt_returned_from_erp_at ,erp_receipt_status_transaction, r.erp_receivable_id,r.erp_clustered_receivable_id , r.conciliator_id ,r.transaction_type ,r.contract_number ,r.credit_card_brand ,r.truncated_credit_card ,r.price_list_value ,r.interest_value,r.administration_tax_percentage ,r.administration_tax_value , date_format( r.billing_date, '%Y-%m-%d') billing_date ,date_format(r.credit_date, '%Y-%m-%d') credit_date ,r.registration_gym_student ,r.fullname_gym_student ,r.oic_ids,cliente.identification_financial_responsible ,cliente.document_kind,Substring(UPPER(cliente.full_name), 1, 75) full_name,ii.front_product_id,ii.erp_item_ar_id ,ii.erp_item_ar_name ,url ,status_url from oic_db.order_to_cash otc inner join oic_db.receivable r on otc.id =r.order_to_cash_id inner join oic_db.invoice_customer cliente on otc.id = cliente.order_to_cash_id inner join oic_db.invoice i  on otc.id = i.order_to_cash_id left join oic_db.invoice_items ii  on i.id  = ii.id_invoice LEFT JOIN oic_db.invoice_erp_configurations iec ON iec.country = otc.country where DATE_FORMAT(otc.created_at,'%Y-%m-%d') between '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' and otc .country ='Peru' AND iec.erp_business_unit = otc.erp_business_unit AND iec.erp_legal_entity = otc.erp_legal_entity AND iec.erp_subsidiary = otc.erp_subsidiary AND iec.origin_system = otc.origin_system AND iec.operation = otc.operation AND status_url ='error';")
        data = cur.fetchall()
        resultado = []
        for row in data:
            content = {
            'id': row[0],
            'created_at': row[1],
            'country': row[2],
            'unity_identification': row[3],
            'erp_business_unit': row[4],
            'erp_legal_entity': row[5],
            'erp_subsidiary': row[6],
            'acronym': row[7],
            'to_generate_invoice': row[8],
            'origin_system': row[9],
            'minifactu_id': row[10],
            'front_id': row[11],
            'erp_invoice_customer_send_to_erp_at': row[12],
            'erp_invoice_customer_returned_from_erp_at': row[13],
            'erp_invoice_customer_status_transaction': row[14],
            'erp_receivable_sent_to_erp_at': row[15],
            'erp_receivable_returned_from_erp_at': row[16],
            'erp_receivable_customer_identification': row[17],
            'erp_receivable_status_transaction': row[18],
            'erp_invoice_send_to_erp_at': row[19],
            'erp_invoice_returned_from_erp_at': row[20],
            'erp_invoice_status_transaction': row[21],
            'erp_receipt_send_to_erp_at': row[22],
            'erp_receipt_returned_from_erp_at': row[23],
            'erp_receipt_status_transaction': row[24],
            'erp_receivable_id': row[25],
            'erp_clustered_receivable_id': row[26],
            'conciliator_id': row[27],
            'transaction_type': row[28],
            'contract_number': row[29],
            'credit_card_brand': row[30],
            'truncated_credit_card': row[31],
            'price_list_value': row[32],
            'interest_value': row[33],
            'administration_tax_percentage': row[34],
            'administration_tax_value': row[35],
            'billing_date': row[36],
            'credit_date': row[37],
            'registration_gym_student': row[38],
            'fullname_gym_student': row[39],
            'oic_ids': row[40],
            'identification_financial_responsible': row[41],
            'document_kind': row[42],
            'full_name': row[43],
            'front_product_id': row[44],
            'erp_item_ar_id': row[45],
            'erp_item_ar_name': row[46],
            'url': row[47],
            'status_url': row[48]}
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
        cur.close()

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
@app.route('/facturacion_rp/<fecha_inicio>/<fecha_fin>')
def facturacion_rp(fecha_inicio, fecha_fin):
    try:
        cur = mysql.connection.cursor()
        cur.execute("Select CASE WHEN acronym ='LIMVMT1' THEN '75' WHEN acronym ='CUSCUS1' THEN '31' WHEN acronym ='HUAHUA1' THEN '6' WHEN acronym ='PIUPIU1' THEN '10' WHEN acronym ='LIMATE1' THEN '59' WHEN acronym ='TRUTRU2' THEN '14' WHEN acronym ='CHICLA2' THEN '3' WHEN acronym ='CAJCAJ1' THEN '33' WHEN acronym ='AQPCAY2' THEN '1' END as Numeracion, CASE WHEN acronym ='LIMVMT1' THEN 'LC-410'  WHEN acronym ='CUSCUS1' THEN 'TI-04' WHEN acronym ='HUAHUA1' THEN 'LE-02'  WHEN acronym ='PIUPIU1' THEN 'TI-04' WHEN acronym ='LIMATE1' THEN 'TI-206' WHEN acronym ='TRUTRU2' THEN 'LC-201'  WHEN acronym ='CHICLA2' THEN 'LC-203' WHEN acronym ='CAJCAJ1' THEN 'TI-01'  WHEN acronym ='AQPCAY2' THEN 'LC-301' END as Cod_RP,date_format(otc.created_at, '%Y-%m-%d') as Fecha, date_format(otc.created_at, '%Y%m%d') fecha_rp, count(*) as Total_Transacciones, round(SUM(r.price_list_value/1.18),2) as Base_Imponible from oic_db.order_to_cash otc inner join oic_db.receivable r on otc.id = r.order_to_cash_id  where date_format(otc.created_at, '%Y-%m-%d')  between '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' and country ='Peru' and otc.erp_invoice_status_transaction ='created_at_erp' and acronym in  ('LIMVMT1', 'CUSCUS1','HUAHUA1', 'PIUPIU1','LIMATE1','TRUTRU2','CHICLA2', 'CAJCAJ1', 'AQPCAY2') group by acronym order by Fecha desc;")
        data = cur.fetchall()
        resultado = []
        for row in data:
            content = {
            'Numeracion': row[0],
            'cod_RP': row[1],
            'Fecha': row[2],
            'fecha_rp': row[3],
            'Total_Transacciones': row[4],
            'Base_Imponible': row[5]}
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
        cur.close()
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

@app.route('/gesplan/<fecha_inicio>/<fecha_fin>')
def gesplan(fecha_inicio, fecha_fin):
    try:
        cur = mysql.connection.cursor()
        cur.execute("SELECT otc.minifactu_id, date_format(otc.created_at, '%Y-%m-%d') created_at,otc.erp_receipt_status_transaction , otc.erp_business_unit, otc.front_id, otc.erp_subsidiary, recg.erp_source_name, recg.erp_type_transaction, recg.erp_payments_terms, recg.erp_currency_code, recg.erp_currency_conversion_type, crc.identification_financial_responsible, crc.full_name, date_format(rec.credit_date, '%Y-%m-%d') AS fecha_cobro, rftv.bank_number, right(rftv.bank_branch, 4) AS bank_branch, convert(rftv.bank_account, unsigned) AS bank_account, RTRIM(concat('RD_', rftv.bank_number, ' ', right(rftv.bank_branch, 4), ' ', convert(rftv.bank_account, unsigned))) Receipt_Method, RTRIM(concat(crc.identification_financial_responsible, 'Faturar')) AS Customer_Site, cast(IFNULL(rec.gross_value, 0) AS decimal(18, 2)) - cast(IFNULL(rec.administration_tax_value, 0) AS decimal(18, 2))AS comision, rec.credit_card_brand,rec.contract_number, rec.transaction_type, date_format(rec.billing_date, '%Y-%m-%d') billing_date, rec.price_list_value, rec.nsu,  date_format(rec.credit_date, '%Y-%m-%d') credit_date, rec.gross_value, rec.authorization_code, rec.erp_clustered_receivable_id,  rec.transaction_type payment_method FROM receivable rec INNER JOIN order_to_cash otc ON otc.id = rec.order_to_cash_id  INNER JOIN  customer crc  ON crc.identification_financial_responsible = otc.erp_receivable_customer_identification  LEFT JOIN  receivable_erp_configurations recg  ON recg.country = otc.country AND recg.origin_system = otc.origin_system AND recg.operation = otc.operation AND recg.transaction_type = rec.transaction_type AND recg.erp_business_unit = otc.erp_business_unit AND recg.memoline_setting = 'gross_value' AND recg.converted_smartfin = rec.converted_smartfin LEFT JOIN receipt_from_to_version rftv ON rftv.origin_system = otc.origin_system AND rftv.order_to_cash_operation = otc.operation AND rftv.erp_business_unit = otc.erp_business_unit AND rftv.receivable_transaction_type = rec.transaction_type AND rftv.erp_receivable_customer_identification = crc.identification_financial_responsible WHERE date_format(otc.created_at, '%Y-%m-%d') between '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' AND otc.country = 'Peru'")
        data = cur.fetchall()
        resultado = []
        for row in data:
            content = {
            'minifactu_id': row[0],
            'created_at': row[1],
            'erp_receipt_status_transaction': row[2],
            'erp_business_unit': row[3],
            'front_id': row[4],
            'erp_subsidiary': row[5],
            'erp_source_name': row[6],
            'erp_type_transaction': row[7],
            'erp_payments_terms': row[8],
            'erp_currency_code': row[9],
            'erp_currency_conversion_type': row[10],
            'identification_financial_responsible': row[11],
            'full_name': row[12],
            'fecha_cobro': row[13],
            'bank_number': row[14],
            'bank_branch': row[15],
            'bank_account': row[16],
            'Receipt_Method': row[17],
            'Customer_Site': row[18],
            'comision': row[19],
            'credit_card_brand': row[20],
            'contract_number': row[21],
            'transaction_type': row[22],
            'billing_date': row[23],
            'price_list_value': row[24],
            'nsu': row[25],
            'credit_date': row[26],
            'gross_value': row[27],
            'authorization_code': row[28],
            'erp_clustered_receivable_id': row[29],
            'payment_method': row[30]}
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
        cur.close()

@app.route('/scheduled_pe')
def scheduled_pe():
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute("select operadora ,brand ,date_format(due_on, '%Y-%m-%d') due_on ,acronym ,count(*) as Total_TX,sum(amount) as Valorizado  from prod_lake_modeled_refined.fin_otc where data_sistema_front  is null and operadora  in ('MCprocesosPERU','peru_interbank','VisanetPERU') and status_front ='scheduled' group by operadora,brand,due_on  ,acronym")
        resultado = []
        for row in cursor:
            content = {
            'operadora':row[0],
            'brand':row[1],
            'due_on':row[2],
            'acronym':row[3],
            'Total_TX':row[4],
            'Valorizado':row[5]}
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

#AQUI INICIA LOS API REST PARA VOXIVA

@app.route('/unidades/<geografico>')
def unidades(geografico):
        try:
            cars = []
            cursor = conn.cursor()
            cursor.execute("SELECT ID_UNIDAD ,CODIGO ,UNIDAD ,RUC ,COMPANIA ,SERIE_BOLETAS ,SERIE_FACTURAS ,COMERCIO_VISA_INGENICO ,COMERCIO_MC_INGENICO ,ESTADO ,CODIGO_TCI ,CODIGO_ESTABLECIMIENTO_SUNAT ,DIRECCION ,DISTRITO ,PROVINCIA ,DEPARTAMENTO ,UBIGEO_UNIDAD ,AFORO ,CONVERT(DATE,FECHA_INAUGURACION,103)FECHA_INAUGURACION ,LATITUD ,LONGITUD ,CONVERT(DATE,FECHA_DE_CREACION,103)FECHA_DE_CREACION ,UNIFIED_LOCATION_ID ,ERP_BUSINESS_UNIT ,ERP_SUBSIDIARY ,BRAND ,ESTADO_UNIDAD ,TIPO ,INVENTORY_ORGANIZATION ,LOCATION ,ID_UNIDAD_SMARTSYSTEM ,FLAG_TUNQUI ,REGION_UNIDAD ,LIDER_REGIONAL_UNIDAD ,NRO_DOCUMENTO_REGIONAL FROM PROCESOS_SMARTFIT.SMARTFIT.UNIDADES_SMARTFIT_PERU WHERE ESTADO_UNIDAD = 'ACTIVO' AND TIPO = 'Filial' AND CODIGO NOT IN ('LIMADM1','LIMADM2','LIMADM3','LIMADM5','SMFERP1','LIMCOR1','LIMCOR2','LIMCOR3','CALBEL2') AND DEPARTAMENTO = '"+str(geografico)+"' ")
            for row in cursor.fetchall():
                content = {
                        'ID_UNIDAD': row[0],
                            'CODIGO': row[1],
                            'UNIDAD': row[2],
                            'RUC': row[3],
                            'COMPANIA': row[4],
                            'SERIE_BOLETAS': row[5],
                            'SERIE_FACTURAS': row[6],
                            'COMERCIO_VISA_INGENICO': row[7],
                            'COMERCIO_MC_INGENICO': row[8],
                            'ESTADO': row[9],
                            'CODIGO_TCI': row[10],
                            'CODIGO_ESTABLECIMIENTO_SUNAT': row[11],
                            'DIRECCION': row[12],
                            'DISTRITO': row[13],
                            'PROVINCIA': row[14],
                            'DEPARTAMENTO': row[15],
                            'UBIGEO_UNIDAD': row[16],
                            'AFORO': row[17],
                            'FECHA_INAUGURACION': row[18],
                            'LATITUD': row[19],
                            'LONGITUD': row[20],
                            'FECHA_DE_CREACION': row[21],
                            'UNIFIED_LOCATION_ID': row[22],
                            'ERP_BUSINESS_UNIT': row[23],
                            'ERP_SUBSIDIARY': row[24],
                            'BRAND': row[25],
                            'ESTADO_UNIDAD': row[26],
                            'TIPO': row[27],
                            'INVENTORY_ORGANIZATION': row[28],
                            'LOCATION': row[29],
                            'ID_UNIDAD_SMARTSYSTEM': row[30],
                            'FLAG_TUNQUI': row[31],
                            'REGION_UNIDAD': row[32],
                            'LIDER_REGIONAL_UNIDAD': row[33],
                            'NRO_DOCUMENTO_REGIONAL': row[34]}
                cars.append(content)
            return jsonify(cars)
        except Exception as e:
            print(e)

#ws_relatorio
@app.route('/relatorio/<fecha_inicio>/<fecha_fin>/<geografico>')
def relatorio(fecha_inicio,fecha_fin,geografico):
        try:
            cars = []
            cursor = conn.cursor()
            cursor.execute(" SELECT ID_RELATORIO ,REFERENCIA ,MES ,CONVERT(DATE,FECHA,103)FECHA ,DIA ,UNIDAD ,VALOR_PAGO ,STATUS ,TIPO_TARJETA ,TIPO_COBRANZA ,TENTATIVA_DE_COBRANZA ,TENTATIVA_DE_COBRANZA_TOTAL ,CODIGO_IMPORTACION ,CODIGO_PAGAMENTO ,CONVERT(DATE,FECHA_VENCIMIENTO_RELATORIO,103)FECHA_VENCIMIENTO_RELATORIO ,TIPO_COBRANZA_2 ,CODIGO_RESPUESTA ,DESCRIPCION_RESPUESTA ,COD_ALUMNO ,CONVERT(DATE,FECHA_IMPORTACION,103)FECHA_IMPORTACION ,ID_FIN ,CODIGO_CONTRATO ,NUMERO_DE_REFERENCIA_RELATORIO ,PRODUCTO_RELATORIO ,CONVERT(DATE,FECHA_DE_CREACION,103)FECHA_DE_CREACION FROM VOXIVA.DWH.MAESTRO_RELATORIO_FIN  WHERE CONVERT(DATE,FECHA,103) BETWEEN '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' and DEPARTAMENTO = '"+str(geografico)+"' ")
            for row in cursor.fetchall():
                content = {
                        'ID_RELATORIO': row[0],
                            'REFERENCIA': row[1],
                            'MES': row[2],
                            'FECHA': row[3],
                            'DIA': row[4],
                            'UNIDAD': row[5],
                            'VALOR_PAGO': row[6],
                            'STATUS': row[7],
                            'TIPO_TARJETA': row[8],
                            'TIPO_COBRANZA': row[9],
                            'TENTATIVA_DE_COBRANZA': row[10],
                            'TENTATIVA_DE_COBRANZA_TOTAL': row[11],
                            'CODIGO_IMPORTACION': row[12],
                            'CODIGO_PAGAMENTO': row[13],
                            'FECHA_VENCIMIENTO_RELATORIO': row[14],
                            'TIPO_COBRANZA_2': row[15],
                            'CODIGO_RESPUESTA': row[16],
                            'DESCRIPCION_RESPUESTA': row[17],
                            'COD_ALUMNO': row[18],
                            'FECHA_IMPORTACION': row[19],
                            'ID_FIN': row[20],
                            'CODIGO_CONTRATO': row[21],
                            'NUMERO_DE_REFERENCIA_RELATORIO': row[22],
                            'PRODUCTO_RELATORIO': row[23],
                            'FECHA_DE_CREACION': row[24]}
                cars.append(content)
            return jsonify(cars)
        except Exception as e:
            print(e)

#ws_promociones
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
#ws_ingenico
@app.route('/ingenico/<fecha_inicio>/<fecha_fin>/<geografico>')
def ingenico(fecha_inicio,fecha_fin,geografico):
        try:
            cars = []
            cursor = conn.cursor()
            cursor.execute(" SELECT COMERCIO_ORDEN_ID ,CONVERT(DATE,FECHA,103)FECHA ,UNIDAD ,VALOR_PAGO ,ESTADO ,TIPO_COBRANZA ,TENTATIVA_DE_COBRANZA_TOTAL ,CODIGO_PAGAMENTO ,COD_ALUMNO ,MODO_INGRESO ,TIPO ,COMERCIO ,COD_PRODUCTO ,PAN ,COD_ACCION ,MOTIVO_NEGACION ,RESP_COD_MOP ,RESP_MOP ,RESP_COD_PSP ,RESP_PSP ,RESP_EXT_PSP ,BIN ,PLANN ,TRY_CONVERT(DATE,FECHA_VENCIMIENTO,103)FECHA_VENCIMIENTO ,TRY_CONVERT(DATE,FECHA_PUBLICACION,103)FECHA_PUBLICACION ,NRO_COMPROBANTE ,TIPO_COMPROBANTE FROM VOXIVA.DWH.INGENICO WHERE CONVERT(DATE,FECHA,103) BETWEEN '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' and DEPARTAMENTO = '"+str(geografico)+"' ")
            for row in cursor.fetchall():
                content = {
                        'COMERCIO_ORDEN_ID': row[0],
                            'FECHA': row[1],
                            'UNIDAD': row[2],
                            'VALOR_PAGO': row[3],
                            'ESTADO': row[4],
                            'TIPO_COBRANZA': row[5],
                            'TENTATIVA_DE_COBRANZA_TOTAL': row[6],
                            'CODIGO_PAGAMENTO': row[7],
                            'COD_ALUMNO': row[8],
                            'MODO_INGRESO': row[9],
                            'TIPO': row[10],
                            'COMERCIO': row[11],
                            'COD_PRODUCTO': row[12],
                            'PAN': row[13],
                            'COD_ACCION': row[14],
                            'MOTIVO_NEGACION': row[15],
                            'RESP_COD_MOP': row[16],
                            'RESP_MOP': row[17],
                            'RESP_COD_PSP': row[18],
                            'RESP_PSP': row[19],
                            'RESP_EXT_PSP': row[20],
                            'BIN': row[21],
                            'PLANN': row[22],
                            'FECHA_VENCIMIENTO': row[23],
                            'FECHA_PUBLICACION': row[24],
                            'NRO_COMPROBANTE': row[25],
                            'TIPO_COMPROBANTE': row[26]}
                cars.append(content)
            return jsonify(cars)
        except Exception as e:
            print(e)

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

#ws_bin
@app.route('/wsbin/<bin>')
def wsbin(bin):
        try:
            cars = []
            cursor = conn.cursor()
            cursor.execute("SELECT TIPO,BIN,CATEGORY,BRAND,ALPHA,CONTRY_NAME FROM VOXIVA.DWH.BIN where BIN = '"+str(bin)+"'  ")
            for row in cursor.fetchall():
                content = {
                        'TIPO': row[0],
                        'BIN': row[1],
                        'CATEGORY': row[2],
                        'BRAND': row[3],
                        'ALPHA': row[4],
                        'CONTRY_NAME': row[5]}
                cars.append(content)
            return jsonify(cars)
        except Exception as e:
            print(e)

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

#AQUI TERMINA API REST DE VOXIVA
#API SERVICIOS
#Api topdesk_odata_backlog
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
        cursor.execute(" with payments as (select 	purc. id purchase_id 	,otc.id_payment 	,pay.payed_at 	,pay.due_at  	,pay.state payment_state 	,coalesce(prod.description,pay.payable_type)  product_name 	,pay.amount_paid 	,otc.forma_pagamento 	,loc.acronym acronym_payet from 	prod_lake_modeled_refined.minifactu_otc otc left join prod_lake_ss_refined.payments pay on 	otc.id_payment = pay.id left join prod_lake_ss_refined.memberships mem on 	pay.payable_id = mem.id and lower(pay.payable_type) = 'membership' left join prod_lake_modeled_refined.dim_locations loc on 	pay.location_id = loc.id 	and loc.country = 'Peru' left join prod_lake_ss_refined.services ser on 	pay.payable_id = ser.id and lower(pay.payable_type) = 'service' left join prod_lake_ss_refined.products prod on 	ser.product_id = prod.id left join prod_lake_ss_refined.purchases purc on 	purc.id = case when pay.payable_type = 'Membership' then mem.purchase_id else ser.purchase_id end ) select 	 purc.promotion_id id_promocion 	,date_format(prom.starts_at,'%Y-%m-%d') fecha_inicio_promo 	,date_format(prom.expires_at,'%Y-%m-%d') vigencia_promo 	,prom.active estado_promocion 	,replace(prom.title, '|', ' ')title 	,purc.id id_compra 	,date_format(purc.created_at,'%Y-%m-%d')fecha_de_compra 	,date_format(purc.confirmed_at,'%Y-%m-%d') fecha_de_confirm_compra 	,date_format(purc.cancelled,'%Y-%m-%d') fecha_cancel_compra 	,date_format(purc.expired_at,'%Y-%m-%d') fecha_exp_compra 	,purc.person_id cod_matricula 	,cli.cliente_nome nombre_cliente 	,cli.cliente_idade edad_cliente 	,peo.status_aluno 	,peo.status_catraca 	,plan.name nombre_plan 	,loc.acronym cod_unidad_compra 	,case 	 		when purc.created_at is not null and purc.expired_at is null then 'Activo' 	 		else 'Cancelado' 	end status_compra 	,p.id_payment payment_id 	,date_format(p.due_at,'%Y-%m-%d')due_at 	,date_format(p.payed_at,'%Y-%m-%d')payed_at 	,p.amount_paid 	,p.payment_state 	,date_format(purc.load_datetime,'%Y-%m-%d')load_datetime from prod_lake_ss_refined.purchases purc left join prod_lake_modeled_salesforce_latam.salesforce_dim_clientes_latam cli on 	cast(purc.person_id as varchar(30)) = cli.cliente_person_id and cli.cliente_pais = 'Peru' left join prod_lake_modeled_refined.dim_people peo on 	purc.person_id = peo.person_id left join prod_lake_modeled_refined.dim_locations loc on 	purc.original_location_id = loc.id left join prod_lake_modeled_refined.dim_promotions prom on 	purc.promotion_id = prom.id left join prod_lake_ss_refined.plans plan on 	purc.plan_id = plan.id left join payments p on 	purc.id = p.purchase_id where purc.original_location_id in (select id from prod_lake_modeled_refined.dim_locations where country='Peru') and purc.promotion_id  in (11832, 11983, 12198, 12401, 12448, 12543, 12688, 13071, 13127, 13450, 13600, 13565, 14041, 14233) and date_format(purc.created_at,'%Y-%m-%d') between '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' ")
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

server_name = app.config['SERVER_NAME']
if server_name and ':' in server_name:
    host, port = server_name.split(":")
    port = int(port)
else:
    port = 1247
    host = "0.0.0.0"

    app.run(debug=True,host=host, port=port)
