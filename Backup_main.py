import requests as requests
from flask import Flask, jsonify,request
from flask_mysqldb import MySQL
from pyathena import connect


app = Flask(__name__)

app.config['MYSQL_HOST'] = 'oic-db-prod.cgzshounia8v.us-east-1.rds.amazonaws.com'
app.config['MYSQL_USER'] = 'luis.azanero'
app.config['MYSQL_PASSWORD'] = 'Lu1s0Ic2023'
app.config['MYSQL_DB'] = 'oic_db'
mysql = MySQL(app)


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
        cursor.execute("select id_payment,	status_pagamento,	date_format(payed_at, '%Y-%m-%d') payed_at,	amount_paid,	pag_elegivel,	propriedade,	forma_pagamento,	country,	acronym,	external_id,	minifactu_id,	errors,	validacao_erro,	retornou_minifactu,	error,	validacao_coerce,	retornou_front,	gross_value,	amount_pag_elegivel,	exportado_minifactu,	exportado_sistema_externo,	amount_validacao_coerce,	amount_retornou_minifactu,	amount_validacao_erro,	amount_retornou_front,	date_format(load_datetime, '%Y-%m-%d') load_datetime,	amount_exportado_sistema_externo,	amount_exportado_minifactu from prod_lake_modeled_refined.minifactu_otc where date_format(payed_at, '%Y-%m-%d') between '"+str(fecha_inicio)+"' and '"+str(fecha_fin)+"' and country='Peru'")
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
        cursor.execute("select operadora ,brand ,date_format(due_on, '%Y-%m-%d') due_on ,acronym ,count(*) as Total_TX,sum(amount) as Valorizado  from prod_lake_modeled_refined.fin_otc where data_sistema_front  is null and operadora  in ('MCprocesosPERU','peru_interbank','VisanetPERU') and status_front ='scheduled' and status_pagamento ='rejeitado' group by operadora,brand,due_on  ,acronym")
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

if __name__ == '__main__':
    app.run(debug=True)
