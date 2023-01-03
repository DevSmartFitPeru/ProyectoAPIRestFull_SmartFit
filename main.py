from flask import Flask, jsonify
from flask_mysqldb import MySQL
from pyathena import connect


app = Flask(__name__)

app.config['MYSQL_HOST'] = 'oic-db-test.cgzshounia8v.us-east-1.rds.amazonaws.com'
app.config['MYSQL_USER'] = 'luis.azanero'
app.config['MYSQL_PASSWORD'] = 'smart#123'
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
        cursor.execute("SELECT acronym  from prod_lake_modeled_refined.dim_locations where country ='Peru'")
        resultado = []
        for row in cursor:
            content = { 'acronym':row[0] }
            resultado.append(content)
        return jsonify(resultado)

    except Exception as e:
        print(e)
    finally:
        cursor.close()

@app.route('/minifactu/<fecha_inicio>')
def minifactu(fecha_inicio):
    try:
        cursor = connect(aws_access_key_id="AKIA4LTBLLTUCHTCM2ZY", aws_secret_access_key="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", s3_staging_dir="s3://7-smartfit-da-de-lake-artifacts-athena-latam/", region_name="us-east-1", work_group="peru", schema_name="prod_lake_modeled_refined").cursor()
        cursor.execute("select id_payment,	status_pagamento,	date_format(payed_at, '%Y-%m-%d') payed_at,	amount_paid,	pag_elegivel,	propriedade,	forma_pagamento,	country,	acronym,	external_id,	minifactu_id,	errors,	validacao_erro,	retornou_minifactu,	error,	validacao_coerce,	retornou_front,	gross_value,	amount_pag_elegivel,	exportado_minifactu,	exportado_sistema_externo,	amount_validacao_coerce,	amount_retornou_minifactu,	amount_validacao_erro,	amount_retornou_front,	date_format(load_datetime, '%Y-%m-%d') load_datetime,	amount_exportado_sistema_externo,	amount_exportado_minifactu from prod_lake_modeled_refined.minifactu_otc where payed_at = cast('"+str(fecha_inicio)+"' as date) and country='Peru'")
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
        cursor.execute("SELECT fin.id ,fin .external_id ,fin .origin_system ,fin .amount ,date_format(fin .due_on, '%Y-%m-%d')due_on ,fin.status_pagamento ,fin .forma_pagamento ,fin .status_front ,fin .data_sistema_front, fin.brand ,fin .operadora ,fin .acronym ,fin.country,otc .external_id ,otc.status_pagamento ,otc.errors,otc.error ,date_format(otc.payed_at, '%Y-%m-%d')payed_at FROM prod_lake_modeled_refined.fin_otc fin left join prod_lake_modeled_refined.minifactu_otc otc on fin .external_id = otc.id_payment where otc.payed_at between cast('"+str(fecha_inicio)+"' as date) and  cast('"+str(fecha_fin)+"' as date) and otc.country='Peru'")
        resultado = []
        for row in cursor:
            content = { 'id':row[0],
            'external_id':row[1],
            'origin_system':row[2],
            'amount':row[3],
            'due_on':row[4],
            'status_pagamento':row[5],
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
            'payed_at':row[17]}
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


if __name__ == '__main__':
    app.run(debug=True)
