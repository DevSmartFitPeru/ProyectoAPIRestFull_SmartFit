from flask import Flask, render_template, request,jsonify
from flask_mysqldb import MySQL

app = Flask(__name__)

app.config['MYSQL_HOST'] = 'oic-db-test.cgzshounia8v.us-east-1.rds.amazonaws.com'
app.config['MYSQL_USER'] = 'luis.azanero'
app.config['MYSQL_PASSWORD'] = 'smart#123'
app.config['MYSQL_DB'] = 'oic_db'

mysql = MySQL(app)

@app.route('/products', methods=['GET'])
def index():
    cur = mysql.connection.cursor()
    cur.execute("select * from oic_db.product_from_to_version where country ='Peru'")
    rv = cur.fetchall()
    result = []
    for row in rv:
        content = {
            'id': row[0],
            'created_at': row[1],
            'country': row[2],
            'product_from_to_origin_system': row[3],
            'product_from_to_operation': row[4],
            'product_from_to_front_product_id': row[5],
            'front_product_name': row[6],
            'erp_item_ar_id': row[7],
            'erp_item_ar_name': row[8],
            'erp_item_ar_overdue_recovery_id': row[9],
            'erp_item_ar_overdue_recovery_name': row[10],
            'erp_item_ar_discount_id': row[11],
            'erp_gl_segment_id': row[12],
            'erp_gl_segment_name': row[13],
            'erp_ncm_code': row[14],
            'to_generate_fiscal_document': row[15]
        }
        result.append(content)
        return jsonify(result)

if __name__ == '__main__':
            app.run(debug=True)