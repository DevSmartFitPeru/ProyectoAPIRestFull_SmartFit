from flask import Flask, render_template, request,jsonify
from flask_mysqldb import MySQL

app = Flask(__name__)

app.config['MYSQL_HOST'] = 'oic-db-test.cgzshounia8v.us-east-1.rds.amazonaws.com'
app.config['MYSQL_USER'] = 'luis.azanero'
app.config['MYSQL_PASSWORD'] = 'smart#123'
app.config['MYSQL_DB'] = 'oic_db'

mysql = MySQL(app)

@app.route('/planes', methods=['GET'])
def index():
    cur = mysql.connection.cursor()
    cur.execute("select *  from oic_db.plan_from_to_version where country ='Peru'")
    rv = cur.fetchall()
    return jsonify(rv)

if __name__ == '__main__':
    app.run(debug=True)