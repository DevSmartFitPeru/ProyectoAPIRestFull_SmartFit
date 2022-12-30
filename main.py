from flask import Flask, jsonify
from flask_mysqldb import MySQL
app = Flask(__name__)
app.config['MYSQL_DATABASE_HOST'] = '127.0.0.1'
app.config['MYSQL_DATABASE_PORT'] = '3306'
app.config['MYSQL_DATABASE_USER'] = 'root'
app.config['MYSQL_DATABASE_PASSWORD'] = 'root'
app.config['MYSQL_DATABASE_DB']	= 'dwh'
app.config['MYSQL_DATABASE_CHARSET'] = 'utf-8'
mysql = MySQL(app)

@app.route('/user')
def getAllUser():
    cur = mysql.connection.cursor()
    cur.execute('SELECT id, firstname, lastname, email, reg_date FROM myguests')
    data = cur.fetchall()
    result=[]
    for row in data:
        content ={
        'id':row[0],
        'firstname': row[1],
        'lastname': row[2],
        'email': row[3],
        'reg_date': row[4]
          }
    result.append(content)
    return jsonify(result)

@app.route('/')
def index():
    resultrado ={
        "name":"Luis",
        "last_name": "Azañero"

    }

if __name__ == '__main__':
 app.run(None, 3000, True)