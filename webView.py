from flask import Flask, render_template, url_for, redirect, abort
from flask_mysql_connector import MySQL

app = Flask(__name__)
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'password'
app.config['MYSQL_DATABASE'] = 'coviddata'
app.config['DEBUG'] = True

mysql = MySQL(app)


@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')


@app.route('/results')
def code_execution():
    cur = mysql.connection.cursor()
    sqlQuery = "SELECT * FROM country;"
    cur.execute(sqlQuery)
    fetchedData = cur.fetchall()
    cur.close()
    return render_template('execution.html', data=fetchedData)



if __name__ == '__main__':
    app.run()
