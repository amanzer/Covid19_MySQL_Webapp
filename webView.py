from flask import Flask, render_template, url_for, redirect, abort, request
from flask_mysql_connector import MySQL

app = Flask(__name__)
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = ''
app.config['MYSQL_DATABASE'] = 'coviddata'
app.config['DEBUG'] = True

mysql = MySQL(app)


@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')


@app.route('/results', methods=["POST", "GET"])
def run_command():
    if request.method == "POST":
        sqlQuery = request.form['req']
        if sqlQuery != "":
            #try:
            cur = mysql.connection.cursor()
            cur.execute(sqlQuery)
            fetchedData = cur.fetchall()
            cur.close()
            #except:
            #    return render_template('error.html')
            #finally:
            #    if cur:
            #        cur.close()

            return render_template('execution.html', data=fetchedData)
        else:
            return render_template('index.html')
    else:
        return render_template('index.html')


@app.route('/Error')
def error_handler():
    return render_template('error.html')


if __name__ == '__main__':
    app.run()
