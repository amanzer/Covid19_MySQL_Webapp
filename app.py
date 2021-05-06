from flask import Flask, render_template, url_for, redirect, abort, request, session, g
import os
from flask_mysql_connector import MySQL
from sql_commands import project_requests, database_commands

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = ''
app.config['MYSQL_DATABASE'] = 'coviddata'
app.config['DEBUG'] = True

mysql = MySQL(app)
epidemiologist = False


def executeMySqlCommand(query):
    cur = mysql.connection.cursor()
    cur.execute(query)
    fetchedData = cur.fetchall()
    cur.close()
    return fetchedData


def isValidAccount(given_username, given_password):
    # sql_query = "SELECT EXISTS ( SELECT * FROM person WHERE username = ? AND password = ? )"
    # cur = mysql.connection.cursor()
    # cur.execute(sql_query, given_username, given_password)
    # fetchedData = cur.fetchall()
    # cur.close()
    # if fetchedData == 1:
    #     return True
    # else:
    #     return False
    return True


def isUserEpidemiologist():
    return True


@app.route('/', methods=["GET", "POST"])
def login():
    if request.method == "POST":
        session.pop('user', None)

        given_username = request.form['username']
        given_password = request.form['password']
        if isValidAccount(given_username, given_password):
            global epidemiologist
            epidemiologist = isUserEpidemiologist()
            session['user'] = request.form['username']
            return redirect(url_for('home'))

    return render_template('login.html')


@app.route('/logout')
def logout():
    session.pop('user', None)
    return render_template('login.html')



@app.route('/homePage')
def home():
    return render_template("homePage.html", userEpi=epidemiologist)


@app.route('/showData/<int:message>', methods=["GET", "POST"])
def showData(message):
    sqlCommand = database_commands[message]
    dataList = executeMySqlCommand(sqlCommand)
    if epidemiologist and message == 4:
        return render_template('executionEpidemiologist.html', data=dataList)
    else:
        return render_template('execution.html', data=dataList)


@app.route('/showRequest/<int:message>', methods=["GET", "POST"])
def showRequest(message):
    sqlCommand = project_requests[message]
    dataList = executeMySqlCommand(sqlCommand)
    return render_template('execution.html', data=dataList, userEpi=epidemiologist)


@app.route('/modifyData', methods=["GET", "POST"])
def modifyData():
    return render_template("modifyHospitalsData.html", userEpi=epidemiologist)

if __name__ == '__main__':
    app.run()
