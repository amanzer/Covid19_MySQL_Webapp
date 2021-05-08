from flask import Flask, render_template, url_for, redirect, abort, request, session, g
import os
from flask_mysql_connector import MySQL
from sql_commands import project_requests, database_commands, verifyEpidemiologist
from datetime import timedelta

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.permanent_session_lifetime = timedelta(minutes=30)
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'password'
app.config['MYSQL_DATABASE'] = 'coviddata'
app.config['DEBUG'] = True

mysql = MySQL(app)
epidemiologist = False


# class User:
#     def __init__(self, id=None, first_name=None, last_name=None, username=None, address=None, password=None):
#         self.id = id
#         self.username = username
#         self.first_name = first_name
#         self.last_name = last_name
#         self.username = username
#         self.address = address
#         self.password = password
#
#     def __repr__(self):
#         return f'<User>'
#
#     def logout(self):
#         self.id=None
#         self.username = None
#         self.first_name = None
#         self.last_name = None
#         self.username = None
#         self.address = None
#         self.password = None


def executeMySqlCommand(query):
    cur = mysql.connection.cursor()
    cur.execute(query)
    fetchedData = cur.fetchall()
    cur.close()
    return fetchedData


def isValidAccount(given_username, given_password):
    str= ("SELECT EXISTS (SELECT * FROM person WHERE person.username = '%s' AND person.password= '%s');" % (given_username, given_password))

    res =executeMySqlCommand(str)
    if res[0][0] == 1:
        return True
    else:
        return False


def isUserEpidemiologist():
    return True


@app.route('/login', methods=["GET", "POST"])
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
            # session.permanent =True
            return redirect(url_for('home'))
        else:
            return render_template('login.html', label="Ce compte n'éxiste pas")
    else:
        if "user" in session:
            return redirect(url_for('home'))
        else:
            return render_template('login.html')


@app.route('/logout')
def logout():
    session.pop('user', None)
    return render_template('login.html')


@app.route('/register')
def register():
    if request.method == "POST":
        pass
    else:
        return render_template('register.html')


@app.route('/lostPassword')
def lostPassword():
    return render_template('lostPassword.html')


@app.route('/homePage')
def home():
    if "user" in session:
        return render_template("homePage.html", userEpi=epidemiologist)
    else:
        return redirect(url_for('login'))


@app.route('/showData/<int:message>', methods=["GET", "POST"])
def showData(message):
    if "user" in session:
        sqlCommand = database_commands[message]
        dataList = executeMySqlCommand(sqlCommand)
        return render_template('execution.html', data=dataList)
    else:
        return redirect(url_for('login'))


@app.route('/showRequest/<int:message>', methods=["GET", "POST"])
def showRequest(message):
    if "user" in session:
        sqlCommand = project_requests[message]
        dataList = executeMySqlCommand(sqlCommand)
        return render_template('execution.html', data=dataList, userEpi=epidemiologist)
    else:
        return redirect(url_for('login'))


@app.route('/modifyData', methods=["GET", "POST"])
def modifyData():
    if "user" in session:
        if request.method == "POST":
            iso_code=request.form["iso_code"]
            date= request.form["date"]
            icu_patients = request.form["icu_patients"]
            hosp_patients= request.form["hosp_patients"]
            if iso_code != "" and date != "" and icu_patients !="" and hosp_patients:
                print(iso_code +" " + date + " " + icu_patients + " " + hosp_patients)
                return render_template("homePage.html", userEpi=epidemiologist)
            else:
                return render_template("modifyHospitalsData.html", userEpi=epidemiologist, label="Il faut compléter tous les champs")
        else:
            return render_template("modifyHospitalsData.html", userEpi=epidemiologist)
    else:
        return redirect(url_for('login'))


if __name__ == '__main__':
    # thisUser= User()
    app.run()