"""
Utilisateurs devs :
épidémiologist : id: 0e61cae8-546f-404f-a33a-7069127118c5 password : testing
utilisateur normal : id:usdd password : usdd


"""

from flask import Flask, render_template, url_for, redirect, request, session
import os
import string
import random
from flask_mysql_connector import MySQL
from sql_commands import project_requests, database_commands
from datetime import timedelta

# Paramètres de Flask
app = Flask(__name__)
app.secret_key = os.urandom(24)
app.permanent_session_lifetime = timedelta(minutes=30)
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = ''
app.config['MYSQL_DATABASE'] = 'coviddata'
app.config['DEBUG'] = True

# Les variables globales
mysql = MySQL(app)  # la BDD
epidemiologist = False  # pour choisir l'interface à afficher
thisUserId = ""  # Stock l'ID de l'utilisateur connecté


def executeMySqlCommand(query):
    """
    exécute la query sql
    """
    cur = mysql.connection.cursor()
    cur.execute(query)
    fetchedData = cur.fetchall()
    cur.close()
    return fetchedData


def generate_user_id():
    """
    Génère aléatoirement un ID
    J'ai enlevé la vérification car les chances de tomber sur une clé qui existe déjà sont vraiment minime
    """
    p1 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
    p2 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    p3 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    p4 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    p5 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))

    newId = p1 + "-" + p2 + "-" + p3 + "-" + p4 + "-" + p5
    return newId


def createUser(username, first_name, last_name, address, password):
    """
    Créer un nouvel utilisateur dans la bdd et renvoie le nouvel user ID crée.
    """
    newId = generate_user_id()
    insertCommand = "INSERT INTO person(id, first_name, last_name, username, address, password) VALUES ('%s', '%s', '%s', '%s', '%s', '%s');" % (
        newId, first_name, last_name, username, address, password)
    cur = mysql.connection.cursor()
    cur.execute(insertCommand)
    cur.execute("COMMIT;")
    cur.close()
    print(insertCommand)
    return newId


def accountExists(given_userID, given_password):
    """
    Vérifie si le compte existe dans la bdd.
    """
    isUserValidCommand = ("SELECT EXISTS (SELECT * FROM person WHERE person.id = '%s' AND person.password= '%s');" % (
        given_userID, given_password))

    res = executeMySqlCommand(isUserValidCommand)
    if res[0][0] == 1:
        return True
    else:
        return False


def isUserEpidemiologist(userId):
    """
    Vérifie si le compte qui se connecte est un épidemiologiste
    """
    epidemiologistIdCommand=("SELECT EXISTS (SELECT * FROM epidemiologist WHERE epidemiologist.id_person = '%s');" % userId)
    res = executeMySqlCommand(epidemiologistIdCommand)
    if res[0][0] == 1:
        return True
    else:
        return False


def addNewDataToHospitals(iso_code, date, icu_patients, hosp_patients):
    """
    Ajoute les données dans la table hospitalsdata

    TODO: Si iso_code n'éxiste pas lance une erreur de fk
    """
    userId = thisUserId
    insertCommand = "INSERT INTO hospitalsdata (iso_code, date, icu_patients, hosp_patients, source_epidemiologist) VALUES ('%s', '%s', '%s', '%s', '%s');" % (
        iso_code, date, icu_patients, hosp_patients, userId)
    cur = mysql.connection.cursor()
    cur.execute(insertCommand)
    cur.execute("COMMIT;")
    cur.close()
    print(insertCommand)


########################################################################################################################
"""
Les méthodes qui gèrent les pages de l'appli web:
- /login et /
- /logout
- /register
- /lostPassword
- /homePage
- /showData
- /showRequest
- /modifyData
"""


@app.route('/login', methods=["GET", "POST"])
@app.route('/', methods=["GET", "POST"])
def login():
    if request.method == "POST":
        session.pop('user', None)
        given_userID = request.form['userID']
        given_password = request.form['password']

        if accountExists(given_userID, given_password) and given_password != "" and given_userID != "":
            global epidemiologist, thisUserId
            epidemiologist = isUserEpidemiologist(given_userID)
            thisUserId = given_userID
            session['user'] = request.form['userID']
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
    global thisUserId
    session.pop('user', None)
    thisUserId = ""
    return render_template('login.html')


@app.route('/register', methods=["GET", "POST"])
def register():
    if request.method == "POST":
        username = request.form["username"]
        first_name = request.form["first_name"]
        last_name = request.form["last_name"]
        address = request.form["address"]
        password = request.form["password"]
        confirm_password = request.form["confirm_password"]
        if username == "" or first_name == "" or last_name == "" or address == "" or password == "" or confirm_password == "" or (
                password != confirm_password):
            return render_template('register.html', label="Informations pas correctes")
        else:
            id = createUser(username, first_name, last_name, address, password)
            if id != "":
                return render_template('register.html',
                                       label="Votre compte a été créer, voici votre userID qui vous permettra de vous connecter : %s" % (
                                           id))
            else:
                return render_template('register.html', label="Le compte n'a pas pu être créer.")
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
            iso_code = request.form["iso_code"]
            date = request.form["date"]
            icu_patients = request.form["icu_patients"]
            hosp_patients = request.form["hosp_patients"]
            if iso_code != "" and date != "" and icu_patients != "" and hosp_patients:
                addNewDataToHospitals(iso_code, date, icu_patients, hosp_patients)
                return render_template("homePage.html", userEpi=epidemiologist)
            else:
                return render_template("modifyHospitalsData.html", userEpi=epidemiologist,
                                       label="Il faut compléter tous les champs")
        else:
            return render_template("modifyHospitalsData.html", userEpi=epidemiologist)
    else:
        return redirect(url_for('login'))


if __name__ == '__main__':
    app.run()
