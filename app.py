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
app.config['MYSQL_PASSWORD'] = 'password'
app.config['MYSQL_DATABASE'] = 'coviddata'
app.config['DEBUG'] = True

# Les variables globales
mysql = MySQL(app)  # la BDD
epidemiologist = False  # pour choisir l'interface à afficher
thisUserId = ""  # Stock l'ID de l'utilisateur connecté


def executeMySqlCommand(query):
    """
    exécute la query sql et renvoie les données obtenu.
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
    return p1 + "-" + p2 + "-" + p3 + "-" + p4 + "-" + p5


def createUser(username, first_name, last_name, address, password):
    """
    Créer un nouvel utilisateur dans la bdd et renvoie le nouveau user ID crée.
    """
    newId = generate_user_id()
    insertCommand = "INSERT INTO person(id, first_name, last_name, username, address, password) VALUES (?, ?, ?, ?, ?, ?);"
    user_args = (newId, first_name, last_name, username, address, password)
    cur = mysql.connection.cursor(prepared=True)
    cur.execute(insertCommand, user_args)
    cur.execute("COMMIT;")
    cur.close()
    print(insertCommand)
    return True


def accountExists(given_username, given_password):
    """
    Vérifie si le compte existe dans la bdd.
    """
    isUserValidCommand = ("SELECT EXISTS (SELECT * FROM person WHERE person.username = '%s' AND person.password= '%s');" % (
        given_username, given_password))
    res = executeMySqlCommand(isUserValidCommand)
    if res[0][0] == 1:
        return True
    else:
        return False


def isUserEpidemiologist(username):
    """
    Vérifie si le compte qui se connecte est un épidemiologiste
    """
    getUserIdCommand=("SELECT person.id FROM person WHERE person.username = '%s';" % (
        username))
    userId = executeMySqlCommand(getUserIdCommand)
    epidemiologistIdCommand = (
            "SELECT EXISTS (SELECT * FROM epidemiologist WHERE epidemiologist.id_person = '%s');" % userId[0][0])

    res = executeMySqlCommand(epidemiologistIdCommand)
    if res[0][0] == 1:
        return True
    else:
        return False


def addNewDataToHospitals(iso_code, date, icu_patients, hosp_patients):
    """
    Ajoute les données dans la table hospitalsdata
    """
    cur = mysql.connection.cursor()
    userId = thisUserId
    sqlCountryExists = "SELECT EXISTS (SELECT * FROM country WHERE country.iso_code = '%s');" % iso_code

    res = executeMySqlCommand(sqlCountryExists)
    ttuple = (iso_code, date, icu_patients, hosp_patients, userId)
    if res[0][0] == 1:
        insertCommand = "INSERT INTO hospitalsdata (iso_code, date, icu_patients, hosp_patients, source_epidemiologist) VALUES (?, ?, ?, ?, ?);"
        cur.execute(insertCommand, ttuple)
        cur.execute("COMMIT;")
    else:
        sqlInsertCountry = "INSERT INTO country(iso_code) VALUES(?)"
        cur.execute(sqlInsertCountry, iso_code)
        insertCommand = "INSERT INTO hospitalsdata (iso_code, date, icu_patients, hosp_patients, source_epidemiologist) VALUES (?, ?, ?, ?, ?);"
        cur.execute(insertCommand, ttuple)
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
        given_username = request.form['username']
        given_password = request.form['password']

        if accountExists(given_username, given_password) and given_password != "" and given_username != "":
            global epidemiologist, thisUserId
            epidemiologist = isUserEpidemiologist(given_username)
            thisUserId = given_username
            session['user'] = request.form['username']
            session.permanent = True
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
            if createUser(username, first_name, last_name, address, password):
                return render_template('register.html',
                                       label="Votre compte a été créer")
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
        if message == 0:
            dataList.insert(0, ('Iso_code', "Continent", "Région", "Pays", "IDH", "Population", "Superficie", "Climat",
                                "Date de 1ére vac."))
        elif message == 1:
            dataList.insert(0, ("id", "Vaccin"))
        elif message == 2:
            dataList.insert(0, ("Iso_code", "Id du Vaccin utilisé"))
        elif message == 3:
            dataList.insert(0, ("Iso_code", "Date", "Tests effectués", "Vaccionations ceffectuées"))
        elif message == 4:
            dataList.insert(0, ("Id", "Iso_code", "Date", "Icu_Patients", "Hosp_patients", "L'ID de l'épidémiologiste"))
        elif message == 5:
            dataList.insert(0, ("Id", "Description"))
        return render_template('execution.html', data=dataList)
    else:
        return redirect(url_for('login'))


@app.route('/showRequest/<int:message>', methods=["GET", "POST"])
def showRequest(message):
    if "user" in session:
        sqlCommand = project_requests[message]
        dataList = executeMySqlCommand(sqlCommand)
        messageToDisplay = ""
        if message == 0:
            dataList.insert(0, ("Pays",))
            messageToDisplay = "Les pays qui ont eu plus de 5000 personnes hospitalisées"
        elif message == 1:
            dataList.insert(0, ("Vaccinations", "Iso_code", "Pays",))
            messageToDisplay = "Le pays qui a administré le plus grand nombre de vaccins"
        elif message == 2:
            dataList.insert(0, ("L'id du vaccin", "Le vaccin", "La liste des pays qui l'utilise",))
            messageToDisplay = "Les vaccins avec la liste des pays qui l'utilisent"
        elif message == 3:
            dataList.insert(0, ("L'iso_code", "La proportion de la population hospitalisée, le 1/01/2021",))
            messageToDisplay = "La proportion de la population hospitalisée pour chaque pays le 1er janvier 2021"
        elif message == 4:
            dataList.insert(0, ("l'Iso_coode", "Date", "hospitalisations", "hospitalisattions"))
            messageToDisplay = "L'évolution pour chaque jour et chaque pays du nombre de patients hospitalisés"
        elif message == 5:
            dataList.insert(0, ("Vaccin", "Id du vaccin"))
            messageToDisplay = "Les vaccins disponibles à la fois en Belgique et en france."

        return render_template('execution.html', data=dataList, userEpi=epidemiologist, label=messageToDisplay)
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
                return render_template("modifyHospitalsData.html", userEpi=epidemiologist,
                                       label="Les données ont été insérer dans la base de données")
            else:
                return render_template("modifyHospitalsData.html", userEpi=epidemiologist,
                                       label="Il faut compléter tous les champs")
        else:
            return render_template("modifyHospitalsData.html", userEpi=epidemiologist)
    else:
        return redirect(url_for('login'))


if __name__ == '__main__':
    app.run()
