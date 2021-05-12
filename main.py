"""
COVIDATA - 13/05/2021

Authors : Ali, Elias, Ossama

SQL DDL and DML for creating a MySQL database.
"""


import numpy as np
import pymysql.cursors
import pandas as pd


class Parser:
    """
    Lit les données des fichiers csv et remplace les données non existantes par des None.
    """
    climate = ""
    country = ""
    hospitals = ""
    producers = ""
    vaccinations = ""
    merged_country = ""

    def __init__(self, files):
        self.climate = files + "/climate.csv"
        self.country = files + "/country.csv"
        self.hospitals = files + "/hospitals.csv"
        self.producers = files + "/producers.csv"
        self.vaccinations = files + "/vaccinations.csv"

    def get_climate_data(self):
        frame = pd.read_csv(self.climate, sep=";")
        frame = frame.replace({np.NAN: None})
        return frame.iterrows()

    def get_country_data(self):
        country = pd.read_csv(self.country, sep=";")
        producers = pd.read_csv(self.producers, sep=";")
        frame = pd.merge(country, producers[['iso_code', 'date']], on='iso_code', how='left')
        frame = frame.replace({np.NAN: None})
        return frame.iterrows()

    def get_hospitals_data(self):
        frame = pd.read_csv(self.hospitals, sep=",")
        frame = frame.replace({np.NAN: None})
        return frame.iterrows()

    def get_person_and_epidemiologist_data(self):
        frame = pd.read_csv(self.hospitals, sep=',')
        newFrame = frame["source_epidemiologiste"]
        newFrame = newFrame.drop_duplicates()
        return newFrame

    def get_producers_data(self):
        frame = pd.read_csv(self.producers, sep=";")
        frame = frame.replace({np.NAN: None})
        return frame.iterrows()

    def get_vaccinations_data(self):
        vaccinations = pd.read_csv(self.vaccinations, sep=",")
        vaccinations = vaccinations.replace({np.NAN: None})
        return vaccinations.iterrows()


class DataBase:
    """
    Créer la base de donnée coviddata et ses tables puis insère les données receuilli par Parser.

    Il y a 2 utilisateurs tests :
    user normal
    username : ali
    password : password

    et

    user épidemiologiste
    username : mohamed
    password : password
    """
    parser: Parser

    def __init__(self, data):
        connection = pymysql.connect(host='localhost', user='root', password='', \
                                     charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        connection.cursor().execute('CREATE database coviddata')
        connection.select_db('coviddata')
        self.createAllTables(connection)  # DDL - Data Definition Language
        self.parser = Parser(data)
        self.insertIntoTables(connection)  # DML - Data Manipulation Language

    def createAllTables(self, connection):
        self.createClimateTable(connection)
        self.createCountryTable(connection)
        self.createVaccineTable(connection)
        self.createCountryVaccineTable(connection)
        self.createVaccinationsTable(connection)
        self.createPersonTable(connection)
        self.createEpidemiologistTable(connection)
        self.createHospitalsTable(connection)
        print("Tables creation completed")

    def insertIntoTables(self, connection):
        self.insertIntoClimate(self.parser.get_climate_data(), connection)
        print("Data inserted into tables. (1/7)")
        self.insertIntoCountry(self.parser.get_country_data(), connection)
        print("Data inserted into tables. (2/7)")
        self.insertIntoVaccine(connection)
        print("Data inserted into tables. (3/7)")
        self.insertIntoVaccineCountry(self.parser.get_producers_data(), connection)
        print("Data inserted into tables. (4/7)")
        self.insertIntoPersonAndEpidemiologist(self.parser.get_person_and_epidemiologist_data(), connection)
        print("Data inserted into tables. (5/7)")
        self.insertIntoHospitals(self.parser.get_hospitals_data(), connection)
        print("Data inserted into tables. (6/7)")
        self.insertIntoVaccinations(self.parser.get_vaccinations_data(), connection)
        print("Data inserted into tables. (7/7)")
        self.insertDevUser(connection)
        print("Data insertion completed.")

    def createClimateTable(self, connection):
        with connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS climate (" \
                  "Id INT(2) unsigned primary key," \
                  "Description TEXT not null )"
            cursor.execute(sql)

    def createCountryTable(self, connection):
        with connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS country (Iso_code varchar(3) PRIMARY KEY," \
                  "Continent varchar(20) ," \
                  "Region varchar(50) ," \
                  "Country varchar(50) ," \
                  "Hdi double ," \
                  "Population int(11) unsigned ," \
                  "Area_sq_ml int(11) unsigned ," \
                  "Climate int(2) unsigned," \
                  "Date_first_vacciantion DATETIME," \
                  "FOREIGN KEY(climate) REFERENCES climate(id)) "
            cursor.execute(sql)

    def createVaccineTable(self, connection):
        with connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS Vaccine(Id INT(2) AUTO_INCREMENT PRIMARY KEY," \
                  "Name VARCHAR(30) NOT NULL UNIQUE" \
                  ")"
            cursor.execute(sql)

    def createCountryVaccineTable(self, connection):
        with connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS CountryVaccine(Iso_code VARCHAR(3) NOT NULL," \
                  "Vaccine_id int(2) NOT NULL," \
                  "PRIMARY KEY (Iso_code, Vaccine_id)," \
                  "FOREIGN KEY (Iso_code) REFERENCES Country(Iso_code)," \
                  "FOREIGN KEY (Vaccine_id) REFERENCES Vaccine(Id))"
            cursor.execute(sql)

    def createVaccinationsTable(self, connection):
        with connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS Vaccinations(Iso_code varchar(3) NOT NULL," \
                  " Date DATETIME NOT NULL," \
                  " Tests INT(10) unsigned," \
                  " Vaccinations_done INT(10) unsigned, " \
                  "PRIMARY KEY(Iso_code, Date)," \
                  "FOREIGN KEY (Iso_code) REFERENCES Country(Iso_code))"
            cursor.execute(sql)

    def createPersonTable(self, connection):
        with connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS Person(Id VARCHAR(40) PRIMARY KEY , " \
                  "First_name VARCHAR(20) , " \
                  "Last_name VARCHAR(20)," \
                  "Username VARCHAR(20) unique," \
                  "Address TEXT ," \
                  "Password VARCHAR(100) )"
            cursor.execute(sql)

    def createEpidemiologistTable(self, connection):
        with connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS Epidemiologist(Id_person VARCHAR(40) PRIMARY KEY," \
                  "Center VARCHAR(50) ," \
                  "Service_phone VARCHAR(20) ," \
                  "FOREIGN KEY (Id_person) REFERENCES Person(Id))"
            cursor.execute(sql)

    def createHospitalsTable(self, connection):
        with connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS HospitalsData(id INT AUTO_INCREMENT PRIMARY KEY ," \
                  "Iso_code varchar(3) NOT NULL," \
                  " Date DATE NOT NULL," \
                  " Icu_patients INT(10)  NOT NULL," \
                  " Hosp_patients INT(10)  NOT NULL, " \
                  " Source_epidemiologist VARCHAR(40) NOT NULL," \
                  "FOREIGN KEY (Iso_code) REFERENCES Country(Iso_code), " \
                  "FOREIGN KEY (Source_epidemiologist) REFERENCES Epidemiologist(Id_person))"
            cursor.execute(sql)

    # DDL
    ####################################################################################################################
    # DML

    def insertIntoClimate(self, data, connection):
        for index, elem in data:
            id, description = elem["id"], elem["decription"]

            with connection.cursor() as cursor:
                sql = "INSERT INTO climate (id, description) VALUES (%s, %s)"
                cursor.execute(sql, (id, description))
            connection.commit()

    def insertIntoCountry(self, data, connection):
        for index, elem in data:
            iso_code = elem["iso_code"]
            continent = elem["continent"]
            region = elem["region"]
            country = elem["country"]
            hdi = elem["hdi"]
            population = elem["population"]
            area_sq_ml = elem["area_sq_ml"]
            climate = elem["climate"]
            date_first_vacciantion = elem["date"]

            with connection.cursor() as cursor:
                sql = "INSERT INTO country (iso_code, \
                continent, \
                region, \
                country, \
                hdi, \
                population, \
                area_sq_ml, \
                climate, \
                date_first_vacciantion) VALUES (%s,%s,%s,%s,%s,%s,%s,%s, %s)"
                cursor.execute(sql, (iso_code, continent, region, country, hdi, population, \
                                     area_sq_ml, climate, date_first_vacciantion))
            connection.commit()

    def insertIntoVaccine(self, connection):
        vaccines = ["Pfizer/BioNTech", "Sinopharm", "Sputnik V", "Moderna", "Oxford/AstraZeneca", "Sinovac", "CNBG"]

        for vaccin in vaccines:
            with connection.cursor() as cursor:
                sql = "INSERT INTO vaccine (name) VALUES (%s)"
                cursor.execute(sql, (vaccin))
            connection.commit()

    def insertIntoVaccineCountry(self, data, connection):
        switcher = {
            "Pfizer/BioNTech": 1,
            "Sinopharm": 2,
            "Sputnik V": 3,
            "Moderna": 4,
            "Oxford/AstraZeneca": 5,
            "Sinovac": 6,
            "CNBG": 7
        }
        with connection.cursor() as cursor:
            for index, elem in data:
                vaccines = elem["vaccines"].split(",")
                for vaccine in vaccines:
                    sql = "INSERT INTO CountryVaccine (iso_code, vaccine_id) VALUES (%s, %s)"
                    try:
                        cursor.execute(sql, (elem["iso_code"], switcher.get(vaccine.lstrip(), "error")))
                    except:
                        # Si l'iso_code n'est pas contenu dans country ou autre raison.
                        continue

            connection.commit()

    def insertIntoVaccinations(self, data, connection):
        for index, elem in data:
            iso_code = elem["iso_code"]
            date = elem["date"]
            tests = elem["tests"]
            vaccinations_done = elem["vaccinations"]
            with connection.cursor() as cursor:
                try:
                    sql = "INSERT INTO vaccinations (iso_code, \
                      date, \
                      tests, \
                      vaccinations_done) VALUES (%s, %s, %s, %s)"
                    cursor.execute(sql, (iso_code, date, tests, vaccinations_done))
                except:
                    pass
            connection.commit()

    def insertIntoHospitals(self, data, connection):
        for index, elem in data:
            iso_code = elem["iso_code"]
            date = elem["date"]
            datesplit = date.split("/")
            date = datesplit[2] + "-" + datesplit[1] + "-" + datesplit[0]
            icu_patients = elem["icu_patients"]
            hosp_patients = elem["hosp_patients"]
            source_epidemiologist = elem["source_epidemiologiste"]

            with connection.cursor() as cursor:
                sql = "INSERT INTO hospitalsdata (iso_code, \
                   date, \
                   icu_patients, \
                   hosp_patients, \
                   source_epidemiologist) VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(sql, (iso_code, date, icu_patients, hosp_patients, source_epidemiologist))

            connection.commit()

    def insertIntoPersonAndEpidemiologist(self, data, connection):
        for elem in data:
            id = elem
            first_name = None
            last_name = None
            username = None
            address = None
            password = None

            center = None
            service_phone = None

            with connection.cursor() as cursor:
                sql = "INSERT INTO person (id, \
                   first_name, \
                   last_name, \
                   username, \
                   address, \
                   password) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (id, first_name, last_name, username, address, password))

                sql2 = "INSERT INTO epidemiologist (id_person," \
                       " center," \
                       " service_phone)" \
                       " VALUES (%s,%s,%s)"
                cursor.execute(sql2, (id, center, service_phone))
            connection.commit()

    def insertDevUser(self, connection):

        id = "dev_epidemiologist"
        id_lambda = "lambda"
        first_name = "mohamed"
        last_name = ""
        username = "mohamed"
        username2 = "ali"
        address = ""
        password = "pbkdf2:sha256:150000$Zd5n1R4i$657966d066a7d992d8b1ad7c480c151242e03667be654b4c6855029676c70df2"
        center = ""
        service_phone = ""

        with connection.cursor() as cursor:
            sql = "INSERT INTO person (id, \
               first_name, \
               last_name, \
               username, \
               address, \
               password) VALUES (%s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, (id, first_name, last_name, username, address, password))

            sql2 = "INSERT INTO epidemiologist (id_person," \
                   " center," \
                   " service_phone)" \
                   " VALUES (%s,%s,%s)"
            cursor.execute(sql2, (id, center, service_phone))
            sql3 = "INSERT INTO person (id, \
               first_name, \
               last_name, \
               username, \
               address, \
               password) VALUES (%s, %s, %s, %s, %s, %s)"
            cursor.execute(sql3, (id_lambda, first_name, last_name, username2, address, password))
        connection.commit()


if __name__ == '__main__':
    files_to_parse = "zip"  # sys.argv[1]
    db = DataBase(files_to_parse)
