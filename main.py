import numpy as np
import pymysql.cursors
import pandas as pd


class Parser:
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
        newFrame= frame["source_epidemiologiste"]
        newFrame = newFrame.drop_duplicates()
        return newFrame

    def get_producers_data(self):
         frame = pd.read_csv(self.producers, sep=";")
         frame.fillna('')
         return frame.iterrows()

    def get_vaccinations_data(self):
        vaccinations = pd.read_csv(self.vaccinations, sep=",")
        vaccinations = vaccinations.replace({np.NAN: None})
        return vaccinations.iterrows()



class DataBase:
    parser: Parser

    def __init__(self, data):
        connection = pymysql.connect(host='localhost', user='root', password='', database='coviddata', \
                                     charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        self.createAllTables(connection)

        self.parser = Parser(data)
        self.insertIntoTables(connection)


    def createAllTables(self, connection):
        self.createClimateTable(connection)
        self.createCountryTable(connection)
        self.createVaccineTable(connection)
        self.createCountryVaccineTable(connection)
        self.createVaccinationsTable(connection)
        self.createPersonTable(connection)
        self.createEpidemiologistTable(connection)
        self.createHospitalsTable(connection)

    def insertIntoTables(self, connection):
        self.insertIntoClimate(self.parser.get_climate_data(), connection)
        self.insertIntoCountry(self.parser.get_country_data(), connection)
        self.insertIntoVaccine(connection)
        self.insertIntoVaccineCountry(self.parser.get_producers_data(), connection)
        self.insertIntoPersonAndEpidemiologist(self.parser.get_person_and_epidemiologist_data(), connection)
        self.insertIntoHospitals(self.parser.get_hospitals_data(), connection)
        self.insertIntoVaccinations(self.parser.get_vaccinations_data(), connection)  # Ne fonctionne pas erreur :
        # IntegrityError: (1452, 'Cannot add or update a child row: a foreign key constraint fails (`coviddata`.`vaccinations`, CONSTRAINT `vaccinations_ibfk_1` FOREIGN KEY (`iso_code`) REFERENCES `country` (`iso_code`))')

    def createClimateTable(self, connection):
        with connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS climate (" \
                  "id INT(2) unsigned primary key," \
                  "description TEXT not null )"
            cursor.execute(sql)



    def createCountryTable(self, connection):
        with connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS country (iso_code varchar(3) PRIMARY KEY," \
                  "continent varchar(20) NOT NULL," \
                  "region varchar(50) NOT NULL," \
                  "country varchar(50) NOT NULL," \
                  "hdi double NOT NULL," \
                  "population int(11) unsigned NOT NULL," \
                  "area_sq_ml int(11) unsigned NOT NULL," \
                  "climate int(2) unsigned," \
                  "date_first_vacciantion DATETIME," \
                  "FOREIGN KEY(climate) REFERENCES climate(id)) "
            cursor.execute(sql)


    def createVaccineTable(self, connection):
        with connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS Vaccine(id INT(2) AUTO_INCREMENT PRIMARY KEY," \
                  "name VARCHAR(30) NOT NULL UNIQUE" \
                  ")"
            cursor.execute(sql)

    def createCountryVaccineTable(self, connection):
        with connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS CountryVaccine(iso_code VARCHAR(3) NOT NULL," \
                  "vaccine_id int(2) NOT NULL," \
                  "PRIMARY KEY (iso_code, vaccine_id)," \
                  "FOREIGN KEY (iso_code) REFERENCES Country(iso_code)," \
                  "FOREIGN KEY (vaccine_id) REFERENCES vaccine(id))"
            cursor.execute(sql)

    def createVaccinationsTable(self, connection):
        with connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS vaccinations(iso_code varchar(3) NOT NULL," \
                  " date DATETIME NOT NULL," \
                  " tests INT(10) unsigned," \
                  " vaccinations_done INT(10) unsigned, " \
                  "PRIMARY KEY(iso_code, date)," \
                  "FOREIGN KEY (iso_code) REFERENCES country(iso_code))"
            cursor.execute(sql)


    def createPersonTable(self, connection):
        with connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS person(id VARCHAR(40) PRIMARY KEY , " \
                  "first_name VARCHAR(20) NOT NULL, " \
                  "last_name VARCHAR(20) NOT NULL," \
                  "username VARCHAR(20) NOT NULL," \
                  "address TEXT NOT NULL," \
                  "password VARCHAR(20) NOT NULL)"
            cursor.execute(sql)

    def createEpidemiologistTable(self, connection):
        with connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS epidemiologist(id_person VARCHAR(40) PRIMARY KEY," \
                  "center VARCHAR(50) NOT NULL," \
                  "service_phone VARCHAR(20) NOT NULL," \
                  "FOREIGN KEY (id_person) REFERENCES person(id))"
            cursor.execute(sql)

    def createHospitalsTable(self, connection):
        with connection.cursor() as cursor:
            sql = "CREATE TABLE IF NOT EXISTS hospitalsData(id INT AUTO_INCREMENT PRIMARY KEY ," \
                  "iso_code varchar(3) NOT NULL," \
                  " date DATE NOT NULL," \
                  " icu_patients INT(10)  unsigned NOT NULL," \
                  " hosp_patients INT(10) unsigned NOT NULL, " \
                  " source_epidemiologist VARCHAR(40) NOT NULL," \
                  "FOREIGN KEY (iso_code) REFERENCES country(iso_code), " \
                  "FOREIGN KEY (source_epidemiologist) REFERENCES epidemiologist(id_person))"
            cursor.execute(sql)


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

        for vaccin in vaccines :
            with connection.cursor() as cursor:
                sql = "INSERT INTO vaccine (name) VALUES (%s)"
                cursor.execute(sql, (vaccin))
            connection.commit()

    def insertIntoVaccineCountry(self, data, connection):
        switcher = {
            "Pfizer/BioNTech" : 1,
            "Sinopharm" : 2,
            "Sputnik V" : 3 ,
            "Moderna" : 4,
            "Oxford/AstraZeneca" : 5,
            "Sinovac" : 6,
            "CNBG" : 7
        }
        with connection.cursor() as cursor:
            for index, elem in data:
                vaccines = elem["vaccines"].split(",")
                for vaccine in vaccines:
                    sql = "INSERT INTO CountryVaccine (iso_code, vaccine_id) VALUES (%s, %s)"
                    try:
                        cursor.execute(sql,(elem["iso_code"], switcher.get(vaccine.lstrip(), "error")))
                    except:
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
            first_name = "first_name"
            last_name = "last_name"
            username = "username"
            address = "address"
            password = "password"

            center = "center"
            service_phone = "service_phone"

            with connection.cursor() as cursor:
                sql = "INSERT INTO person (id, \
                   first_name, \
                   last_name, \
                   username, \
                   address, \
                   password) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (id, first_name, last_name, username, address, password))

                sql2="INSERT INTO epidemiologist (id_person," \
                     " center," \
                     " service_phone)" \
                     " VALUES (%s,%s,%s)"
                cursor.execute(sql2,(id, center, service_phone))
            connection.commit()

if __name__ == '__main__':
    files_to_parse = "zip"  # sys.argv[1]

    db = DataBase(files_to_parse)
    # db.get_country_data()

    #testParser = Parser(files_to_parse)
    #testParser.get_hospitals_data()

