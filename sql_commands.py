"""
Ce fichier contient les commandes SQL qui vont être lancées par l'application.

"""

project_requests = ["SELECT country.country FROM hospitalsdata JOIN country on hospitalsdata.iso_code=country.iso_code WHERE hosp_patients >= 5000 GROUP BY hospitalsdata.iso_code",
                    "SELECT sum(v.vaccinations_done), v.iso_code, c.country \
           FROM vaccinations v JOIN Country c on v.iso_code = c.iso_code \
           GROUP BY iso_code \
           ORDER BY SUM(v.vaccinations_done) DESC \
           LIMIT 1;",
                    "SELECT id as idVaccine, name, (SELECT GROUP_CONCAT(Country.country) \
         FROM Vaccine JOIN CountryVaccine on Vaccine.id = CountryVaccine.vaccine_id JOIN Country on CountryVaccine.iso_code = Country.iso_code \
                    WHERE vaccine.id = idVaccine) \
                    FROM Vaccine",
                    "SELECT Country.iso_code, hosp_patients / country.population \
                    FROM hospitalsdata JOIN Country on hospitalsdata.iso_code = Country.iso_Code \
                    WHERE \
                    date = '2021-01-01'",
                    "SELECT iso_code as country, date as currentDate, hosp_patients, hosp_patients-(select hosp_patients \
                    FROM HospitalsData \
                    WHERE date = subdate(currentDate, 1) and iso_code = country) \
                    FROM Hospitalsdata",
                    "Select DISTINCT Vaccine.name, Vaccine.id \
                    FROM Vaccine JOIN CountryVaccine on Vaccine.id = CountryVaccine.vaccine_id \
                    WHERE vaccine.id IN ( Select vaccine_id \
                    FROM CountryVaccine \
                    WHERE iso_Code = 'FRA') \
                    AND  vaccine.id IN ( Select vaccine_id \
                    FROM CountryVaccine \
                    WHERE iso_Code = 'BEL')"]

database_commands = ["SELECT * FROM country",
                     "SELECT * FROM vaccine",
                     "SELECT * FROM countryvaccine",
                     "SELECT * FROM vaccinations",
                     "SELECT * FROM hospitalsdata",
                     "SELECT * FROM climate"]

verifyEpidemiologist = "SELECT EXISTS(SELECT * FROM epidemiologist WHERE epidemiologist.id_person = ?);"

# "SELECT EXISTS (SELECT * FROM person WHERE person.username = ? AND person.password= ?);"

