import mysql.connector
import pandas as pd
from mysql.connector.constants import ClientFlag

config = {
            'user': 'root',
                'password': 'root',
                    'host': '35.242.147.116',
                        'client_flags': [ClientFlag.SSL]
                                    }

# now we establish our connection
cnxn = mysql.connector.connect(**config)

#cursor = cnxn.cursor()  # initialize connection cursor
#cursor.execute('CREATE DATABASE pricing_database')  # create a new 'testdb' database
#cnxn.close()  # close connection because we will be reconnecting to testdb
config['database'] = 'pricing_database'  # add new database to config dict
cnxn = mysql.connector.connect(**config)
cursor = cnxn.cursor()
'''
cursor.execute("CREATE TABLE space_missions ("
               "company_name VARCHAR(255),"
               "location VARCHAR(255),"
               "datum DATETIME,"
               "detail VARCHAR(255),"
               "status_rocket VARCHAR(255),"
               "rocket FLOAT(6,2),"
               "status_mission VARCHAR(255) )")

cnxn.commit()  # this commits changes to the database
# first we setup our query
# first we setup our query
'''
'''
query = ("INSERT INTO space_missions (company_name, location, datum, detail, status_rocket, rocket, status_mission) "
         "VALUES (%s, %s, %s, %s, %s, %s, %s)")
'''
# then we execute with every row in our dataframe
data=pd.read_csv("people.csv")
df = pd.DataFrame(data, columns= ['Name','Country','Age'])
# Create Table
#cursor.execute('CREATE TABLE people_info (Name nvarchar(50), Country nvarchar(50), Age int)')

# Insert DataFrame to Table
for row in df.itertuples():
    sql = "INSERT INTO pricing_database.people_info (Name,Country,Age) VALUES (%s, %s,%s)"
    val = (row.Name,row.Country,row.Age)
    cursor.execute(sql,val)
cnxn.commit()

cursor.execute("SELECT * FROM pricing_database.people_info")
out = cursor.fetchall()
for row in out:
    print(row)

