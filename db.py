import mysql.connector
import pandas as pd
from mysql.connector.constants import ClientFlag

config = {
            'user': 'root',
                'password': 'root',
                    'host': '34.89.51.179',
                        'client_flags': [ClientFlag.SSL]
                                    }

try:
    # now we establish our connection
    cnxn = mysql.connector.connect(**config)
    cursor = cnxn.cursor()
    #cursor.execute("SHOW DATABASES")
    #cursor = cnxn.cursor()  # initialize connection cursor
    cursor.execute('CREATE DATABASE IF NOT EXISTS pricing_database')  # create a new 'testdb' database
    cnxn.close()  # close connection because we will be reconnecting to testdb
except:
    print("1 error connecting to database")

config['database'] = 'pricing_database'  # add new database to config dict
try:

    cnxn = mysql.connector.connect(**config)
    cursor=cnxn.cursor()
except:
    print("2 error connecting to dataset ")

# then we execute with every row in our dataframe
data=pd.read_csv("people.csv")
df = pd.DataFrame(data, columns= ['Name','Country','Age'])
# Create Table
cursor.execute('CREATE TABLE IF NOT EXISTS people_info (Name nvarchar(50), Country nvarchar(50), Age int)')

# Insert DataFrame to Table
for row in df.itertuples():
    sql = "INSERT INTO pricing_database.people_info (Name,Country,Age) VALUES (%s, %s,%s)"
    val = (row.Name,row.Country,row.Age)
    cursor.execute(sql,val)
cnxn.commit()
print("rows appended to table successfully")

cursor.execute("SELECT * FROM pricing_database.people_info")
out = cursor.fetchall()
for row in out:
    print(row)

