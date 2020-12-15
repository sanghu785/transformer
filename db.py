import mysql.connector
import pandas as pd
from mysql.connector.constants import ClientFlag

config = {
            'user': 'root',
                'password': 'root',
                    'host': '34.89.51.179',
                        'client_flags': [ClientFlag.SSL]
                                    }

def create_dataset_if_not_exists():
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

def create_tables_if_not_exists():
	try:
		cnxn = mysql.connector.connect(**config)
    		cursor=cnxn.cursor()
		# Create Table
		cursor.execute('CREATE TABLE IF NOT EXISTS customer (cust_id nvarchar(50), glob_cust_id nvarchar(50), cust_name nvarchar(50))')

    		cursor=cnxn.cursor()
		# Create Table
		cursor.execute('CREATE TABLE IF NOT EXISTS account (acct_id nvarchar(50), acct_num int(50), cust_id nvarchar(50))')

    		cursor=cnxn.cursor()
		# Create Table
		cursor.execute('CREATE TABLE IF NOT EXISTS product_pricing (prdt_id nvarchar(50), prdt_cd nvarchar(50), prdt_desc nvarchar	(50),prdt_group nvarchar(50),prdt_ctgry nvarchar(50),pricing_ccy nvarchar(50),price int(50))')

    		cursor=cnxn.cursor()
		# Create Table
		cursor.execute('CREATE TABLE IF NOT EXISTS cust_prdt_pric_rel (cust_prdt_pric_rel_id nvarchar(50), cust_id nvarchar(50),prdt_id nvarchar(50))')
		cnxn.close()
	except:
    		print("2 error while creating tables")

def process_csv():
	cnxn = mysql.connector.connect(**config)
    	cursor=cnxn.cursor()
	
	# then we execute with every row in our dataframe
	data=pd.read_csv("people.csv")
	df = pd.DataFrame(data, columns= ['Name','Country','Age'])

	# Insert DataFrame to Table
	for row in df.itertuples():
    		sql = "INSERT INTO pricing_database.people_info (Name,Country,Age) VALUES (%s, %s,%s)"
    		val = (row.Name,row.Country,row.Age)
    		cursor.execute(sql,val)
	cnxn.commit()
	print("rows appended to table successfully")
	cnxn.close()
'''
def print_table_info(table_name):
	cnxn = mysql.connector.connect(**config)
    	cursor=cnxn.cursor()
	cursor.execute("SELECT * FROM pricing_database.people_info")
	out = cursor.fetchall()
	for row in out:
	    print(row)
'''
if __name__=='__main__':
	create_dataset_if_not_exists()
	create_tables_if_not_exists()
