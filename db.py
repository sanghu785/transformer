#!/usr/bin/python
# -*- coding: utf-8 -*-

import mysql.connector
import pandas as pd
import datetime
import random
import json
from mysql.connector.constants import ClientFlag

config = {
    'user': 'root',
    'password': 'root',
    'host': '34.105.221.119',
    'client_flags': [ClientFlag.SSL],
    }


def create_dataset_if_not_exists():
    try:

        # now we establish our connection

        cnxn = mysql.connector.connect(**config)
        cursor = cnxn.cursor()

        # cursor.execute("SHOW DATABASES")
        # cursor = cnxn.cursor()  # initialize connection cursor

        cursor.execute('CREATE DATABASE IF NOT EXISTS pricing_database')  # create a new 'testdb' database
        cnxn.close()  # close connection because we will be reconnecting to testdb
    except:
        print('1 error connecting to database')

    config['database'] = 'pricing_database'  # add new database to config dict


def create_tables_if_not_exists():
    try:
        cnxn = mysql.connector.connect(**config)
        cursor = cnxn.cursor()

        # Create Table

        cursor.execute('CREATE TABLE IF NOT EXISTS customer (cust_id int not null,glob_cust_id varchar(50),cust_name varchar(50),PRIMARY KEY (cust_id))')

        cursor = cnxn.cursor()

        # Create Table

        cursor.execute('CREATE TABLE IF NOT EXISTS account (acct_id int not null,acct_num varchar(50),cust_id int,PRIMARY KEY (acct_id),FOREIGN KEY (cust_id) REFERENCES customer(cust_id))')

        cursor = cnxn.cursor()

        # Create Table

        cursor.execute('CREATE TABLE IF NOT EXISTS product_pricing (prdt_id int not null,prdt_cd varchar(50),prdt_desc varchar(50),prdt_group varchar(50),prdt_ctgry varchar(50),pricing_ccy varchar(50),pricing_typ varchar(50),price DECIMAL(10,2),PRIMARY KEY (prdt_id))'
                       )

        cursor = cnxn.cursor()

        # Create Table

        cursor.execute('CREATE TABLE IF NOT EXISTS cust_prdt_pric_rel (cust_prdt_pric_rel_id int not null,cust_id int,prdt_id int,FOREIGN KEY (cust_id) REFERENCES customer(cust_id),FOREIGN KEY (prdt_id) REFERENCES product_pricing(prdt_id))')
        #cnxn.close()

        cursor = cnxn.cursor()

        # Create Table

        cursor.execute('CREATE TABLE IF NOT EXISTS acct_prdt_pric_rel (acct_prdt_pric_rel_id int not null,acct_id int,prdt_id int,FOREIGN KEY (acct_id) REFERENCES account(acct_id),FOREIGN KEY (prdt_id) REFERENCES product_pricing(prdt_id))')
        cnxn.close()

    except:
        print('2 error while creating tables')


def process_csv():
    cnxn = mysql.connector.connect(**config)
    cursor = cnxn.cursor()

    # then we execute with every row in our dataframe

    data = pd.read_csv("gs://pricing-bucket-123/pricing.csv")
    df = pd.DataFrame(data, columns=['Name', 'Country', 'Age'])

    # Insert DataFrame to Table

    for row in df.itertuples():
        sql = \
            'INSERT INTO pricing_database.people_info (Name,Country,Age) VALUES (%s, %s,%s)'
        val = (row.Name, row.Country, row.Age)
        cursor.execute(sql, val)
    cnxn.commit()
    print ('rows appended to table successfully')
    cnxn.close()


def process_json():
    cnxn = mysql.connector.connect(**config)
    cursor = cnxn.cursor()
    with open('data.json') as json_file:
        data = json.load(json_file)
    for actual_data in data:
        #cust_id=datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
        cust_id=random.randint(1000000,9999999)
        sql = 'INSERT INTO pricing_database.customer (cust_id,glob_cust_id,cust_name) VALUES (%s, %s,%s)'
        val = (cust_id, actual_data['globCustId'],"dummy-name")
        cursor.execute(sql, val)
        cnxn.commit()
############################################################################################################
        for i in range(0,2):
            #acc=datetime.datetime.now().strftime('d%H%M%S%f')
            acct_id = random.randint(100000,999999)
            sql ='INSERT INTO pricing_database.account (acct_id,acct_num,cust_id) VALUES (%s, %s,%s)'
            val = (acct_id, actual_data['acctNumber'],cust_id)
            cursor.execute(sql, val)
            cnxn.commit()
##########################################################################################################
            rand_num=random.randint(1000000,9999999)
            for j in range(0,10):
                prdt_id=j+rand_num
                sql ='INSERT INTO pricing_database.product_pricing(prdt_id,prdt_cd,prdt_desc,prdt_group,prdt_ctgry,pricing_ccy,pricing_typ,price) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
                val = (prdt_id,actual_data['prdctCd'],actual_data['prdctDesc'],actual_data['prdctGrp'],actual_data['prdctCtgry'],actual_data['pricingCcy'],actual_data['pricingType'],actual_data['price'],)
                cursor.execute(sql, val)
                cnxn.commit()
################################################################################################################3
                #acc_rel_id=datetime.datetime.now().strftime('%m%d%H%M%S%f')
                acc_rel_id=random.randint(11111,99999)
                sql = 'INSERT INTO pricing_database.acct_prdt_pric_rel (acct_prdt_pric_rel_id,acct_id,prdt_id) VALUES (%s, %s,%s)'
                val = (acc_rel_id, acct_id,prdt_id)
               # cursor.execute(sql, val)
######################################i##########################################################################
                #cus_rel_id=datetime.datetime.now().strftime('%m%d%H%M%S%f')
                cus_rel_id=random.randint(10000000,99999999)
                sql ='INSERT INTO pricing_database.cust_prdt_pric_rel (cust_prdt_pric_rel_id,cust_id,prdt_id) VALUES (%s, %s,%s)'
                val = (cus_rel_id, cust_id,prdt_id)
                cursor.execute(sql, val)
    cnxn.commit()


if __name__ == '__main__':
    create_dataset_if_not_exists()
    create_tables_if_not_exists()
    process_json()
