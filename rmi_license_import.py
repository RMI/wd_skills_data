
import pandas as pd
from pathlib import Path
import sqlalchemy
from dotenv import load_dotenv
import os
import mysql.connector
from datetime import date, timezone, datetime
import shutil
import numpy as np
from sqlalchemy import text


# Define Folder Locations
#archive = ''
# Load environment file that contains database credentials
load_dotenv('cred.env')
rmi_db = os.getenv('DBASE_PWD')
rmi_db_ip = os.getenv('DBASE_IP')

# Get Excel file
#sheets = pd.ExcelFile("C:\\Users\\ghoffman\\OneDrive - RMI\\KM Financial documents\\Subscriptions\\Combined User lists.xlsx")
#names = sheets.sheet_names
#names = [x.replace(' ','') for x in names]
#print(names)

#for i in names:
   # df = pd.read_excel('C:\\Users\\ghoffman\\OneDrive - RMI\\KM Financial documents\\Subscriptions\\Combined User lists.xlsx', sheet_name=i)
   # globals()[i] = df

df = pd.read_excel('C:\\Users\\ghoffman\\OneDrive - RMI\\KM Financial documents\\Subscriptions\\Combined User lists.xlsx', sheet_name='All Users All Subscriptions')

print(len(df))

df = df[['Name', 'Email', 'License']]

df.rename(columns={'Name':'worker', 'Email':'email', 'License':'license'}, inplace=True)

df = df[df['worker'].notnull()]
df = df[df['email'].notnull()]
df = df[df['license'].notnull()]

#Extract existing record uids and compare, removing duplicates
database_username = 'rmiadmin'
database_password = rmi_db
database_ip       = rmi_db_ip
database_name     = 'rmi_skills'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))

with database_connection.connect() as conn:
    conn.execute(text("delete from worker_license"))
    conn.commit()

df.to_sql(con = database_connection, name='worker_license', if_exists='append', index=False)

