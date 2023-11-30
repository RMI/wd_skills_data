import pandas as pd
from pathlib import Path
import sqlalchemy
from dotenv import load_dotenv
import os
import mysql.connector
from datetime import date, timezone, datetime
import shutil
import numpy as np

load_dotenv('cred.env')
rmi_db = os.getenv('DBASE_PWD')
rmi_db_ip = os.getenv('DBASE_IP')


database_username = 'rmiadmin'
database_password = rmi_db
database_ip       = rmi_db_ip
database_name     = 'rmi_skills'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))

backup = pd.read_csv('wd_skills_data/backup_09052023.csv')

df = backup

df_import = df

# Create uid for skill record (referenceid + skill)
df_import['skill'] = df_import['skill'].astype(str)
df_import['uid'] = df_import['email'].astype(str)
df_import['uid'] = df_import['uid'].str.cat(df_import['skill'], sep= "_")


with database_connection.connect() as conn:
    result = conn.execute("select email, skill from worker_skills")
    df1 = pd.DataFrame(result.fetchall())
    df1.columns = result.keys()

df1['skill'] = df1['skill'].astype(str)
df1['uid'] = df1['email'].astype(str)
df1['uid'] = df1['uid'].str.cat(df1['skill'], sep= "_")

df_import.set_index('uid')
df2 = df_import.drop(df_import[~(df_import.uid.isin(df1['uid']))].index.tolist())
df_import.reset_index(inplace=True)

print(len(df4))
print(df2.head())

df4 = df2.drop_duplicates(subset=['uid'])

df_import = df4

print(df_import.columns)

df_import = df_import[['worker', 'referenceid', 'email', 'job_title', 'supervisory',
       'cost_center', 'skill', 'skill_category', 'date_added', 'date_updated']]



with database_connection.connect() as conn:
    conn.execute("delete from worker_skills")

# Import new records
df_import.to_sql(con=database_connection, name='worker_skills', if_exists='append', index=False)
