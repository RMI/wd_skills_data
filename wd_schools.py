# Purpose: Import new Workday worker schools attended and fields of study to MySQL every week.

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


#################################################################
#########################  PARAMETERS ###########################

# Define Folder Locations
#archive = ''
# Load environment file that contains database credentials
load_dotenv('cred.env')
rmi_db = os.getenv('DBASE_PWD')
rmi_db_ip = os.getenv('DBASE_IP')

# Define database connection parameters
database_username = 'rmiadmin'
database_password = rmi_db
database_ip       = rmi_db_ip
database_name     = 'rmi_skills'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))
# Define File Names
raw_backup_school = 'wd_skills_data/imports/raw_schools_'+ str(date.today()) + '.xlsx'
raw_backup_fieldstudy = 'wd_skills_data/imports/raw_fieldstudy_'+ str(date.today()) + '.xlsx'

# Define the path for your data, including the new export and where you want backups to go
mydir = Path("wd_skills_data/")
# Same as above, just not applied to Path()
sourcepath='wd_skills_data/'
# Where you want the export to go after it's been processed
destinationpath = 'wd_skills_data/archive/'

################ BELOW THIS LINE SHOULDN'T NEED TO BE MODIFIED TO RUN SCRIPT #################
##############################################################################################

dfs = []

# Loop through data folder, appending any csv files to df
for file in mydir.glob('RMI - Employee Education *.xlsx'):
    data = pd.read_excel(file, header=[1])
    dfs.append(data)

df = pd.concat(dfs, ignore_index=True)
df = pd.DataFrame(df)

# Move new csv to archive
sourcefiles = os.listdir(sourcepath)
for file in sourcefiles:
    if file.startswith('RMI - Employee Education'):
        shutil.move(os.path.join(sourcepath,file), os.path.join(destinationpath,file))

# Update columns
df.rename(columns={'Preferred Name':'name', 'Education':'institution', 'Fields of Study':'field'}, 
            inplace=True)

study = df[['name', 'field']]
instit = df[['name', 'institution']]

cols = ['institution', 'field']

for i in cols:
    val = df[i].str.split('\\n\\n', expand=True)
    df2 = df[['name']]
    df3 = pd.concat([df2, val], axis=1)
    df4 = pd.melt(df3, id_vars= {'name'}, value_vars= df3[2:len(df3.columns)], value_name= i, var_name= 'field_del')
    df4 = df4[df4[i] != '']
    df4 = df4[df4[i].notnull()]
    df4 = df4[df4['name'].notnull()]
    df4 = df4[df4['name'] != '']
    df4 = df4[['name', i]]
    df4['uid'] = df4['name'].astype(str)
    df[i] = df4[i].astype(str)
    df4['uid'] = df4['uid'].str.cat(df4[i], sep= "_")
    globals()[i] = df4

df_field = field
df_institution = institution

# Export Excel backup
df_field.to_excel(raw_backup_fieldstudy)
df_institution.to_excel(raw_backup_school)


# Remove education institution records already in database
with database_connection.connect() as conn:
    result = conn.execute(text("select worker, institution from worker_schools"))
    df1 = pd.DataFrame(result.fetchall())
    df1.columns = result.keys()

df1['institution'] = df1['institution'].astype(str)
df1['uid'] = df1['worker'].astype(str)
df1['uid'] = df1['uid'].str.cat(df1['institution'], sep= "_")

df_institution.set_index('uid')
df_institution = df_institution.drop(df_institution[df_institution.uid.isin(df1['uid'])].index.tolist())
df_institution.reset_index(inplace=True)
df_institution = df_institution.drop({"index", "uid"}, axis= 1)

df_institution = df_institution[['name', 'institution']]
df_institution.rename(columns={'name':'worker'}, inplace=True)

df_institution.to_sql(con=database_connection, name='worker_schools', if_exists='append', index=False)


# Remove field of study records already in database
with database_connection.connect() as conn:
    result = conn.execute(text("select worker, field from worker_field_study"))
    df1 = pd.DataFrame(result.fetchall())
    df1.columns = result.keys()

df1['field'] = df1['field'].astype(str)
df1['uid'] = df1['worker'].astype(str)
df1['uid'] = df1['uid'].str.cat(df1['field'], sep= "_")

df_field.set_index('uid')
df_field = df_field.drop(df_field[df_field.uid.isin(df1['uid'])].index.tolist())
df_field.reset_index(inplace=True)
df_field = df_field.drop({"index", "uid"}, axis= 1)

df_field = df_field[['name', 'field']]
df_field.rename(columns={'name':'worker'}, inplace=True)

df_field.to_sql(con=database_connection, name='worker_field_study', if_exists='append', index=False)



