# Purpose: Import new Workday skill interests to MySQL every week.

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
# Define File Names
raw_backup = 'wd_skills_data/imports/raw_language_'+ str(date.today()) + '.xlsx'
import_backup = 'wd_skills_data/imports/import_language_'+ str(date.today()) + '.xlsx'


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
for file in mydir.glob('RMI - Languages *.xlsx'):
    data = pd.read_excel(file, header=[1])
    dfs.append(data)

df = pd.concat(dfs, ignore_index=True)
df = pd.DataFrame(df)

# Move new csv to archive
sourcefiles = os.listdir(sourcepath)
for file in sourcefiles:
    if file.startswith('RMI - Languages'):
        shutil.move(os.path.join(sourcepath,file), os.path.join(destinationpath,file))

# Update columns
df.rename(columns={'Email':'email','Preferred Name':'worker', 'Languages':'language'}, 
            inplace=True)


# split skills by return unicode (\n\n)
languages = df['language'].str.split('\\n\\n', expand=True)

# join course info and wide skills
df = df[['email']]
df2 = pd.concat([df, languages], axis=1)

# transform skills to long format
df_import = pd.melt(df2, id_vars= {'email'}, value_vars= df2[2:len(df2.columns)], value_name='language', var_name='skill_del')

# remove blank and null course names and skills
df_import = df_import[df_import['language'] != '']
df_import = df_import[df_import['language'].notnull()]
df_import = df_import[df_import['email'].notnull()]
df_import = df_import[df_import['email'] != '']
df_import = df_import[['email', 'language']]



#Extract existing record uids and compare, removing duplicates
database_username = 'rmiadmin'
database_password = rmi_db
database_ip       = rmi_db_ip
database_name     = 'rmi_skills'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))

with database_connection.connect() as conn:
    conn.execute(text("delete from worker_language"))
    conn.commit()


# Export Excel backup
df_import.to_excel(import_backup)

# Import new records
df_import.to_sql(con=database_connection, name='worker_language', if_exists='append', index=False)

