# Purpose: Import new Workday skills to MySQL every day.

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
raw_backup = 'wd_skills_data/imports/raw_'+ str(date.today()) + '.xlsx'
import_backup = 'wd_skills_data/imports/import_skills_'+ str(date.today()) + '.xlsx'

# Load Reference skill category data
ref_skill = pd.read_excel('C:/Users/ghoffman/OneDrive - RMI/Skills and Experiences System/Workday_Maintained_Skills.xlsx', sheet_name='Skills')

# Define the path for your data, including the new export and where you want backups to go
mydir = Path("wd_skills_data/")
# Same as above, just not applied to Path()
sourcepath='wd_skills_data/'
# Where you want the export to go after it's been processed
destinationpath = 'wd_skills_data/archive/'

################ BELOW THIS LINE SHOULDN'T NEED TO BE MODIFIED TO RUN SCRIPT #################
##############################################################################################

dfs = []

# df_base = pd.DataFrame(columns= ['Worker', 'Reference ID', 'Email - Work', 'Job Title', 'Supervisory Organization', 
                                #  'Cost Center', 'Skill Item', 'Category' 
                                #  ])

# Loop through data folder, appending any csv files to df
for file in mydir.glob('Worker Skills*.xlsx'):
    data = pd.read_excel(file, header=[2])
    dfs.append(data)

df = pd.concat(dfs, ignore_index=True)
df = pd.DataFrame(df)
df1 = df

#df1 = pd.concat([df_base, df], ignore_index=True)
#Move new csv to archive
sourcefiles = os.listdir(sourcepath)
for file in sourcefiles:
    if file.startswith('Worker Skills'):
        shutil.move(os.path.join(sourcepath,file), os.path.join(destinationpath,file))

# Update columns
df1.rename(columns={'Worker':'worker', 'Reference ID' : 'referenceid', 'Email - Work':'email',
            'Job Title': 'job_title', 'Supervisory Organization': 'supervisory', 
            'Cost Center':'cost_center', 'Skill Item': 'skill'}, 
            inplace=True)

# drop incorrect category variable
df_import = df1.drop(axis=1, columns="Category")

# trim worker to preferred name
df_import.loc[df_import['worker'].astype(str).str.contains('\|'), 'worker'] = df_import['worker'].astype(str).str.split("|").str[1]
df_import.loc[df_import['worker'].astype(str).str.contains('\|') == False, 'worker'] = df_import['worker'].astype(str)
df_import['worker'] = df_import['worker'].str.replace(" (On Leave)", "", regex=False)
df_import['worker'] = df_import['worker'].str.replace(" [C]", "", regex=False)
df_import['worker'] = df_import['worker'].str.strip()
df_import['supervisory'] = df_import['supervisory'].str.strip()


# trim supervisor to preferred name
df_import.loc[df_import['supervisory'].astype(str).str.contains('\|'), 'supervisory'] = df_import['supervisory'].astype(str).str.split("|").str[1]
df_import.loc[df_import['supervisory'].astype(str).str.contains('\|') == False, 'supervisory'] = df_import['supervisory'].astype(str)


# Join in categories from reference
ref_skill.rename(columns={'Skill Category':'skill_category', 'Skill':'skill'}, inplace=True)

cols = ['skill']
df2 = df_import.join(ref_skill.set_index(cols), on=cols)

df2.drop(axis=1, columns = {"#"})

# Export Excel backup
df2.to_excel(raw_backup)

df_import = df2

# Create uid for skill record (referenceid + skill)
df_import['skill'] = df_import['skill'].astype(str)
df_import['uid'] = df_import['email'].astype(str)

df_import['uid'] = df_import['uid'].str.cat(df_import['skill'], sep= "_")

 #Extract existing record uids and compare, removing duplicates
database_username = 'rmiadmin'
database_password = rmi_db
database_ip       = rmi_db_ip
database_name     = 'rmi_skills'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))

with database_connection.connect() as conn:
    result = conn.execute(text("select email, skill from worker_skills"))
    df1 = pd.DataFrame(result.fetchall())
    df1.columns = result.keys()

df1['skill'] = df1['skill'].astype(str)
df1['uid'] = df1['email'].astype(str)
df1['uid'] = df1['uid'].str.cat(df1['skill'], sep= "_")

df_import.set_index('uid')
df_import = df_import.drop(df_import[df_import.uid.isin(df1['uid'])].index.tolist())
df_import.reset_index(inplace=True)
df_import = df_import[['worker', 'email', 'job_title', 'supervisory',
       'cost_center', 'skill', 'skill_category']]

# Fill null ref ID
# df_import['referenceid'] = df_import['referenceid'].replace(np.nan, 00000)


# Export Excel backup
df_import.to_excel(import_backup)

# Import new records
df_import.to_sql(con=database_connection, name='worker_skills', if_exists='append', index=False)

