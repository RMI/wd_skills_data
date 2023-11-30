# Purpose: Weekly import of L&D course offering data from Workday

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


# Load environment file that contains database credentials
load_dotenv('cred.env')
rmi_db = os.getenv('DBASE_PWD')
rmi_db_ip = os.getenv('DBASE_IP')

# Load Reference skill category data
ref_skill = pd.read_excel('C:/Users/ghoffman/OneDrive - RMI/Skills and Experiences System/Workday_Maintained_Skills.xlsx', sheet_name='Skills')

# Define the path for your data, including the new export and where you want backups to go
mydir  = Path("wd_skills_data/")
# Same as above, just not applied to Path()
sourcepath='wd_skills_data/'
# Where you want the export to go after it's been processed
destinationpath = 'wd_skills_data/archive/'
# backup file pathway
backup = 'wd_skills_data/imports/import_learning_'+ str(date.today()) + '.xlsx'

#database connection details
database_username = 'rmiadmin'
database_password = rmi_db
database_ip       = rmi_db_ip
database_name     = 'rmi_skills'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))

dfs = []

# grab file and create dataframe
for file in mydir.glob('Export_Learning*.xlsx'):
    data = pd.read_excel(file, header=[2], skiprows=[0,3])
    dfs.append(data)

df = pd.concat(dfs, ignore_index=True)
df = pd.DataFrame(df)

# move file to archive
sourcefiles = os.listdir(sourcepath)
for file in sourcefiles:
    if file.startswith('Export_Learning Content'):
        shutil.move(os.path.join(sourcepath,file), os.path.join(destinationpath,file))

# select relevant fields
df2 = df[['Topic', 'Skills', 'Title']]

# remove LinkedIn Learning courses
df2 = df2[df2['Topic'] != 'LinkedIn Learning']

df2.rename(columns={'Title':'name',  'Topic':'topic','Skills':'skills'}, inplace=True)
df2.drop_duplicates(subset=['name'], inplace=True)

# split skills by return unicode (\n\n)
skills = df2['skills'].str.split('\\n\\n', expand=True)

# join course info and wide skills
df2 = df2[['name', 'topic']]
df_import = pd.concat([df2, skills], axis=1)

# transform skills to long format
df_import2 = pd.melt(df_import, id_vars= {'name', 'topic'}, value_vars= df_import[2:len(df_import.columns)], value_name='skill', var_name='skill_del')

# remove blank and null course names and skills
df_import2 = df_import2[df_import2['skill'] != '']
df_import2 = df_import2[df_import2['skill'].notnull()]
df_import2 = df_import2[df_import2['name'].notnull()]
df_import2 = df_import2[df_import2['name'] != '']
df_import = df_import2[['name', 'skill', 'topic']]

# Join in categories from reference
ref_skill.rename(columns={'Skill Category':'skill_category', 'Skill':'skill'}, inplace=True)
cols = ['skill']
df2 = df_import.join(ref_skill.set_index(cols), on=cols)
df2.drop(axis=1, columns = {"#", "International Equivalents"}, inplace=True)

# write out backup
df2.to_excel(backup)

# drop existing courses
with database_connection.connect() as conn:
    conn.execute(text("delete from learning"))
    conn.commit()


# import to database
df2.to_sql(con=database_connection, name='learning', if_exists='append', index=False)

