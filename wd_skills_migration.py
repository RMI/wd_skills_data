########### 

# purpose: merge skills, interests, and mentor skills into single table with skill_type column.



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



#Extract existing record uids and compare, removing duplicates
database_username = 'rmiadmin'
database_password = rmi_db
database_ip       = rmi_db_ip
database_name     = 'rmi_skills'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))

with database_connection.connect() as conn:
    result = conn.execute(text("select email, skill, skill_category, date_added, date_updated from worker_skills"))
    df1 = pd.DataFrame(result.fetchall())
    df1.columns = result.keys()

df_profile = df1
df_profile['skill_type'] = 'Profile'

with database_connection.connect() as conn:
    result = conn.execute(text("select email, interest, skill_category, date_added, date_updated from worker_skill_interest"))
    df1 = pd.DataFrame(result.fetchall())
    df1.columns = result.keys()

df_interest = df1.rename(columns={'interest':'skill'})
df_interest['skill_type'] = 'Interest'


with database_connection.connect() as conn:
    result = conn.execute(text("select email, mentor_skill, skill_category, date_added, date_updated from worker_skill_mentor"))
    df1 = pd.DataFrame(result.fetchall())
    df1.columns = result.keys()

df_mentor = df1.rename(columns={'mentor_skill':'skill'})
df_mentor['skill_type'] = 'Mentor'


df_import = pd.concat([df_profile, df_interest, df_mentor], ignore_index=True)


with database_connection.connect() as conn:
   query = text("delete from worker_skills_all")
   conn.execute(query)
   conn.commit()


df_import.to_sql(con=database_connection, name='worker_skills_all', if_exists='append', index=False)

# Remove skills for staff who have left RMI

with database_connection.connect() as conn:
    result = conn.execute(text("select distinct email from worker_skills_all"))
    df1_skills = pd.DataFrame(result.fetchall())
    df1_skills.columns = result.keys()

with database_connection.connect() as conn:
    result = conn.execute(text("select email from worker_details"))
    df_roster = pd.DataFrame(result.fetchall())
    df_roster.columns = result.keys()

# Remove staff who have left RMI
df1_skills.set_index('email')
df_roster.set_index('email')
df_drop = df1_skills.drop(df1_skills[df1_skills.email.isin(df_roster['email'])].index.tolist())

id_list = list(df_drop['email'])


if len(id_list) > 1:

    for i in id_list:
        query_string = "delete from worker_skills_all where email = " + "'"+ str(i) + "'"
        with database_connection.connect() as conn:
            conn.execute(text(query_string))
            conn.commit()

    for i in id_list:
        query_string = "delete from worker_skill_interest where email = " + "'"+ str(i) + "'"
        with database_connection.connect() as conn:
            conn.execute(text(query_string))
            conn.commit()

    for i in id_list:
        query_string = "delete from worker_skill_mentor where email = " + "'"+ str(i) + "'"
        with database_connection.connect() as conn:
            conn.execute(text(query_string))
            conn.commit()

else:
    query_string = "delete from worker_skills_all where email = " + "'"+ str(id_list) + "'"
    with database_connection.connect() as conn:
        conn.execute(text(query_string))
        conn.commit()

    for i in id_list:
        query_string = "delete from worker_skill_interest where email = " + "'"+ str(id_list) + "'"
        with database_connection.connect() as conn:
            conn.execute(text(query_string))
            conn.commit()

    for i in id_list:
        query_string = "delete from worker_skill_mentor where email = " + "'"+ str(id_list) + "'"
        with database_connection.connect() as conn:
            conn.execute(text(query_string))
            conn.commit()

