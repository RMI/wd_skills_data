# Purpose: Weekly import of worker administrative data from Workday

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


# Define the path for your data, including the new export and where you want backups to go
mydir  = Path("wd_skills_data/")

# Where you want the export to go after it's been processed
destinationpath = 'wd_skills_data/archive/'


dfs = []

# grab file and create dataframe
for file in mydir.glob('RMI Active*.xlsx'):
    data = pd.read_excel(file, header=[1])
   # data = pd.read_excel(file)
    dfs.append(data)

df = pd.concat(dfs, ignore_index=True)
df = pd.DataFrame(df)

# Move new excel file to archive
sourcefiles = os.listdir(mydir)
for file in sourcefiles:
    if file.startswith('RMI Active'):
        shutil.move(os.path.join(mydir,file), os.path.join(destinationpath,file))

# subset df and rename columns to match database
df_import = df[['Preferred Name', 'Company', 'Cost Center', 'Business Title', 'Email Address', 'Manager', 'Location']].rename(
    columns={ 'Preferred Name': 'worker', 'Company': 'company', 'Cost Center':'cost_center',
              'Business Title': 'job_title', 'Email Address':'email', 'Manager':'people_leader', 'Location':'location'})


# trim people leader to preferred name
df_import.loc[df_import['people_leader'].astype(str).str.contains('\|'), 'people_leader'] = df_import['people_leader'].astype(str).str.split("|").str[1]
df_import.loc[df_import['people_leader'].astype(str).str.contains('\|') == False, 'people_leader'] = df_import['people_leader'].astype(str)
df_import['people_leader'] = df_import['people_leader'].str.strip()

# Remove remote from location
df_import['location'] = df_import['location'].str.replace("Remote - ", "")
df_import['location'] = df_import['location'].str.replace("Remote-", "")


# Remove duplicates

database_username = 'rmiadmin'
database_password = rmi_db
database_ip       = rmi_db_ip
database_name     = 'rmi_skills'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))

# with database_connection.connect() as conn:
#     query_string = "select email from worker_details"
#     result = conn.execute(text(query_string))
#     df1 = pd.DataFrame(result.fetchall())
#     df1.columns = result.keys()

# # Remove staff who have left RMI
# df_import.set_index('email')
# df1.set_index('email')
# df_drop = df1.drop(df1[df1.email.isin(df_import['email'])].index.tolist())


# id_list = list(df_drop['email'])

# for i in id_list:
#     query_string = "delete from worker_details where email = " + "'"+ i + "'"
#     with database_connection.connect() as conn:
#         conn.execute(text(query_string))
#         conn.commit()



# filter import df to new staff
# df_import = df_import.drop(df_import[df_import.email.isin(df1['email'])].index.tolist())
# df_import.reset_index(inplace=True)
# df_import = df_import.drop({"index"}, axis= 1)


query_string = "delete from worker_details"
with database_connection.connect() as conn:
    conn.execute(text(query_string))
    conn.commit()

# import new data
df_import.to_sql(con=database_connection, name='worker_details', if_exists='append', index=False)


