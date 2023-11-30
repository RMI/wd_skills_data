
import pandas as pd
from pathlib import Path
import sqlalchemy
from dotenv import load_dotenv
import os
import mysql.connector
from datetime import date, timezone, datetime
import shutil
import numpy as np


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
    result = conn.execute("delete  from workday_data")
