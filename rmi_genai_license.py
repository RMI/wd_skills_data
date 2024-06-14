

# Goal of this script is to pull data from Monday.com API to get new GenAI license requests
# Currently using admin token from Kevin, will need to obtain dedicated KM token in the future

import requests
import pandas as pd
import os
from dotenv import load_dotenv
import sqlalchemy
from sqlalchemy import text
from time import sleep


load_dotenv('cred.env')
monday_token = os.getenv('MONDAY_TOKEN')
rmi_db_ip = os.getenv('DBASE_IP')
rmi_db = os.getenv('DBASE_PWD')

# database connection
database_username = 'rmiadmin'
database_password = rmi_db
database_ip       = rmi_db_ip
database_name     = 'rmi_skills'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))

def get_monday_call(x):
    headers = {'Authorization': monday_token}
    response = requests.post('https://api.monday.com/v2', json={'query': x}, headers=headers)
    return response.json()


# M365 Copilot licenses assigned
query = "query { items_page_by_column_values (limit: 500, board_id: 6112808324, columns: [{column_id: \"3_onboarding_status\", column_values: [\"Complete and emailed\"]}, {column_id: \"single_select\", column_values: [\"Copilot for M365\"]}]) { cursor items {  name }}}"
res = get_monday_call(query)
# print(res.keys())
# print(res['errors'])
# Get list of license holders
copilot = pd.json_normalize(res['data']['items_page_by_column_values']['items'])
copilot = pd.DataFrame(copilot)
copilot['platform'] = 'Copilot for M365'

# sleep 5 seconds to avoid rate limiting
sleep(5)

query = "query { items_page_by_column_values (limit: 500, board_id: 6112808324, columns: [{column_id: \"3_onboarding_status\", column_values: [\"Complete and emailed\"]}, {column_id: \"single_select\", column_values: [\"ChatGPT Team\"]}]) { cursor items {  name }}}"
res = get_monday_call(query)
chatgpt = pd.json_normalize(res['data']['items_page_by_column_values']['items'])
chatgpt = pd.DataFrame(chatgpt)
chatgpt['platform'] = 'ChatGPT Team'

# sleep 5 seconds to avoid rate limiting
sleep(5)

query = "query { items_page_by_column_values (limit: 500, board_id: 6112808324, columns: [{column_id: \"3_onboarding_status\", column_values: [\"Complete and emailed\"]}, {column_id: \"single_select\", column_values: [\"GitHub Copilot Business\"]}]) { cursor items {  name }}}"
res = get_monday_call(query)
github = pd.json_normalize(res['data']['items_page_by_column_values']['items'])
github = pd.DataFrame(github)
github['platform'] = 'GitHub Copilot Business'

# Combine all license holders
licenses = pd.concat([copilot, chatgpt, github])

# get email from database
with database_connection.connect() as conn:
    query = text("select worker, email from worker_details")
    emails = conn.execute(query)
    emails = emails.fetchall()

emails = pd.DataFrame(emails)

licenses['name_match'] = licenses['name'].str.lower()
emails['worker_match'] = emails['worker'].str.lower()

# merge emails with licenses, but only keep the records that match
df = pd.merge(emails, licenses, how='right', left_on='worker_match', right_on='name_match')


df.rename(columns={'platform':'license'}, inplace=True)

df = df[['worker', 'email', 'license']]

# Chunk to delete existing records, but don't need it now because it's redundant of the rmi_license_import.py script
# license_map = ['Copilot for M365', 'ChatGPT Team', 'GitHub Copilot Business']

# for i in license_map:

#     with database_connection.connect() as conn:
#         conn.execute(text("delete from worker_license where license = :license"), license=i)
#         conn.commit()

df.to_sql(con = database_connection, name='worker_license', if_exists='append', index=False)


# print out number imported to console
print("GenAI license import complete: " + str(len(df)) + " records imported")


# Will need this section to pull more than just names from the Monday.com API. Currently provides weirdly formatted data.

# query = "query { boards (ids: 6112808324) { items_page(limit: 500) { cursor items { id name column_values{ id value text } } } } } "
# res = get_monday_call(query)
# print(res['data']['boards'][0])
# print(res['data']['boards']['items_page']['items'])
# test = pd.json_normalize(res['data']['boards'])
# test['items_page.items'] = test['items_page.items'].apply(eval)

# # Normalize the column with json_normalize
# df_normalized = pd.json_normalize(test['items_page.items'])
