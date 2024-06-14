

# Goal of this script is to pull data from Monday.com API to get new GenAI license requests
# Current blocker is that the workspace is not accessible to the API token.
# Will need to either have PS workspace opened up to the API token or have a new workspace created for this purpose.

# import requests
# import pandas as pd
# import json
# from pandas.io.json import json_normalize
# import os
# from dotenv import load_dotenv

# load_dotenv('cred.env')
# monday_token = os.getenv('MONDAY_TOKEN')
# rmi_db_ip = os.getenv('DBASE_IP')


# def get_monday_call(x):
#     headers = {'Authorization': monday_token}
#     response = requests.post('https://api.monday.com/v2', json={'query': x}, headers=headers)
#     return response.json()


# query = "query { boards (ids: 6168474309) { items { id name column_values{ id value text } } } } "

# query = "query { boards (ids: 2208962537){ groups {title  } items_page() {  items { id name  }  }  } } "

# # Workday training
# query = "query { boards (ids: 4792947131){ name id}} "

# query = "query { boards (ids: 4792947131) { items_page { id name column_values{ id value text } } } } "

# query = "query { boards (ids: 4792947131){ groups {title  } items_page() {  items { id name  }  }  } } "



# query = "query { items_page_by_column_values (limit: 500, board_id: 4792947131, columns: [{column_id: \"3_onboarding_status\", column_values: [\"Complete and emailed\"]}]) { cursor items {  name }}}"

# query <- "query { boards (ids: 6112808324) { items_page(limit: 500) { cursor items { id name column_values{ id value text } } } } } "

# query = "query { items_page_by_column_values (limit: 500, board_id: 6112808324, columns: [{column_id: \"3_onboarding_status\", column_values: [\"Complete and emailed\"]}]) { cursor items {  name }}}"

# query = "query { boards (ids: 6112808324){ name id}} "

# Goal of this script is to pull data from Monday.com API to get published knowledge cards
# Currently using admin token from Kevin, will need to obtain dedicated KM token in the future

import requests
import pandas as pd
import os
from dotenv import load_dotenv
import sqlalchemy
from sqlalchemy import text
from time import sleep
from random import randint

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



# Initial query to get first 25 items
#query = "query {boards (ids: 4792947131){items_page (limit: 25, query_params:{rules:{column_id: \"date4\", compare_value: [\"\"], operator:is_not_empty}}) {cursor items { name column_values(ids: [\"name\",\"long_text05\", \"status\", \"link\",\"date4\"]){column {title} value } }} }}"

query = "query {boards (ids: 4792947131){items_page (limit: 25) {cursor items { name column_values(ids: [\"name\",\"text\", \"text3\", \"text6\",\"text65\",\"text8\",\"text83\",\"status\",\"text5\"]){column {title} value } }} }}"

res = [get_monday_call(query)]

print(res)
print(res[0].keys())

print(res['data']['boards'][0].keys())

print(res[0]['data']['boards'][0].keys())


# Get cursor for next page
cursor = res[0]['data']['boards'][0]['items_page']['cursor']
cursor_new = "\"" + cursor + "\""

# page through remaining values until cursor is None
while cursor_new is not None:
    query = "query {boards (ids: 4792947131){items_page (limit: 25, cursor: " + cursor_new + ") {cursor items { name column_values(ids: [\"name\",\"text\", \"text3\", \"text6\",\"text65\",\"text8\",\"text83\",\"status\",\"text5\"]){column {title} value } }} }}"
    res_next = get_monday_call(query)
    res.append(res_next)
    cursor = res_next['data']['boards'][0]['items_page']['cursor']
    cursor_new = "\"" + cursor + "\""
    print(cursor_new)
    sleep(randint(1, 3))

# create dataframe with columns from monday board
columns = []
for i in range(0,8):
    column = res[0]['data']['boards'][0]['items_page']['items'][1]['column_values'][i]['column']['title']
    columns.append(column)

df = pd.DataFrame(columns= columns)
df['Name'] = ''

# for each list item in res, get the items and append to dataframe
for i in range(len(res)):
# 0 is status
# 1 is link
# 2 is date
# 3 is description
# Get column names
    items = res[i]['data']['boards'][0]['items_page']['items']
# Get values and concat to dataframe
    for t in range(len(items)):
        status = res[i]['data']['boards'][0]['items_page']['items'][t]['column_values'][0]['value']
        link = res[i]['data']['boards'][0]['items_page']['items'][t]['column_values'][1]['value']
        date = res[i]['data']['boards'][0]['items_page']['items'][t]['column_values'][2]['value']
        description = res[i]['data']['boards'][0]['items_page']['items'][t]['column_values'][3]['value']
        name = items[t]['name']
        df = df.append({'Name': name,'Status': status, 'Link to page': link, 'Date Published': date, 'Brief Card Description': description}, ignore_index=True)


# Define a regular expression for URLs
url_pattern = r'(http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)'
# Extract URLs from the 'Link to page' column
df['url'] = df['Link to page'].str.extract(url_pattern, expand=False)

# extract Date from Date Published
date_pattern = r'(\d{4}-\d{2}-\d{2})'
df['pubDate'] = df['Date Published'].str.extract(date_pattern, expand=False)

# Extract text from the 'Brief Card Description' column
df['description'] = df['Brief Card Description'].str.extract(r'([A-Za-z].*)', expand=False)

# Drop unneeded columns and rename
df = df.drop(columns=['Link to page', 'Date Published', 'Status', 'Brief Card Description'])
df.rename(columns={'Name': 'title'}, inplace=True)

# drop null URL
df = df.dropna(subset=['url'])

# Delete existing data in the database
with database_connection.connect() as conn:
    conn.execute(text("DELETE FROM know_cards"))

# Write the dataframe to the database
df.to_sql('know_cards', con=database_connection, if_exists='append', index=False)

# # Status
# print(res['data']['boards'][0]['items_page']['items'][]['column_values'][0]['value'])
# # # Link
# print(res['data']['boards'][0]['items_page']['items'][3]['column_values'][1]['value'][1]['text'])

# print(res['data']['boards'][0]['items_page']['items'][1]['column_values'][1]['value']['url'])
# # Date
# print(res['data']['boards'][0]['items_page']['items'][1]['column_values'][2]['value']) 
# # Description
# print(res['data']['boards'][0]['items_page']['items'][1]['column_values'][3]['value'])
