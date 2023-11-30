import win32com.client as win32
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt
import seaborn as sns
from textwrap import TextWrapper
from matplotlib.ticker import MultipleLocator
import base64
from sqlalchemy import text

load_dotenv('cred.env')
rmi_db = os.getenv('DBASE_PWD')
rmi_db_ip = os.getenv('DBASE_IP')

# database cred
database_username = 'rmiadmin'
database_password = rmi_db
database_ip       = rmi_db_ip
database_name     = 'rmi_skills'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))

# Get all profile skills and group them by program and role
query = "select worker_skills_all.email, worker_details.cost_center, worker_details.job_title, worker_skills_all.skill_type, worker_skills_all.skill_category, worker_skills_all.skill from worker_skills_all, worker_details where worker_skills_all.skill_type = 'profile' and worker_details.email = worker_skills_all.email"

with database_connection.connect() as conn:
    result = conn.execute(text(query))
    df1 = pd.DataFrame(result.fetchall())
    df1.columns = result.keys()

#copy data 
skills_all = df1
####################################################################
# print(df1.columns)
# df_role = df1

# df_role['count'] = df_role.groupby(['job_title', 'skill'])['skill'].transform('count')
# df_role['drop'] = df_role['job_title'] + df_role['skill']
# df_role = df_role.drop_duplicates(subset=['drop'])

# Get 10 most popular skills by program, used for bar plots
# top_5_role = df_role.groupby(['job_title']).apply(lambda x: x.nlargest(5, 'count')).reset_index(drop=True)
# top_5_role = top_5_role[['job_title', 'skill', 'count']]

# print(top_5_role)

# top_summary = top_5_role.groupby(['job_title'])['skill'].agg(lambda x: ', '.join(map(str, set(x)))).reset_index()

# top_summary.to_excel('skillsByCostCenter.xlsx')

########################################################################
# Get most popular skills by cost center
df = df1

df['count'] = df.groupby(['cost_center', 'skill'])['skill'].transform('count')
df['drop'] = df['cost_center'] + df['skill']
df = df.drop_duplicates(subset=['drop'])

# Get 10 most popular skills by program, used for bar plots
top_10_counts = df.groupby(['cost_center']).apply(lambda x: x.nlargest(5, 'count')).reset_index(drop=True)
top_10_counts = top_10_counts[['cost_center', 'skill', 'count']]

# Aggregate top skills into single cell, used for text summary
top_summary = top_10_counts.groupby(['cost_center'])['skill'].agg(lambda x: ', '.join(map(str, set(x)))).reset_index()

blue_palette=['#003B63', '#134972', '#255881', '#386690', '#4A759F', '#5D83AE', '#6F92BD', '#82A0CC', '#94AFDB', '#A7BDEA']

# Loop through and create plot images for each cost center
for i in top_10_counts['cost_center']:
    df = top_10_counts[top_10_counts['cost_center'] == i]
    plt.figure(figsize=(6, 4))
    wrapper = TextWrapper(width=15)  # Adjust the width as needed
    wrapped_labels = [wrapper.fill(text) for text in df['skill']]
    ax = sns.barplot(x='count', y=wrapped_labels, data=df, palette=blue_palette, orient='h')
    # Adjust plot appearance
    plt.title('Popular Skills in Your Program')
    ax.set_yticklabels(wrapped_labels)
    ax.tick_params(bottom=False)
    plt.yticks(fontsize = 8)
    plt.xticks(fontsize = 8)
    sns.despine(right=True)
    sns.despine(bottom=True)
    plt.xticks(visible=False)
    sns.despine(bottom=True)
    # Set x-axis tick interval to whole numbers
    x_major_locator = MultipleLocator(base=1)  # Set the desired interval (e.g., 1 for whole numbers)
    ax.xaxis.set_major_locator(x_major_locator)
    ax.xaxis.set_label_text('')
    name = 'skill_charts/'+ i + 'chart.png'
    plt.savefig(name, bbox_inches='tight')
    plt.close()

# Get all staff without profile skills
query = "select worker_details.email, worker_details.cost_center, worker_details.job_title from worker_details"

with database_connection.connect() as conn:
    result = conn.execute(query)
    df1 = pd.DataFrame(result.fetchall())
    df1.columns = result.keys()

df1.set_index('email')
df_no_skills = df1.drop(df1[df1.email.isin(skills_all['email'])].index.tolist())
df_no_skills.reset_index(inplace=True)

# merge top skill summary by cost center
df = pd.merge(df_no_skills, top_summary, 'left', on='cost_center')

# contacts = df.groupby(['cost_center'])['email'].agg(lambda x: ', '.join(map(str, set(x)))).reset_index()

#Use this for contacts after testing is complete
contacts = df[['cost_center','email']]
active_programs = top_10_counts['cost_center'].unique()
# Filter to staff in cost centers with at least one skill profile
contacts = contacts[contacts['cost_center'].isin(active_programs)]
# Get list of cost centers to send to
cost_centers = contacts['cost_center'].unique()

# contacts.to_excel('impacted_users.xlsx')

# Create data for testing
data = [['Africa', 'ghoffman@rmi.org'], ['US', 'ghoffman@rmi.org'],
        ['Africa', 'jpruzan@rmi.org'], ['US', 'mcrogle@rmi.org'], ['US', 'yqi@rmi.org'],
        ['Strategy Team', 'mskinner@rmi.org'], ['China', 'kmark@rmi.org'], ['Strategy Team', 'lauren.gilmartin@rmi.org']] 
# Create df with testing contacts
contacts = pd.DataFrame(data, columns=['cost_center', 'email']) 
cost_centers = ['Africa', 'US', 'China', 'Strategy Team']

# Emails for to line
to = ['ghoffman@rmi.org','knowledgemgmt@rmi.org']
mail_to = "; ".join(to)

# Counter to limit the number of emails to the number of cost centers present
index = 0
limit = len(cost_centers)


# for each cost center, send email with chart. Members are cost center are cc'd, to line is Glenn and the KM group.
for i in cost_centers:

    # Encode the image as a data URI
    with open('skill_charts/'+ i + 'chart.png', 'rb') as image_file:
        image_data = base64.b64encode(image_file.read()).decode('utf-8')

# Create an HTML email with the embedded image
    html_email_message = f"""
    <html>
    <head></head>
    <body>
    <p>
    A reliable database of staff skills and experiences equips RMI to harness its in-house expertise and connect staff 
    at the right moments.<b> Please ensure your skill profile reflects your skills today!</b><span style="color: red;">
    You will continue to receive this request until you complete your skill profile.</span>
    <br><br>
    <b>Need some inspiration?</b> The chart below shows the most popular skills on your team. 
    What skills do you share? What unique skills do you bring to the table?
    <br><br>
    <b>Take Action:</b><br>
    <a href="https://www.myworkday.com/rockymountain/d/task/2998$2353.htmld">Add Skills to Your Workday Profile</a></li>
    (Need help? There’s a <a href="https://rockmtnins.sharepoint.com/:b:/s/Workday/ETwlxY5QVt5Kr4pVUm_yo08B3a0bsaE2EguVigdkWD4NTw?e=LkAi6j">guide</a> for that)
    <br>
    <a href="https://rockmtnins.sharepoint.com/:u:/r/sites/KnowledgeMgmt/SitePages/Skills-Dashboard.aspx?csf=1&web=1&share=EShh03b_EZ5NhuWns5V2OGEB55DRYyjdRmoEyaF4lsz20Q&e=rA0tCh">Access the RMI People Finder</a> to see how you can tap this expanding database!
    <br>
    <a href="https://rockmtnins.sharepoint.com/:u:/r/sites/KnowledgeMgmt/SitePages/Skills-System-Adoption-Metrics.aspx?csf=1&web=1&share=EQOuu2YA49RHkBBpVFTibvcBCz4Dj6uYBBIM6NgNFQlQgA&e=JnaIZG">View Adoption Metrics</a> to see how close we are to our 100% goal
    <br>
    <div style="text-align: center;">
    <a href= "https://rockmtnins.sharepoint.com/:u:/r/sites/KnowledgeMgmt/SitePages/Skills-System-Adoption-Metrics.aspx?csf=1&web=1&share=EQOuu2YA49RHkBBpVFTibvcBCz4Dj6uYBBIM6NgNFQlQgA&e=JnaIZG">
    <img src="data:image/png;base64,{image_data}" alt="Horizontal bar chart with popular skills"></a>
    </div> <br>
    Please reach out to <b>knowledgemgmt@rmi.org</b> with any questions.
    </body>
    </html>
    """
    email = contacts[contacts['cost_center'] == i]
    email = email['email']
    email = list(email)
    send_to = "; ".join(email)

    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.To = mail_to
    mail.CC =  send_to
    mail.Subject = 'Reminder to update your skill profile'
    mail.HTMLBody = html_email_message
    mail.Send()
    index += 1
    if index == limit:
        break

