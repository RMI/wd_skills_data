# Workday Skills and Experiences ETL
 This repository includes Python scripts for migrating skills and experiences data from Workday into RMI's M365 environment (including SharePoint and Power BI). The scripts were developed as data access was being expanded and altered, so inefficiences do exist and will be improved as time allows. Currently, the repository and ETL process are maintained by the Knowledge Management team, and the resulting data are utilized in the RMI People Finder, the Skills System Adoption Metrics dashboard, and for various ad hoc reports related to skills and experiences.

## Requirements/Setup
- Clone this repository
- Using preferred code editor, review scripts to update hard coded OneDrive file paths to desired output locations
   - **Note:** This will be changed to a single parameters file in the future.
- Add an environment file, **cred.env**, with the following:
   - DBASE_PWD= Database password
   - DBASE_IP= IP address for target database
   - **Note:** Currently, data are stored in an Azure for MySQL environment, schema name **rmi_skills**
 - Gain access to the following reports from Workday:
    - Worker Skills for Export
    - RMI Active Worker Directory
    - Learning Content Audit
    - RMI-Languages
    - RMI-Employee Education
    - RMI-Skill Interest
    - RMI-Mentor Skills

## Usage
- This ETL process is typically executed twice per week, on Monday and Wednesday
- The script, **"wd_weekly_import.py"** is the only script that should be required for standard data processing
- The following outlines the steps to update all skills and experiences data:
    - Download all reports outlined above from Workday and place them in your repository folder
        - This is currently a manual process, but will be updated to retrieval from Azure blob storage in CY24
    - Open and execute **"wd_weekly_import.py"**
    - Monitor for errors and debug as necessary
