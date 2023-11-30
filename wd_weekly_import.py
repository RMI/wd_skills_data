# Purpose: Umberalla script to execute weekly data updates from Workday.

# L&D offerings
exec(open('wd_skills_data/wd_learning.py').read())

# Worker administrative data
exec(open('wd_skills_data/wd_worker_data.py').read())

# Skill interests
exec(open('wd_skills_data/wd_skill_interests.py').read())

# Worker schools attended and fields of study
exec(open('wd_skills_data/wd_schools.py').read())

# Worker profile skills
exec(open('wd_skills_data/wd_profile_skills.py').read())

# Mentor skills
exec(open('wd_skills_data/wd_mentor_skills.py').read())

# Languages
exec(open('wd_skills_data/wd_languages.py').read())

# Licenses
exec(open('wd_skills_data/rmi_license_import.py').read())


# Skill migration
exec(open('wd_skills_data/wd_skills_migration.py').read())


