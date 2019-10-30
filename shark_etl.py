#!/usr/bin/env python
# coding: utf-8

# In[1]:


import csv
from datetime import datetime, timedelta
import pyodbc


# In[2]:


conn = pyodbc.connect('DSN=kubricksql;UID=DE14;PWD=password')
cur = conn.cursor()


# In[3]:


sharkfile = r'c:\data\GSAF5.csv'


# In[4]:


attack_dates = []
case_number = []
country = []
activity = []
age = []
gender = []
is_fatal = []
with open(sharkfile) as f:
    reader = csv.DictReader(f)
    for row in reader:
        attack_dates.append(row['Date'])
        case_number.append(row['Case Number'])
        country.append(row['Country'])
        activity.append(row['Activity'])
        age.append(row['Age'])
        gender.append(row['Sex '])
        is_fatal.append(row['Fatal (Y/N)'])


# In[5]:


data = zip(attack_dates, case_number, country, activity, age, gender, is_fatal)


# In[6]:


# use this command for wipe and load
cur.execute('truncate table jess.shark')


# In[7]:


q = 'insert into jess.shark (attack_date, case_number, country, activity, age, gender, is_fatal) values (?, ?, ?, ?, ?, ?, ?)'


# In[8]:


for d in data:
    try:
        cur.execute(q, d)
        conn.commit()
    except:
        conn.rollback()


# In[ ]:




