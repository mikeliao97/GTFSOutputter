import requests
import zipfile
import StringIO
import os
import pandas as pd
import numpy as np
import pytz
import datetime
import MySQLdb
import sqlalchemy
from sqlalchemy.types import String

#Goal: Use Bart Information to Produce the Task 1 For Bart
# Static Feed
# ---- Task 1 ----
# Agency
# int agency_id -> 'agency_id' int(10) unsigned
# required string agency_name  -> 'agency_name' varchar(255)
# required string agency_url -> 'agency_url' varchar(255)
# required string agency_timezone -> 'agency_timezone' smallint(6)
# optional string agency_lang -> 'agency_lang' varchar(255)
# optional string agency_phone -> 'agency_phone' varchar(255)
# required string agency_timezone -> 'timezone_name' varchar(45)
# PRIMARY KEY ('agency_id')
# KEY ('agency_timezone')

try:
    df = pd.read_csv("agencies/bart/agency.txt")
except Exception as e:
    print os.getcwd()
    print e


#what is the point of this?
#what you need to do is to collect information, put it in df and then write it into a new table, then
#write it to the SQL DATABASE
# df.to_sql(con=db, flavor='mysql', index_label="agency_id", name="new_database", if_exists="replace")

tables = {}
columns = ['agency_id', 'agency_name', 'agency_url', 'agency_timezone', 'agency_lang', 'agency_phone', 'timezone_name', 'whatever']
tables['Agency'] = pd.DataFrame(index=np.r_[0:len(df.index)], columns=columns)
for i, row in df.iterrows():
    new_row = tables["Agency"].loc[i]
    new_row['agency_id'] = 10
    new_row['agency_name'] = row['agency_name']
    new_row['agency_url'] = row['agency_url']
    timezone = pytz.timezone(row['agency_timezone'])
    # new_row['agency_lang'] = optional_field(i, 'agency_lang', static_feed['agency'])
    # new_row['agency_phone'] = optional_field(i, 'agency_phone', static_feed['agency'])
    new_row['timezone_name'] = row['agency_timezone']

#Now use tables['Agency'] and write it to the database
print tables['Agency']
another_dataframe = tables['Agency']
db = MySQLdb.connect(host="localhost", user="root",
                     passwd="berkeleypath", db="TrafficTransit")
another_dataframe.to_sql(con=db, flavor='mysql', name="test_database", if_exists="replace")


