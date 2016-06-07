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

def optional_field(index, column, dataframe, default='N/A'):
    row = dataframe.iloc[index]
    return row[column] if (column in dataframe.columns and not pd.isnull(row[column])) else default


tables = {}
# Table1: Agency
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
    agency_df = pd.read_csv("agencies/bart/agency.txt")
except Exception as e:
    print os.getcwd()
    print e

columns = ['agency_id', 'agency_name', 'agency_url', 'agency_timezone', 'agency_lang',
           'agency_phone', 'timezone_name']
tables['Agency'] = pd.DataFrame(index=np.r_[0:len(agency_df.index)], columns=columns)
for i, row in agency_df.iterrows():
    new_row = tables["Agency"].loc[i] #instantiate a NEW ROW

    new_row['agency_id'] = 1
    new_row['agency_name'] = row['agency_name']
    new_row['agency_url'] = row['agency_url']
    timezone = pytz.timezone(row['agency_timezone'])
    # WHAT IS DIFFERENCE BETWEEN AGENCY TIMEZONE and timezone_name???
    # new_row['agency_timezone'] = timezone
    new_row['agency_lang'] = optional_field(i, 'agency_lang', agency_df)
    new_row['agency_phone'] = optional_field(i, 'agency_phone', agency_df)
    new_row['timezone_name'] = row['agency_timezone']

#Now use tables['Agency'] and write it to the database
print tables['Agency'].dtypes
another_dataframe = tables['Agency']
db = MySQLdb.connect(host="localhost", user="root",
                     passwd="berkeleypath", db="TrafficTransit")
another_dataframe.to_sql(con=db, flavor='mysql', name="Agency", if_exists="replace",
                         dtype = {'agency_id' : "INT(10) NOT NULL",
                                  'agency_name': "VARCHAR(255) NOT NULL",
                                   'agency_url': "VARCHAR(255) NOT NULL",
                                    # 'agency_timezone': "smallint(6) NOT NULL",
                                    'agency_lang': "VARCHAR(255) NOT NULL",
                                    'agency_phone': "VARCHAR(255) NOT NULL",
                                    'timezone_name': "VARCHAR(45) NOT NULL"})


#Table 2: Routes
# int agency_id -> 'agency_id' int(10) unsigned
# required string route_short_name -> 'route_short_name' varchar(255)
# optional int direction_id -> 'route_dir' int(10) unsigned
# required int route_type -> 'route_type' int(10) unsigned
# required string route_long_name -> 'route_long_name' varchar(255) default 'N/A'
# optional string route_desc -> 'route_desc' varchar(255) default 'N/A'
# optional string route_url -> 'route_url' varchar(255) default 'N/A'
# optional string route_color -> 'route_color' varchar(255) default 'FFFFFF',
# optional string route_text_color -> 'route_text_color' varchar(255) default '000000'
# required string route_id -> 'route_id' varchar(255) default '000000'
# zipfile MD5 -> 'version' varchar(255)
try:
    agency_df = pd.read_csv("agencies/bart/agency.txt")
except Exception as e:
    print os.getcwd()
    print e

columns = ['agency_id', 'agency_name', 'agency_url', 'agency_timezone', 'agency_lang',
           'agency_phone', 'timezone_name']
tables['Agency'] = pd.DataFrame(index=np.r_[0:len(agency_df.index)], columns=columns)
for i, row in agency_df.iterrows():
    new_row = tables["Agency"].loc[i] #instantiate a NEW ROW

    new_row['agency_id'] = 1
    new_row['agency_name'] = row['agency_name']
    new_row['agency_url'] = row['agency_url']
    timezone = pytz.timezone(row['agency_timezone'])
    # WHAT IS DIFFERENCE BETWEEN AGENCY TIMEZONE and timezone_name???
    # new_row['agency_timezone'] = timezone
    new_row['agency_lang'] = optional_field(i, 'agency_lang', agency_df)
    new_row['agency_phone'] = optional_field(i, 'agency_phone', agency_df)
    new_row['timezone_name'] = row['agency_timezone']

#Now use tables['Agency'] and write it to the database
print tables['Agency'].dtypes
another_dataframe = tables['Agency']
db = MySQLdb.connect(host="localhost", user="root",
                     passwd="berkeleypath", db="TrafficTransit")
another_dataframe.to_sql(con=db, flavor='mysql', name="Agency", if_exists="replace",
                         dtype = {'agency_id' : "INT(10) NOT NULL",
                                  'agency_name': "VARCHAR(255) NOT NULL",
                                   'agency_url': "VARCHAR(255) NOT NULL",
                                    # 'agency_timezone': "smallint(6) NOT NULL",
                                    'agency_lang': "VARCHAR(255) NOT NULL",
                                    'agency_phone': "VARCHAR(255) NOT NULL",
                                    'timezone_name': "VARCHAR(45) NOT NULL"})



