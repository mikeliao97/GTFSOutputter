import MySQLdb
import os
import pandas as pd
import numpy as np
import pytz
import MySQLdb
from google.transit import gtfs_realtime_pb2
import urllib
import datetime

def agencies(tables):
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
        new_row['agency_timezone'] = timezone
        new_row['agency_lang'] = optional_field(i, 'agency_lang', agency_df)
        new_row['agency_phone'] = optional_field(i, 'agency_phone', agency_df)
        new_row['timezone_name'] = row['agency_timezone']

    # Now use tables['Agency'] and write it to the database
    print tables['Agency'].dtypes
    another_dataframe = tables['Agency']
    db = MySQLdb.connect(host="localhost", user="root",
                         passwd="root", db="TrafficTransit")

    another_dataframe.to_sql(con=db, flavor='mysql', name="Agency", if_exists="replace")