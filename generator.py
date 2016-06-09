import requests
import zipfile
import StringIO
import os
import pandas as pd
import numpy as np
import pytz
import datetime
import MySQLdb
from sqlalchemy import create_engine
from sqlalchemy.types import String

#Goal: Use Bart Information to Produce the Task 1 For Bart

def optional_field(index, column, dataframe, default='N/A'):
    row = dataframe.iloc[index]
    return row[column] if (column in dataframe.columns and not pd.isnull(row[column])) else default


tables = {}
trip2pattern = {}
trip2vehicle = {}
agency_id = 1 #for bart
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
# print tables['Agency'].dtypes
another_dataframe = tables['Agency']
db = MySQLdb.connect(host="localhost", user="root",
                     passwd="root", db="TrafficTransit")

another_dataframe.to_sql(con=db, flavor='mysql', name="Agency", if_exists="replace")


# Table 2: Stops
# # int agency_id -> 'agency_id' int(10) unsigned
# # required string stop_id -> 'stop_id' bigint(20) unsigned
# # optional string stop_code -> 'stop_code' varchar(255) default 'N/A'
# # required string stop_name -> 'stop_name' varchar(255)
# # optional string stop_desc -> 'stop_desc' varchar(255) default 'N/A'
# # required float stop_lat -> 'stop_lat' double
# # required float stop_lon -> 'stop_lon' double
# # stop_lat and stop_lon -> 'lat_lon' point
# # optional string stop_url -> `stop_url` varchar(255) default 'N/A'
# # optional int location_type -> 'location_type' int(10) unsigned default '0'
# # optional int parent_station -> 'parent_station' bigint(20) unsigned default '0'
# # optional int wheelchair_boarding -> 'wheelchair_boarding' int(10) unsigned default '0'
# # zipfile MD5 -> 'version' varchar(255)
try:
    stops_df = pd.read_csv("agencies/bart/stops.txt")
except Exception as e:
    print os.getcwd()
    print e

# print stops_df
columns = ['agency_id', 'stop_id', 'stop_code', 'stop_name', 'stop_desc', 'stop_lat', 'stop_lon',
           'lat_lon', 'stop_url', 'location_type', 'parent_station', 'wheelchair_boarding', 'version']
tables['Stops'] = pd.DataFrame(index=np.r_[0:len(stops_df.index)], columns=columns)
for i, row in stops_df.iterrows():
    new_row = tables["Stops"].loc[i] #instantiate a NEW ROW
    new_row['agency_id'] = 1
    new_row['stop_id'] = str(row['stop_id'])
    new_row['stop_code'] = str(optional_field(i, 'stop_code', stops_df))
    new_row['stop_name'] = str(row['stop_name'])
    new_row['stop_desc'] = str(optional_field(i, 'stop_desc', stops_df))
    new_row['stop_lat'] = float(row['stop_lat'])
    new_row['stop_lon'] = float(row['stop_lon'])
    new_row['lat_lon'] = 0 # some calculations, ignore until using MySQL
    new_row['stop_url'] = str(optional_field(i, 'stop_url', stops_df))
    new_row['location_type'] = int(optional_field(i, 'location_type', stops_df, 0))
    new_row['parent_station'] = int(optional_field(i, 'parent_station', stops_df, 0))
    new_row['wheelchair_boarding'] = int(optional_field(i, 'wheelchair_boarding', stops_df, 0))
    #what the hell is version??
    new_row['version'] = 1;

#Now use tables['Agency'] and write it to the database
# print tables['Stops'].dtypes
another_dataframe = tables['Stops']
db = MySQLdb.connect(host="localhost", user="root",
                     passwd="root", db="TrafficTransit")
another_dataframe.to_sql(con=db, flavor='mysql', name="Stops", if_exists="replace")
                         # dtype = {'agency_id' : "INT(10) ",
                         #          'stop_id': "VARCHAR(255) ",
                         #           'stop_code': "VARCHAR(255) default 'N/A' ",
                         #            'stop_name': "VARCHAR(255) ",
                         #            'stop_desc': "VARCHAR(255) default 'N/A' ",
                         #            'stop_lat': "double ",
                         #            'lat_lon' : "point ",
                         #            'stop_url': "VARCHAR(255) ",
                         #            'location_type': "INT(10) ",
                         #            'parent_station': "BIGINT(20) default '0'",
                         #            'wheelchair_boarding': "INT(100) default '0'"})


# Table 3: Routes
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
    routes_df = pd.read_csv("agencies/bart/routes.txt")
    trips_df = pd.read_csv("agencies/bart/trips.txt")
except Exception as e:
    print os.getcwd()
    print e

tables['Routes'] = pd.DataFrame()
columns = ['agency_id', 'route_short_name', 'route_dir',
           'route_type', 'route_long_name', 'route_desc',
           'route_url', 'route_color', 'route_text_color', 'route_id', 'version']

for i, row in routes_df.iterrows():
    count = 0
    for direction_id in trips_df.loc[trips_df['route_id'] == row['route_id']]['direction_id'].unique():
        new_row = {}
        new_row['agency_id'] = 1
        new_row['route_short_name'] = str(optional_field(i, 'route_short_name', routes_df, routes_df.iloc[i]['route_long_name']))
        new_row['route_dir'] = direction_id
        new_row['route_type'] = int(row['route_type'])
        new_row['route_long_name'] = str(optional_field(i, 'route_long_name', routes_df, routes_df.iloc[i]['route_short_name']))
        new_row['route_desc'] = optional_field(i, 'route_desc', routes_df)
        new_row['route_url'] = optional_field(i, 'route_url', routes_df)
        new_row['route_color'] = optional_field(i, 'route_color', routes_df, default='FFFFFF').upper()
        new_row['route_text_color'] = optional_field(i, 'route_text_color', routes_df, default='000000').upper()
        new_row['route_id'] = str(row['route_id'])
        new_row['version'] = 1
        tables['Routes'] = tables['Routes'].append(pd.Series(new_row), ignore_index=True)

# temp_routes = pd.merge(routes_df, trips_df, on="route_id")
# temp_routes = temp_routes.drop_duplicates(subset = ['route_id', 'direction_id'])

another_dataframe = tables['Routes']
db = MySQLdb.connect(host="localhost", user="root",
                     passwd="root", db="TrafficTransit")
another_dataframe.to_sql(con=db, flavor='mysql', name="Routes", if_exists="replace", index=False)


# Table 4: Route_stop_seq
# # int agency_id -> 'agency_id' int(10) unsigned
# # required string route_short_name -> 'route_short_name' varchar(255)
# # optional int direction_id -> 'route_dir' int(10) unsigned
# route_short_name + route_dir + seq -> 'pattern_id' varchar(255)
#  `stop_id` int(10) unsigned NOT NULL,
#  `seq` int(10) unsigned NOT NULL,
#  `is_time_point` int(10) unsigned NOT NULL Default 0,
# zipfile MD5 -> 'version' varchar(255)
'''
print "starting"
try:
    routes_df = pd.read_csv("agencies/bart/routes.txt")
    trips_df = pd.read_csv("agencies/bart/trips.txt")
    stop_times_df = pd.read_csv("agencies/bart/stop_times.txt")
except Exception as e:
    print os.getcwd()
    print e

columns = ['agency_id', 'route_short_name', 'route_dir',
           'pattern_id', 'stop_id', 'seq', 'is_time_point', 'version']

tables['Route_stop_seq'] = pd.DataFrame()
for i, row in routes_df.iterrows(): #iterate through the different routes
    route_id = row['route_id']
    patterns = [] #the patterns
    print row['route_long_name']
    #iterate through trips where trip_id = route_id
    for j, subrow in trips_df.loc[trips_df['route_id'] == route_id].iterrows():
        trip_id = subrow['trip_id'] #get the trip_id
        direction_id = subrow['direction_id'] if 'direction_id' in subrow else 0 #get the direction id in the trip
        #from all stop times that correspond to a certain trip id
        trip_id_block = stop_times_df.loc[stop_times_df['trip_id'] == trip_id]
        sequence = trip_id_block['stop_id'].tolist()
        if str(sequence) not in patterns:
            patterns += [str(sequence)]
        pattern_num = patterns.index(str(sequence)) + 1
        route_short_name = str(optional_field(i, 'route_long_name', routes_df))
        print route_short_name
        pattern_id = "{0}_{1}_{2}".format(route_short_name, direction_id, pattern_num)
        print pattern_id
        for k, subsubrow in trip_id_block.iterrows():
            new_row = {}
            new_row['agency_id'] = agency_id
            new_row['route_short_name'] = route_short_name
            new_row['route_dir'] = direction_id
            new_row['pattern_id'] = pattern_id
            new_row['stop_id'] = str(subsubrow['stop_id'])
            new_row['seq'] = subsubrow['stop_sequence']
            new_row['is_time_point'] = int(optional_field(k, 'timepoint', stop_times_df, 0))
            new_row['version'] = 1; #replace later
            tables["Route_stop_seq"] = tables["Route_stop_seq"].append(pd.Series(new_row), ignore_index=True)

#write the table
print tables["Route_stop_seq"]
db = MySQLdb.connect(host="localhost", user="root",
                     passwd="root", db="TrafficTransit")
tables['Route_stop_seq'].to_sql(con=db, flavor='mysql', name="Route_stop_seq",
                                if_exists="replace", index=False, chunksize = 10000)
print "end"
'''
# Table 5: RunPattern
# # `agency_id` int(10) unsigned NOT NULL,
# # `route_short_name` varchar(255) NOT NULL,
# # `start_date` date NOT NULL,
# # `end_date` date NOT NULL,
# # `service_id` varchar(255) NOT NULL,
# # `day` char(7) NOT NULL,
# # `route_dir` int(10) unsigned NOT NULL,
# # `run` int(10) unsigned NOT NULL,
# # `pattern_id` varchar(255) NOT NULL,
# # `trip_headsign` varchar(255) NOT NULL,
# # `trip_id` bigint(20) unsigned NOT NULL,
# # `version` varchar(255) NOT NULL,
try:
    trips_df = pd.read_csv("agencies/bart/trips.txt")
    calendar_df = pd.read_csv("agencies/bart/calendar.txt")
    stop_times_df = pd.read_csv("agencies/bart/stop_times.txt")
except Exception as e:
    print os.getcwd()
    print e

columns = ['agency_id', 'route_short_name', 'start_date', 'end_date', 'service_id', 'day', 'route_dir', 'run', 'pattern_id', 'trip_headsign', 'trip_id', 'version']
tables['RunPattern'] = pd.DataFrame(index=np.r_[0:len(trips_df.index)], columns=columns)
day_count = {}
for i, row in trips_df.iterrows():
    new_row = tables["RunPattern"].loc[i]
    new_row['agency_id'] = 1
    j = np.where(routes_df['route_id'] == row['route_id'])[0][0]
    new_row['route_short_name'] = str(optional_field(j, 'route_short_name', routes_df, routes_df.iloc[j]['route_long_name']))
    new_row['service_id'] = row['service_id']
    calendar = calendar_df.loc[calendar_df['service_id'] == row['service_id']].iloc[0]
    new_row['start_date'] = datetime.datetime.strptime(str(calendar['start_date']), "%Y%m%d")
    new_row['end_date'] = datetime.datetime.strptime(str(calendar['end_date']), "%Y%m%d")
    new_row['day'] = "{0}{1}{2}{3}{4}{5}{6}".format(calendar['monday'], calendar['tuesday'], calendar['wednesday'], calendar['thursday'], calendar['friday'], calendar['saturday'], calendar['sunday'])
    if new_row['day'] not in day_count:
        day_count[new_row['day']] = 1
    new_row['route_dir'] = int(optional_field(i, 'direction_id', trips_df, 0))
    new_row['run'] = day_count[new_row['day']]
    day_count[new_row['day']] += 1
    # new_row['pattern_id'] = trip2pattern[row['trip_id']]
    new_row['pattern_id'] = 10000
    new_row['trip_headsign'] = optional_field(i, 'trip_headsign', trips_df, stop_times_df.loc[stop_times_df['trip_id'] == row['trip_id']]['stop_headsign'].iloc[0])
    new_row['trip_id'] = str(row['trip_id'])
    new_row['version'] = 1
db = MySQLdb.connect(host="localhost", user="root",
                     passwd="root", db="TrafficTransit")
tables['RunPattern'].to_sql(con=db, flavor='mysql', name="RunPattern",
                                if_exists="replace", index=False, chunksize = 10000)
print "success"
#write the runPattern table to the mysqldatabase



