#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import pandas as pd
import numpy as np
import pytz
import MySQLdb
from google.transit import gtfs_realtime_pb2
import urllib
import datetime
import helper




tables = {}
trip2pattern = {}
trip2vehicle = {}
agency_id = 1 #for bart
'''
'''
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
    new_row['agency_timezone'] = timezone
    new_row['agency_lang'] = helper.optional_field(i, 'agency_lang', agency_df)
    new_row['agency_phone'] = helper.optional_field(i, 'agency_phone', agency_df)
    new_row['timezone_name'] = row['agency_timezone']

# Now use tables['Agency'] and write it to the database
print tables['Agency'].dtypes
another_dataframe = tables['Agency']
db = MySQLdb.connect(host="localhost", user="root",
                     passwd="root", db="TrafficTransit")

another_dataframe.to_sql(con=db, flavor='mysql', name="Agency", if_exists="replace")
'''
# Table 2: Stops
# int agency_id -> 'agency_id' int(10) unsigned
# required string stop_id -> 'stop_id' bigint(20) unsigned
# optional string stop_code -> 'stop_code' varchar(255) default 'N/A'
# required string stop_name -> 'stop_name' varchar(255)
# optional string stop_desc -> 'stop_desc' varchar(255) default 'N/A'
# required float stop_lat -> 'stop_lat' double
# required float stop_lon -> 'stop_lon' double
# stop_lat and stop_lon -> 'lat_lon' point
# optional string stop_url -> `stop_url` varchar(255) default 'N/A'
# optional int location_type -> 'location_type' int(10) unsigned default '0'
# optional int parent_station -> 'parent_station' bigint(20) unsigned default '0'
# optional int wheelchair_boarding -> 'wheelchair_boarding' int(10) unsigned default '0'
# zipfile MD5 -> 'version' varchar(255)
# try:
#     stops_df = pd.read_csv("agencies/bart/stops.txt")
# except Exception as e:
#     print os.getcwd()
#     print e
#
print stops_df
# columns = ['agency_id', 'stop_id', 'stop_code', 'stop_name', 'stop_desc', 'stop_lat', 'stop_lon',
#            'lat_lon', 'stop_url', 'location_type', 'parent_station', 'wheelchair_boarding', 'version']
# tables['Stops'] = pd.DataFrame(index=np.r_[0:len(stops_df.index)], columns=columns)
# for i, row in stops_df.iterrows():
#     new_row = tables["Stops"].loc[i] #instantiate a NEW ROW
#     new_row['agency_id'] = 1
#     new_row['stop_id'] = str(row['stop_id'])
#     new_row['stop_code'] = str(helper.optional_field(i, 'stop_code', stops_df))
#     new_row['stop_name'] = str(row['stop_name'])
#     new_row['stop_desc'] = str(helper.optional_field(i, 'stop_desc', stops_df))
#     new_row['stop_lat'] = float(row['stop_lat'])
#     new_row['stop_lon'] = float(row['stop_lon'])
#     new_row['lat_lon'] = 0 # some calculations, ignore until using MySQL
#     new_row['stop_url'] = str(helper.optional_field(i, 'stop_url', stops_df))
#     new_row['location_type'] = int(helper.optional_field(i, 'location_type', stops_df, 0))
#     new_row['parent_station'] = int(helper.optional_field(i, 'parent_station', stops_df, 0))
#     new_row['wheelchair_boarding'] = int(helper.optional_field(i, 'wheelchair_boarding', stops_df, 0))
    what the hell is version??
    # new_row['version'] = 1;
#
Now use tables['Agency'] and write it to the database
print tables['Stops'].dtypes
# another_dataframe = tables['Stops']
# db = MySQLdb.connect(host="localhost", user="root",
#                      passwd="root", db="TrafficTransit")
# another_dataframe.to_sql(con=db, flavor='mysql', name="Stops", if_exists="replace")
                         dtype = {'agency_id' : "INT(10) ",
                                  'stop_id': "VARCHAR(255) ",
                                   'stop_code': "VARCHAR(255) default 'N/A' ",
                                    'stop_name': "VARCHAR(255) ",
                                    'stop_desc': "VARCHAR(255) default 'N/A' ",
                                    'stop_lat': "double ",
                                    'lat_lon' : "point ",
                                    'stop_url': "VARCHAR(255) ",
                                    'location_type': "INT(10) ",
                                    'parent_station': "BIGINT(20) default '0'",
                                    'wheelchair_boarding': "INT(100) default '0'"})
#
#
Table 3: Routes
int agency_id -> 'agency_id' int(10) unsigned
required string route_short_name -> 'route_short_name' varchar(255)
optional int direction_id -> 'route_dir' int(10) unsigned
required int route_type -> 'route_type' int(10) unsigned
required string route_long_name -> 'route_long_name' varchar(255) default 'N/A'
optional string route_desc -> 'route_desc' varchar(255) default 'N/A'
optional string route_url -> 'route_url' varchar(255) default 'N/A'
optional string route_color -> 'route_color' varchar(255) default 'FFFFFF',
optional string route_text_color -> 'route_text_color' varchar(255) default '000000'
required string route_id -> 'route_id' varchar(255) default '000000'
zipfile MD5 -> 'version' varchar(255)
# try:
#     routes_df = pd.read_csv("agencies/bart/routes.txt")
#     trips_df = pd.read_csv("agencies/bart/trips.txt")
# except Exception as e:
#     print os.getcwd()
#     print e
#
# tables['Routes'] = pd.DataFrame()
# columns = ['agency_id', 'route_short_name', 'route_dir',
#            'route_type', 'route_long_name', 'route_desc',
#            'route_url', 'route_color', 'route_text_color', 'route_id', 'version']
#
# for i, row in routes_df.iterrows():
#     count = 0
#     for direction_id in trips_df.loc[trips_df['route_id'] == row['route_id']]['direction_id'].unique():
#         new_row = {}
#         new_row['agency_id'] = 1
#         new_row['route_short_name'] = str(helper.optional_field(i, 'route_short_name', routes_df, routes_df.iloc[i]['route_long_name']))
#         new_row['route_dir'] = direction_id
#         new_row['route_type'] = int(row['route_type'])
#         new_row['route_long_name'] = str(helper.optional_field(i, 'route_long_name', routes_df, routes_df.iloc[i]['route_short_name']))
#         new_row['route_desc'] = helper.optional_field(i, 'route_desc', routes_df)
#         new_row['route_url'] = helper.optional_field(i, 'route_url', routes_df)
#         new_row['route_color'] = helper.optional_field(i, 'route_color', routes_df, default='FFFFFF').upper()
#         new_row['route_text_color'] = helper.optional_field(i, 'route_text_color', routes_df, default='000000').upper()
#         new_row['route_id'] = str(row['route_id'])
#         new_row['version'] = 1
#         tables['Routes'] = tables['Routes'].append(pd.Series(new_row), ignore_index=True)
#
temp_routes = pd.merge(routes_df, trips_df, on="route_id")
temp_routes = temp_routes.drop_duplicates(subset = ['route_id', 'direction_id'])
#
# another_dataframe = tables['Routes']
# db = MySQLdb.connect(host="localhost", user="root",
#                      passwd="root", db="TrafficTransit")
# another_dataframe.to_sql(con=db, flavor='mysql', name="Routes", if_exists="replace", index=False)
#
#
Table 4: Route_stop_seq
# int agency_id -> 'agency_id' int(10) unsigned
# required string route_short_name -> 'route_short_name' varchar(255)
# optional int direction_id -> 'route_dir' int(10) unsigned
route_short_name + route_dir + seq -> 'pattern_id' varchar(255)
 `stop_id` int(10) unsigned NOT NULL,
 `seq` int(10) unsigned NOT NULL,
 `is_time_point` int(10) unsigned NOT NULL Default 0,
zipfile MD5 -> 'version' varchar(255)
# '''
# print "starting route_stop_seq"
# try:
#     routes_df = pd.read_csv("agencies/bart/routes.txt")
#     trips_df = pd.read_csv("agencies/bart/trips.txt")
#     stop_times_df = pd.read_csv("agencies/bart/stop_times.txt")
# except Exception as e:
#     print os.getcwd()
#     print e
#
# columns = ['agency_id', 'route_short_name', 'route_dir',
#            'pattern_id', 'stop_id', 'seq', 'is_time_point', 'version', 'trip_id']
#
# tables['Route_stop_seq'] = pd.DataFrame()
# for i, row in routes_df.iterrows(): #iterate through the different routes
#     route_id = row['route_id']
#     patterns = [] #the patterns
#     iterate through trips where trip_id = route_id
    # for j, subrow in trips_df.loc[trips_df['route_id'] == route_id].iterrows():
    #     trip_id = subrow['trip_id'] #get the trip_id
    #     direction_id = subrow['direction_id'] if 'direction_id' in subrow else 0 #get the direction id in the trip
    #     from all stop times that correspond to a certain trip id
        # trip_id_block = stop_times_df.loc[stop_times_df['trip_id'] == trip_id]
        # sequence = trip_id_block['stop_id'].tolist()
        # if str(sequence) not in patterns:
        #     patterns += [str(sequence)]
        # pattern_num = patterns.index(str(sequence)) + 1
        # route_short_name = str(helper.optional_field(i, 'route_long_name', routes_df))
        # pattern_id = "{0}_{1}_{2}".format(route_short_name, direction_id, pattern_num)
        # for k, subsubrow in trip_id_block.iterrows():
        #     new_row = {}
        #     new_row['trip_id'] = trip_id
        #     new_row['agency_id'] = agency_id
        #     new_row['route_short_name'] = route_short_name
        #     new_row['route_dir'] = direction_id
        #     new_row['pattern_id'] = pattern_id
        #     new_row['stop_id'] = str(subsubrow['stop_id'])
        #     new_row['seq'] = subsubrow['stop_sequence']
        #     new_row['is_time_point'] = int(helper.optional_field(k, 'timepoint', stop_times_df, 0))
        #     new_row['version'] = 1; #replace later
        #     tables["Route_stop_seq"] = tables["Route_stop_seq"].append(pd.Series(new_row), ignore_index=True)
        # trip2pattern[trip_id] = pattern_id
# write the trip2pattern to a csv fle
#
# with open('Trip2Pattern.csv', 'wb') as f:
#     writer = csv.writer(f)
#     writer.writerow(["trip_id", "pattern_id"])
#     for key, value in trip2pattern.items():
#         writer.writerow([key, value])

# write the table
# db = MySQLdb.connect(host="localhost", user="root",
#                      passwd="root", db="TrafficTransit")
# tables['Route_stop_seq'].to_sql(con=db, flavor='mysql', name="Route_stop_seq",
#                                 if_exists="replace", index=False, chunksize = 10000)
'''
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
# try:
#     trips_df = pd.read_csv("agencies/bart/trips.txt")
#     calendar_df = pd.read_csv("agencies/bart/calendar.txt")
#     stop_times_df = pd.read_csv("agencies/bart/stop_times.txt")
#
    # read the trip_id from the csv
    # if len(trip2pattern) == 0:
    #     trip2pattern = helper.csv2df("Trip2Pattern.csv")
    # print "Trip2Pattern--------------------"
# except Exception as e:
#     print os.getcwd()
#     print e
#
# columns = ['agency_id', 'route_short_name', 'start_date', 'end_date', 'service_id', 'day', 'route_dir', 'run', 'pattern_id', 'trip_headsign', 'trip_id', 'version']
# tables['RunPattern'] =  pd.DataFrame(index=np.r_[0:len(trips_df.index)], columns=columns)
# run count needs route_short_name and day to be unique, so use 3 keys....key1, maps to key2.
# Unique Identifiers:  route_short_name, route_dir, day
# Map identifier: {route_short_name : {service_id : {route_dir: # of unique ones} } }
# basically a map of a map of a map
# the first map checks for the route
# run_count = {}
# for i, row in trips_df.iterrows():
#     new_row = tables["RunPattern"].loc[i]
#     new_row['agency_id'] = 1
#     j = np.where(routes_df['route_id'] == row['route_id'])[0][0]
#     new_row['route_short_name'] = str(helper.optional_field(j, 'route_short_name', routes_df, routes_df.iloc[j]['route_long_name']))
#     new_row['service_id'] = row['service_id']
#     calendar = calendar_df.loc[calendar_df['service_id'] == row['service_id']].iloc[0]
#     new_row['start_date'] = datetime.datetime.strptime(str(calendar['start_date']), "%Y%m%d")
#     new_row['end_date'] = datetime.datetime.strptime(str(calendar['end_date']), "%Y%m%d")
#     new_row['route_dir'] = int(helper.optional_field(i, 'direction_id', trips_df, 0))
#     new_row['day'] = "{0}{1}{2}{3}{4}{5}{6}".format(calendar['monday'], calendar['tuesday'], calendar['wednesday'], calendar['thursday'], calendar['friday'], calendar['saturday'], calendar['sunday'])
#     if new_row['day'] not in day_count:
    #     day_count[new_row['day']] = 1
    # if new_row['route_short_name'] not in run_count.keys():
    #     run_count[new_row['route_short_name']] = {new_row['service_id']: {new_row['route_dir'] : 1}}
    #
    # if new_row['service_id'] not in run_count[new_row['route_short_name']].keys():
    #     run_count[new_row['route_short_name']] = {new_row['service_id']: {new_row['route_dir'] : 1}}
    #
    # if new_row['route_dir'] not in run_count[new_row['route_short_name']][new_row['service_id']].keys():
    #     run_count[new_row['route_short_name']] = {new_row['service_id']: {new_row['route_dir'] : 1}}
    #
    # new_row['run'] = run_count[new_row['route_short_name']][new_row['service_id']][new_row['route_dir']]
    # run_count[new_row['route_short_name']][new_row['service_id']][new_row['route_dir']] += 1 #increment the run because we've seen this before....
    #
    # new_row['pattern_id'] = str(trip2pattern[trip2pattern['trip_id'] == row['trip_id']].iloc[0]['pattern_id'])
    #
    # new_row['trip_headsign'] = helper.optional_field(i, 'trip_headsign', trips_df, stop_times_df.loc[stop_times_df['trip_id'] == row['trip_id']]['stop_headsign'].iloc[0])
    # new_row['trip_id'] = str(row['trip_id'])
    # new_row['version'] = 1
# db = MySQLdb.connect(host="localhost", user="root",
#                      passwd="root", db="TrafficTransit")
# tables['RunPattern'].to_sql(con=db, flavor='mysql', name="RunPattern", if_exists="replace", index=False, chunksize=1000)
# print "success"
#write the runPattern table to the mysqldatabase
'''

###Schedules: gets some data from RunPattern table
## Requires: Trip.txt, stop_times.txt
# CREATE TABLE `Schedules` (
#   `agency_id` int(10) unsigned NOT NULL,               GIVEN
#   `route_short_name` varchar(255) NOT NULL,            Trips.txt
#   `start_date` date NOT NULL,                          Calendar.txt????
#   `end_date` date NOT NULL,                            Calendar.txt???
#   `day` char(7) NOT NULL,
#   `route_dir` int(10) unsigned NOT NULL,
#   `run` int(10) unsigned NOT NULL,
#   `pattern_id` varchar(255) NOT NULL,
#   `seq` int(10) unsigned NOT NULL,
#   `stop_id` int(10) unsigned NOT NULL,
#   `is_time_point` int(10) unsigned NOT NULL Default 0,
#   `pickup_type` int(10) unsigned NOT NULL,
#   `dropoff_type` int(10) unsigned NOT NULL,
#   `arrival_time` varchar(10) NOT NULL,
#   `departure_time` varchar(10) NOT NULL,
#   `stop_headsign` varchar(255) default NULL,
#   `trip_id` bigint(20) unsigned NOT NULL,
#   `version` varchar(255) NOT NULL,
#    add the arrival and departure time for each one of the trips.
# for each trip in trips_df
    #get the run # from run_pattern
    #get the sequence fo stops from this trip_id
    '''
'''
try:
    trips_df = pd.read_csv("agencies/bart/trips.txt")
    stop_times_df = pd.read_csv("agencies/bart/stop_times.txt")
    #load the RunPatternTable for miscellaneous things
    login = {'host':"localhost", 'user':"root",
                     'passwd':"root", 'db': "TrafficTransit"}
    run_pattern_df = helper.sql2df('RunPattern', login)
    route_stop_seq_df = helper.sql2df('Route_stop_seq', login)
except Exception as e:
    print os.getcwd()
    print e

columns = ['agency_id', 'route_short_name', 'start_date', 'end_date', 'day',
           'route_dir', 'run', 'pattern_id', 'seq', 'stop_id',
           'is_time_point', 'pickup_type', 'drop_off_type',
           'arrival_time', 'departure_time', 'stop_headsign', 'trip_id']
tables["Schedules"] = pd.DataFrame()
counter = 0
for a, row in trips_df.iterrows():
    run_pattern_trip_specific = run_pattern_df[run_pattern_df['trip_id'] == row['trip_id']]
    route_stop_seq_trip_specific = route_stop_seq_df[route_stop_seq_df['trip_id']
                                                     == row['trip_id']]
    stop_times_trip_specific = stop_times_df[stop_times_df['trip_id'] == row['trip_id']]
    for b, subrow in route_stop_seq_trip_specific.iterrows():
        new_row = {}
        new_row['agency_id'] = 1
        new_row['route_short_name'] = subrow['route_short_name']
        new_row['start_date'] = run_pattern_trip_specific.iloc[0]['start_date']
        new_row['end_date'] = run_pattern_trip_specific.iloc[0]['end_date']
        new_row['day'] = run_pattern_trip_specific.iloc[0]['day']
        new_row['route_dir'] = subrow['route_dir']
        new_row['run'] = run_pattern_trip_specific.iloc[0]['run']
        new_row['pattern_id'] = subrow['pattern_id']
        new_row['seq'] = subrow['seq']
        new_row['stop_id'] = subrow['stop_id']
        new_row['is_time_point'] = subrow['is_time_point']
        new_row['pickup_type'] = stop_times_trip_specific.iloc[0]['pickup_type']
        new_row['drop_off_type'] = stop_times_trip_specific.iloc[0]['drop_off_type']
        new_row['arrival_time'] = stop_times_trip_specific[stop_times_trip_specific['stop_id'] == subrow['stop_id']].iloc[0]['arrival_time']
        new_row['departure_time'] = stop_times_trip_specific[stop_times_trip_specific['stop_id'] == subrow['stop_id']].iloc[0]['departure_time']
        new_row['stop_headsign'] = stop_times_trip_specific[stop_times_trip_specific['stop_id'] == subrow['stop_id']].iloc[0]['stop_headsign']
        new_row['trip_id'] = row['trip_id']
        tables["Schedules"] = tables["Schedules"].append(pd.Series(new_row), ignore_index=True)
        counter += 1
db = MySQLdb.connect(host="localhost", user="root",
                     passwd="root", db="TrafficTransit")
tables['Schedules'].to_sql(con=db, flavor='mysql', name="Schedules", if_exists="replace", index=False, chunksize=1000)
'''

#Table Points
# CREATE TABLE `Points` (
#   `agency_id` int(10) unsigned NOT NULL,
#   `point_id` int(10) unsigned NOT NULL,
#   `point_lat` double NOT NULL,
#   `point_lon` double NOT NULL,
#   `lat_lon` point NOT NULL,
#   `version` varchar(255) NOT NULL,
#   PRIMARY KEY  (`agency_id`,`point_id`,`version`),
#   SPATIAL KEY `lat_lon` (`lat_lon`)
# ) ENGINE=MyISAM DEFAULT
'''
try:
    shapes_df = pd.read_csv("agencies/bart/shapes.txt")
except Exception as e:
    print os.getcwd()
    print e
point_id = 0
#unique lat and lon define a point. Using a map for uniqueness
point_mapper = {}
tables['Points'] = pd.DataFrame()
for a, row in shapes_df.iterrows():
    # if (row['shape_pt_lat'] not in point_mapper.keys()) or (row['shape_pt_lon'] not in point_mapper[row['shape_pt_lat']].keys()):
        new_row = {}
        new_row['agency_id'] = 1
        new_row['point_lat'] = row['shape_pt_lat']
        new_row['point_lon'] = row['shape_pt_lon']
        new_row['shape_id'] = row['shape_id']
        new_row['shape_pt_sequence'] = row['shape_pt_sequence']
        tables["Points"] = tables["Points"].append(pd.Series(new_row), ignore_index=True)
    # else:
    #     print 'repeat!'
db = MySQLdb.connect(host="localhost", user="root",
                     passwd="root", db="TrafficTransit")
tables['Points'].to_sql(con=db, flavor='mysql', name="Points", if_exists="replace", index=False, chunksize=1000)
print "success points"
# '''

'''
#Table Route_point_Seq
# CREATE TABLE `Route_point_seq` (
  # `agency_id` int(10) unsigned NOT NULL,
  # `route_short_name` varchar(255) NOT NULL,
  # `route_dir` int(10) unsigned NOT NULL,
  # `pattern_id` varchar(255) NOT NULL,
  # `shape_id` varchar(255) NOT NULL,
  # `point_id` int(10) unsigned NOT NULL,
  # `seq` int(10) unsigned NOT NULL,
  # `length` double NOT NULL,
  # `heading` double NOT NULL,
  # `dist` double NOT NULL,
  # `version` varchar(255) NOT NULL,
  # PRIMARY KEY  (`agency_id`,`route_short_name`,`route_dir`,`pattern_id`,`seq`,`version`)
# ) ENGINE=InnoDB DEFAULT CHARSET=latin1
#finding the set of points that represent a pattern on a route
columns = ['agency_id', 'route_short_name', 'route_dir', 'pattern_id', 'shape_id',
           'point_id', 'seq', 'length', 'heading', 'dist', 'version']
try:
    #What is shapes_df also had the trips it is a part of?
    shapes_df = pd.read_csv("agencies/bart/shapes.txt")

    routes_df = pd.read_csv("agencies/bart/routes.txt")
    trips_df = pd.read_csv("agencies/bart/trips.txt")
    #load the Route_Stop_Seq for miscellaneous things
    login = {'host':"localhost", 'user':"root",
                     'passwd':"root", 'db': "TrafficTransit"}
    route_stop_seq_df = helper.sql2df('Route_stop_seq', login)
except Exception as e:
    print os.getcwd()
    print e

#finds the sequence of coordinates for a specific trip.
#what am i having trouble with? getting the shape_id from the table of points.
tables['Route_Point_Seq'] = pd.DataFrame()
counter = 0
for a, row in routes_df.iterrows(): #iterate through the different routes
    if (counter > 1000):
        break
    route_id = row['route_id']
    print route_id
    patterns = [] #the patterns
    for b, subrow in trips_df.loc[trips_df['route_id'] == route_id].iterrows():
        if(counter > 1000):
            break
        trip_id = subrow['trip_id']
        shape_id = subrow['shape_id'] #get the shape_id of the specific trip
        direction_id = subrow['direction_id'] if 'direction_id' in subrow else 0 #get the direction id in the trip
        shape_id_block = shapes_df.loc[shapes_df['shape_id'] == shape_id]
        distanceSinceStart = 0 #keep track of distance since start of the trip
        lastPoint = None
        for c, subsubrow in shape_id_block.iterrows():
            new_row = {}
            new_row['trip_id'] = trip_id
            new_row['agency_id'] = agency_id
            new_row['route_short_name'] = str(helper.optional_field(a, 'route_long_name', routes_df))
            new_row['route_dir'] = direction_id
            # new_row['pattern_id'] = pattern_id
            new_row['shape_id'] =  shape_id
            new_row['shape_pt_sequence'] = subsubrow['shape_pt_sequence']
            new_row['shape_id'] = subsubrow['shape_id']
            #how to calculate the length
            currentLon = subsubrow['shape_pt_lon']
            currentLat = subsubrow['shape_pt_lat']

            if lastPoint != None:
                new_row['length'] = helper.coordToMiles(currentLat, currentLon, lastPoint['current_lat'], lastPoint['current_lon'])
                distanceSinceStart += new_row['length']
                #add the length for the cumumulative distance
                new_row['dist'] = distanceSinceStart

                new_row['heading'] = helper.calculate_heading(currentLat, currentLon, lastPoint['current_lat'],lastPoint['current_lon'])
                lastPoint = {'current_lat': currentLat, 'current_lon': currentLon}
            else: #the case where we just start the way point
                new_row['length']  = 0 # seq i - seq i distance is 0
                new_row['dist'] = 0
                lastPoint = {'current_lat': currentLat, 'current_lon': currentLon}
                new_row['heading'] = "N/A" #the first point doesn't have any heading

            new_row['version'] = 1
            counter += 1
            print counter
            tables["Route_Point_Seq"] = tables["Route_Point_Seq"].append(pd.Series(new_row), ignore_index=True)

print tables["Route_Point_Seq"]
db = MySQLdb.connect(host="localhost", user="root",
                     passwd="root", db="TrafficTransit")
tables['Route_Point_Seq'].to_sql(con=db, flavor='mysql', name="Route_Point_Seq", if_exists="replace", index=False)
'''

### ----- Task 2  ----------
# Table Transfers
# Table schema:
# CREATE TABLE `Transfers` (
#   `from_agency_id` int(11) NOT NULL default '-1',
#   `from_id` int(10) unsigned NOT NULL,
#   `to_agency_id` int(11) NOT NULL default '-1',
#   `to_id` int(10) unsigned NOT NULL,
#   `transfer_type` int(10) unsigned NOT NULL,
#   `min_transfer_time` int(10) unsigned NOT NULL,
#   `transfer_dist` int(11) NOT NULL default '0',
#   PRIMARY KEY  (`from_agency_id`,`from_id`,`to_agency_id`,`to_id`)
# ) ENGINE=InnoDB DEFAULT CHARSET=latin1
# Source: GTFS transfers.txt, Route_stop_seq, Stops
'''
try:
    #What is shapes_df also had the trips it is a part of?
    stops_df = pd.read_csv("agencies/bart/stops.txt")
except Exception as e:
    print os.getcwd()
    print e

tables["Transfers"] = pd.DataFrame()
columns = ['from_agency_id', 'from_id', 'to_agency_id', 'to_id', 'transfer_type',
           'min_transfer_time', 'transfer_dist']
max_distance = 3
#initiate googlemaps for finding minimum transfer time and transfer distance
gmaps = googlemaps.Client(key='AIzaSyB_yzsaBUOOo3ukoeDvtjg5Q32IGSkBUvU')

for a, row in stops_df.iterrows():
    from_id = row['stop_id']
    stops_in_range = helper.find_nearby_stops(from_id, stops_df, max_distance)
    for a, subrow in stops_in_range.iterrows():
        new_row = {}
        new_row['from_agency_id'] = 1 #bart
        new_row['from_id'] = from_id
        new_row['to_agency_id'] = 1
        new_row['to_id'] =  subrow['stop_id']
        new_row['tranfer_type'] = 0

        #how to get the minimum transfer time
        result = helper.google_walking_distance_time(row['stop_lat'], row['stop_lon'],
                                                       subrow['stop_lat'], subrow['stop_lon'])
        distance = result['distance']
        time = result['duration']
        print distance
        print time
        new_row['min_transfer_time'] = time
        #distance returned by google maps
        new_row['tranfer_dist'] = distance
        tables["Transfers"] = tables["Transfers"].append(pd.Series(new_row), ignore_index=True)
print tables["Transfers"]
db = MySQLdb.connect(host="localhost", user="root",
                     passwd="root", db="TrafficTransit")
tables['Transfers'].to_sql(con=db, flavor='mysql', name="Transfers", if_exists="replace", index=False)
'''






### ---- Task 3 -----------------
#first sample just using VTA
'''
columns = ['agency_id', 'veh_id', 'RecordedDate', 'RecordedTime', 'UTC_at_date', 'latitude',
           'longitude', 'speed', 'course']
try:
    feed = gtfs_realtime_pb2.FeedMessage()
    response = urllib.urlopen('http://api.transitime.org/api/v1/key/5ec0de94/agency/vta/command/gtfs-rt/vehiclePositions')
    feed.ParseFromString(response.read())
except Exception as e:
    print e

tables['gps_fixes'] = pd.DataFrame()
for entity in feed.entity:
    new_row = {}
    new_row['agency_id'] = 11 #VTA
    new_row['veh_id'] = entity.id
    new_row['RecordedDate'] = str(datetime.datetime.now().strftime('%Y-%m-%d'))
    new_row['RecordedTime'] = str(datetime.datetime.now().strftime('%H:%M:%S'))
    new_row['UTC_at_date'] = str(datetime.datetime.now().strftime('%Y-%m-%d'))
    new_row['UTC_at_time'] = str(datetime.datetime.now().strftime('%H:%M:%S'))
    new_row['latitude'] = entity.vehicle.position.latitude
    new_row['longitude'] = entity.vehicle.position.longitude
    new_row['speed'] = entity.vehicle.position.speed
    new_row['course'] = "N/A"
    tables["gps_fixes"] = tables["gps_fixes"].append(pd.Series(new_row), ignore_index=True)
db = MySQLdb.connect(host="localhost", user="root",
                    passwd="root", db="TrafficTransit")
tables['gps_fixes'].to_sql(con=db, flavor='mysql', name="gps_fixes", if_exists="replace", index=False)
'''


# Table schema:
# CREATE TABLE `TransitETA` (
#   `agency_id` int(10) unsigned NOT NULL,
#   `RecordedDate` date NOT NULL,
#   `RecordedTime` time NOT NULL,
#   `veh_id` int(11) NOT NULL,
#   `veh_lat` double NOT NULL,
#   `veh_lon` double NOT NULL,
#   `veh_speed` double NOT NULL,
#   `veh_location_time` bigint(20) NOT NULL,
#   `route_short_name` varchar(255) NOT NULL,
#   `route_dir` int(10) unsigned NOT NULL,
#   `day` char(7) NOT NULL,
#   `run` int(10) unsigned NOT NULL,
#   `pattern_id` varchar(255) NOT NULL,
#   `top_id` int(10) unsigned NOT NULL,
#   `seq` int(10) unsigned NOT NULL,
#   `ETA` time NOT NULL,
#   PRIMARY KEY  USING BTREE (`agency_id`,`veh_id`,`RecordedDate`,`RecordedTime`,`route_short_name`,`route_dir`,`day`,`run`,`seq`,`ETA`),
#   KEY `route_short_name` (`route_short_name`),
#   KEY `RecordedDate` (`RecordedDate`),
#   KEY `stop_id` (`stop_id`),
# ) ENGINE=InnoDB DEFAULT CHARSET=latin1
columns = ['agency_id', 'RecordedDate', 'RecordedTime', 'veh_id', 'veh_lat', 'veh_lon',
           'veh_speed', 'veh_location_time', 'route_short_name', 'route_dir',
           'day', 'run', 'pattern_id', 'stop_id', 'seq', 'ETA']
try:
    feed = gtfs_realtime_pb2.FeedMessage()
    response = urllib.urlopen('http://api.transitime.org/api/v1/key/5ec0de94/agency/vta/command/gtfs-rt/vehiclePositions')
    feed.ParseFromString(response.read())
except Exception as e:
    print e



