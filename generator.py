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
import sys
import logging
import transit_agencies
import tablefunctions

def interpret(agency, static_feed, trip_update_feed, alert_feed, vehicle_position_feed):

    tables = {}
    trip2pattern = {}
    trip2vehicle = {}
    agency_id = transit_agencies.get(agency, "id") #for bart


    #Agencies
    tablefunctions.agencies(tables, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, agency_id=agency_id)



    #Stops
    tablefunctions.stops(tables, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, agency_id=agency_id)


    #Routes
    tablefunctions.routes(tables, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, agency_id=agency_id)


    #Route Stop Seq
    tablefunctions.route_stop_seq(tables, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, agency_id=agency_id, trip2pattern=trip2pattern)


    #Run Pattern
    tablefunctions.runPattern(tables, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, agency_id=agency_id, trip2pattern=trip2pattern)


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




def main(argv):
    agencies = []
    print "Length of args: " + str(len(argv))
    if len(argv) == 1:
        logging.error("Not enough arguments")
        return
    agency = argv[1]

    #get the agencies's informatio
    static_feed = helper.get_static(agency)
    trip_update_feed = helper.get_realtime(agency, mode="trip_update")
    alert_feed = helper.get_realtime(agency, mode="alert")
    vehicle_position_feed = helper.get_realtime(agency, mode="vehicle_position")

    #Process The data
    interpret(agency, static_feed, trip_update_feed, alert_feed, vehicle_position_feed)

if __name__ == "__main__":
    main(sys.argv)