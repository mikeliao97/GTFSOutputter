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
    tablefunctions.runPattern(tables=tables, static_feed=static_feed, agency_id=agency_id)


    ###Schedules: gets some data from RunPattern table
    tablefunctions.schedules(tables, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, agency_id=agency_id, trip2pattern=trip2pattern)

    #Table Points
    tablefunctions.points(tables, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, agency_id=agency_id, trip2pattern=trip2pattern)

    #Table Route_point_Seq
    tablefunctions.route_point_seq(tables, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, agency_id=agency_id, trip2pattern=trip2pattern)

    ### ----- Task 2  ----------
    #Table Transfers
    tablefunctions.transfers(tables, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, agency_id=agency_id, trip2pattern=trip2pattern)


    ### ---- Task 3 -----------------
    #Table GPS FIXES
    tablefunctions.gps_fixes(tables, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, agency_id=agency_id, trip2pattern=trip2pattern)

    #Table Transit ETA
    tablefunctions.transit_eta(tables, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, agency_id=agency_id, trip2pattern=trip2pattern)


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