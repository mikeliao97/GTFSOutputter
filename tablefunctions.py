import MySQLdb
import os
import pandas as pd
import numpy as np
import pytz
import MySQLdb
from google.transit import gtfs_realtime_pb2
import urllib
import datetime
import helper
import csv

def agencies(tables, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, agency_id):
    columns = ['agency_id', 'agency_name', 'agency_url', 'agency_timezone', 'agency_lang',
           'agency_phone', 'timezone_name']
    tables['Agency'] = pd.DataFrame(index=np.r_[0:len(static_feed['agency'].index)], columns=columns)
    for i, row in static_feed['agency'].iterrows():
        new_row = tables["Agency"].loc[i] #instantiate a NEW ROW
        new_row['agency_id'] = agency_id
        new_row['agency_name'] = row['agency_name']
        new_row['agency_url'] = row['agency_url']
        timezone = pytz.timezone(row['agency_timezone'])
        new_row['agency_timezone'] = timezone
        new_row['agency_lang'] = helper.optional_field(i, 'agency_lang', static_feed['agency'])
        new_row['agency_phone'] = helper.optional_field(i, 'agency_phone', static_feed['agency'])
        new_row['timezone_name'] = row['agency_timezone']
    helper.write_table(tables, 'Agency')
    print "SUCCESS finished with agencies"

def stops(tables, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, agency_id):
    columns = ['agency_id', 'stop_id', 'stop_code', 'stop_name', 'stop_desc', 'stop_lat', 'stop_lon',
               'lat_lon', 'stop_url', 'location_type', 'parent_station', 'wheelchair_boarding', 'version']
    tables['Stops'] = pd.DataFrame(index=np.r_[0:len(static_feed['stops'].index)], columns=columns)
    for i, row in static_feed['stops'].iterrows():
        new_row = tables["Stops"].loc[i] #instantiate a NEW ROW
        new_row['agency_id'] = 1
        new_row['stop_id'] = str(row['stop_id'])
        new_row['stop_code'] = str(helper.optional_field(i, 'stop_code', static_feed['stops']))
        new_row['stop_name'] = str(row['stop_name'])
        new_row['stop_desc'] = str(helper.optional_field(i, 'stop_desc', static_feed['stops']))
        new_row['stop_lat'] = float(row['stop_lat'])
        new_row['stop_lon'] = float(row['stop_lon'])
        new_row['lat_lon'] = 0 # some calculations, ignore until using MySQL
        new_row['stop_url'] = str(helper.optional_field(i, 'stop_url', static_feed['stops']))
        new_row['location_type'] = int(helper.optional_field(i, 'location_type', static_feed['stops'], 0))
        new_row['parent_station'] = int(helper.optional_field(i, 'parent_station', static_feed['stops'], 0))
        new_row['wheelchair_boarding'] = int(helper.optional_field(i, 'wheelchair_boarding', static_feed['stops'], 0))
    helper.write_table(tables, 'Stops')
    print "SUCCESS with stops"

def routes(tables, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, agency_id):
    columns = ['agency_id', 'route_short_name', 'route_dir',
               'route_type', 'route_long_name', 'route_desc',
               'route_url', 'route_color', 'route_text_color', 'route_id', 'version']
    tables['Routes'] = pd.DataFrame()
    for i, row in static_feed['routes'].iterrows():
        for direction_id in static_feed['trips'].loc[static_feed['trips']['route_id'] == row['route_id']]['direction_id'].unique():
            new_row = {}
            new_row['agency_id'] = 1
            new_row['route_short_name'] = str(helper.optional_field(i, 'route_short_name', static_feed['routes'], static_feed['routes'].iloc[i]['route_long_name']))
            new_row['route_dir'] = direction_id
            new_row['route_type'] = int(row['route_type'])
            new_row['route_long_name'] = str(helper.optional_field(i, 'route_long_name', static_feed['routes'], static_feed['routes'].iloc[i]['route_short_name']))
            new_row['route_desc'] = helper.optional_field(i, 'route_desc', static_feed['routes'])
            new_row['route_url'] = helper.optional_field(i, 'route_url', static_feed['routes'])
            new_row['route_color'] = helper.optional_field(i, 'route_color', static_feed['routes'], default='FFFFFF').upper()
            new_row['route_text_color'] = helper.optional_field(i, 'route_text_color', static_feed['routes'], default='000000').upper()
            new_row['route_id'] = str(row['route_id'])
            new_row['version'] = 1
            tables['Routes'] = tables['Routes'].append(pd.Series(new_row), ignore_index=True)
    helper.write_table(tables, 'Routes')
    print "SUCCESS with routes"

def route_stop_seq(tables, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, agency_id, trip2pattern):
    count = 0
    columns = ['agency_id', 'route_short_name', 'route_dir',
               'pattern_id', 'stop_id', 'seq', 'is_time_point', 'version', 'trip_id']

    tables['Route_stop_seq'] = pd.DataFrame()
    for i, row in static_feed['routes'].iterrows(): #iterate through the different routes
        route_id = row['route_id']
        patterns = [] #the patterns
        if (count > 10000):
            break
        for j, subrow in static_feed['trips'].loc[static_feed['trips']['route_id'] == route_id].iterrows():
            trip_id = subrow['trip_id'] #get the trip_id
            print trip_id
            direction_id = subrow['direction_id'] if 'direction_id' in subrow else 0 #get the direction id in the trip
            trip_id_block = static_feed['stop_times'].loc[static_feed['stop_times']['trip_id'] == trip_id]
            sequence = trip_id_block['stop_id'].tolist()
            if str(sequence) not in patterns:
                patterns += [str(sequence)]
            pattern_num = patterns.index(str(sequence)) + 1
            route_short_name = str(helper.optional_field(i, 'route_long_name', static_feed['routes']))
            pattern_id = "{0}_{1}_{2}".format(route_short_name, direction_id, pattern_num)
            if (count > 10000):
                break
            for k, subsubrow in trip_id_block.iterrows():
                new_row = {}
                new_row['trip_id'] = trip_id
                new_row['agency_id'] = agency_id
                new_row['route_short_name'] = route_short_name
                new_row['route_dir'] = direction_id
                new_row['pattern_id'] = pattern_id
                new_row['stop_id'] = str(subsubrow['stop_id'])
                new_row['seq'] = subsubrow['stop_sequence']
                new_row['is_time_point'] = int(helper.optional_field(k, 'timepoint', static_feed['stop_times'], 0))
                new_row['version'] = 1; #replace later
                tables["Route_stop_seq"] = tables["Route_stop_seq"].append(pd.Series(new_row), ignore_index=True)
                count += 1
            trip2pattern[trip_id] = pattern_id

    with open('Trip2Pattern.csv', 'wb') as f:
        writer = csv.writer(f)
        writer.writerow(["trip_id", "pattern_id"])
        for key, value in trip2pattern.items():
            writer.writerow([key, value])
    helper.write_table(tables, 'Route_stop_seq')
    print "SUCCESS with Route Stop Seq"

def runPattern(tables, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, agency_id, trip2pattern):
    columns = ['agency_id', 'route_short_name', 'start_date', 'end_date', 'service_id', 'day', 'route_dir', 'run', 'pattern_id', 'trip_headsign', 'trip_id', 'version']
    tables['RunPattern'] =  pd.DataFrame(index=np.r_[0:len(static_feed['trips'].index)], columns=columns)
    run_count = {}
    day_count = {}
    for i, row in static_feed['trips'].iterrows():
        new_row = tables["RunPattern"].loc[i]
        new_row['agency_id'] = 1
        j = np.where(static_feed['routes']['route_id'] == row['route_id'])[0][0]
        new_row['route_short_name'] = str(helper.optional_field(j, 'route_short_name', static_feed['routes'], static_feed['routes'].iloc[j]['route_long_name']))
        new_row['service_id'] = row['service_id']
        calendar = static_feed['calendar'].loc[static_feed['calendar']['service_id'] == row['service_id']].iloc[0]
        new_row['start_date'] = datetime.datetime.strptime(str(calendar['start_date']), "%Y%m%d")
        new_row['end_date'] = datetime.datetime.strptime(str(calendar['end_date']), "%Y%m%d")
        new_row['route_dir'] = int(helper.optional_field(i, 'direction_id', static_feed['trips'], 0))
        new_row['day'] = "{0}{1}{2}{3}{4}{5}{6}".format(calendar['monday'], calendar['tuesday'], calendar['wednesday'], calendar['thursday'], calendar['friday'], calendar['saturday'], calendar['sunday'])
        if new_row['day'] not in day_count:
            day_count[new_row['day']] = 1
        if new_row['route_short_name'] not in run_count.keys():
            run_count[new_row['route_short_name']] = {new_row['service_id']: {new_row['route_dir'] : 1}}

        if new_row['service_id'] not in run_count[new_row['route_short_name']].keys():
            run_count[new_row['route_short_name']] = {new_row['service_id']: {new_row['route_dir'] : 1}}

        if new_row['route_dir'] not in run_count[new_row['route_short_name']][new_row['service_id']].keys():
            run_count[new_row['route_short_name']] = {new_row['service_id']: {new_row['route_dir'] : 1}}

        new_row['run'] = run_count[new_row['route_short_name']][new_row['service_id']][new_row['route_dir']]
        run_count[new_row['route_short_name']][new_row['service_id']][new_row['route_dir']] += 1 #increment the run because we've seen this before....

        new_row['pattern_id'] = str(trip2pattern[trip2pattern['trip_id'] == row['trip_id']].iloc[0]['pattern_id'])

        new_row['trip_headsign'] = helper.optional_field(i, 'trip_headsign', static_feed['trips'], stop_times_df.loc[stop_times_df['trip_id'] == row['trip_id']]['stop_headsign'].iloc[0])
        new_row['trip_id'] = str(row['trip_id'])
        new_row['version'] = 1
    db = MySQLdb.connect(host="localhost", user="root",
                         passwd="root", db="TrafficTransit")
    tables['RunPattern'].to_sql(con=db, flavor='mysql', name="RunPattern", if_exists="replace", index=False, chunksize=1000)
    print "success"
