from google.transit import gtfs_realtime_pb2
import urllib

feed = gtfs_realtime_pb2.FeedMessage()
# response = urllib.urlopen('http://api.transitime.org/api/v1/key/5ec0de94/agency/vta/command/gtfs-rt/vehiclePositions')
response = urllib.urlopen('http://api.bart.gov/gtfsrt/tripupdate.aspx')
feed.ParseFromString(response.read())
i = 0
for entity in feed.entity:
    if (i > 5):
        break
    print entity
    # print entity
    i += 1

