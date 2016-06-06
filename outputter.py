import MySQLdb

db = MySQLdb.connect(host="localhost", user="root",
                    passwd="berkeleypath", db="TrafficTransit")

#prepare a cursor to pull the data
cursor = db.cursor()


# Static Feed
#  ---- Task 1 ----
#  Agency
#  int agency_id -> 'agency_id' int(10) unsigned
# required string agency_name  -> 'agency_name' varchar(255)
# required string agency_url -> 'agency_url' varchar(255)
# required string agency_timezone -> 'agency_timezone' smallint(6)
# optional string agency_lang -> 'agency_lang' varchar(255)
# optional string agency_phone -> 'agency_phone' varchar(255)
# required string agency_timezone -> 'timezone_name' varchar(45)
# PRIMARY KEY ('agency_id')
# KEY ('agency_timezone')

#drop Agency Table if it exists
cursor.execute("DROP TABLE IF EXISTS AGENCY")

#create the table
sql = """CREATE TABLE AGENCY(
         agency_id int(10) unsigned NOT NULL,
         agency_name varchar(255) NOT NULL,
         agency_url varchar(255) NOT NULL,
         agency_timezone smallint(6) NOT NULL,
         agency_lang varchar(255) NOT NULL,
         agency_phone varchar(255) NOT NULL,
         timezone_name varchar(45) NOT NULL,
         PRIMARY KEY(agency_id)
         )"""

sql_insertion = sql2 = "INSERT INTO AGENCY(agency_id, \
    agency_name, agency_url, agency_timezone, agency_lang, agency_phone, " \
                       "timezone_name) \
    VALUES ('%s', '%s', '%d', '%c', '%d' )" % \
      ('Second', 'Employee', 21, 'M', 3000)
try:
   # Execute the SQL command
   cursor.execute(sql)
   # Commit your changes in the database
   db.commit()
except:
   # Rollback in case there is any error
   db.rollback()

