import MySQLdb

db = MySQLdb.connect(host="localhost", user="root",
                    passwd="berkeleypath", db="berkeley_path")

#prepare a cursor object using cursor() method
cursor = db.cursor()

#drop table
cursor.execute("DROP TABLE IF EXISTS EMPLOYEE")

#create table
sql = """CREATE TABLE EMPLOYEE (
         FIRST_NAME  CHAR(20) NOT NULL,
         LAST_NAME  CHAR(20),
         AGE INT,
         SEX CHAR(1),
         INCOME FLOAT )"""

cursor.execute(sql)

#try to insert another input
sql = "INSERT INTO EMPLOYEE(FIRST_NAME, \
    LAST_NAME, AGE, SEX, INCOME) \
    VALUES ('%s', '%s', '%d', '%c', '%d' )" % \
      ('Mac', 'Mohan', 20, 'M', 2000)

sql2 =  "INSERT INTO EMPLOYEE(FIRST_NAME, \
    LAST_NAME, AGE, SEX, INCOME) \
    VALUES ('%s', '%s', '%d', '%c', '%d' )" % \
      ('Second', 'Employee', 21, 'M', 3000)

try:
   # Execute the SQL command
   cursor.execute(sql)
   cursor.execute(sql2)
   # Commit your changes in the database
   db.commit()
except:
   # Rollback in case there is any error
   db.rollback()

#READ STUFF NOW
sql = "SELECT * FROM EMPLOYEE WHERE INCOME > '%d'" % (1000)

try:
    #execute teh sql command
    cursor.execute(sql)
    #fetch all the rows in a list of lists
    results = cursor.fetchall()

    for row in results:
        fname = row[0]
        lname = row[1]
        age = row[2]
        sex = row[3]
        income = row[4]
        #Now print the fetched result
        print row
        print "fname=%s,lname=%s,age=%d,sex=%s,income=%d" % \
      (fname, lname, age, sex, income)
except:
    print "Error: Unable to fetch data"

db.close()


