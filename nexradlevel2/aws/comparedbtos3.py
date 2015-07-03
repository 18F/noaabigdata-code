import boto
import os
import sys
from filechunkio import FileChunkIO
from boto.s3.connection import S3Connection
import sqlite3

def convertToS3Key(name):
  #new_key =  /<Year>/<Month>/<Day>/<Nexrad Station>/ <filename>  NWS_NEXRAD_NXL2LG_KAKQ_20010101080000_20010101155959.tar
  parts = name.split("/")
  #print  parts
  year = parts[1][0:4]
  #print  year
  month = parts[1][4:6]
  #print  month
  day = parts[1][6:8]
  #print  day
  station = parts[2]
  #print  station
  fname = os.path.basename(name)
  new_key =  u"%s/%s/%s/%s/%s" % (year,month,day,station,fname)
  return new_key

db_name = "nexradl2.db"
quick = False


#
#  sys.argv[1]: dbname
#

if len(sys.argv) > 1:
  print sys.argv
  db_name= sys.argv[1]
if len(sys.argv) > 2:
  if sys.argv[2] == "quick":
    quick = True

print db_name


conn = S3Connection()

bucket = conn.get_bucket('noaa-nexrad-l2')



conn = sqlite3.connect(db_name)
c = conn.cursor()

q = []
query = "SELECT * FROM files"
if quick == True:
  query = "SELECT * FROM files where aws=''"

for row in c.execute(query):
   print row
   # check to see if this file is in aws, and that the file sizes match
   try:
     keyname = convertToS3Key(row[0])
     print keyname
     key = bucket.lookup(keyname)
     print key
     print key.size
     print row[1]
     if (int(key.size) == int(row[1])):
       print "item matches"
       q.append("update files set aws='yes' where path = '%s'" % (row[0]))
     else:
       q.append("update files set aws='' where path = '%s'" % (row[0]))
   except Exception, err:
      q.append("update files set aws='' where path = '%s'" % (row[0]))
      print "exception %s" % err


   #print item

print q
for sql in q:
  c.execute(sql)

conn.commit()
conn.close()
exit()

