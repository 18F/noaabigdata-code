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

#
#  sys.argv[1]: dbname
#  sys.argv[2]: inventory.csv
#

if len(sys.argv) > 1:
  #print sys.argv
  inventory_name = sys.argv[1]

conn = S3Connection()

bucket = conn.get_bucket('noaa-nexrad-l2')


with open(inventory_name) as f:
  for line in f:
       com = line.strip().split(',')
       name = com[0]
       if name == "#FILENAME":
         continue
       size= com[1]
       #print "com:"
       #print com
       #print "res:"
       #print res
       #print "size:"
       #print size
       #print name
#/200605/20060529/KGRK/NWS_NEXRAD_NXL2LG_KGRK_20060529000000_20060529075959.tar
       parts = name.split('_')
       #print parts
       year = parts[4][0:4]
       #print year
       month = parts[4][4:6]
       #print month
       day = parts[4][6:8]
       #print day
       path = year+month+'/'+year+month+day+'/'+parts[3]+'/'+name
       #print path

       try:
         keyname = convertToS3Key(path)
         #print keyname
         key = bucket.lookup(keyname)
         #print key
         #print key.size
         if (int(key.size) == int(size)):
           #print "item matches"
           match = 1
         else :
           print "size,%s,%s,%s," % (size,key.size,path)
       except Exception, err:
          print "exception %s" % err
          print "exception,%s,%s,%s," % (size,0,path)



