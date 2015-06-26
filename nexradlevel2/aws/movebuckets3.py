import boto
import os
from filechunkio import FileChunkIO

from boto.s3.connection import S3Connection
conn = S3Connection()
bucket = conn.get_bucket('noaa.nexradl2')
dstBucket = conn.get_bucket('noaa.nexrad.l2')
rs = bucket.list()

for key in rs:
  print key.name
  #new_key =  /<Year>/<Month>/<Day>/<Nexrad Station>/ <filename>  NWS_NEXRAD_NXL2LG_KAKQ_20010101080000_20010101155959.tar
  parts = key.name.split("/")
  #print  parts
  year = parts[1][0:4]
  #print  year
  month = parts[1][4:6]
  #print  month
  day = parts[1][6:8]
  #print  day
  station = parts[2]
  #print  station
  fname = os.path.basename(key.name)
  new_key =  u"%s/%s/%s/%s/%s" % (year,month,day,station,fname)
  print new_key
  new_key_exists = dstBucket.get_key(new_key,  validate=True)
  if new_key_exists is None:
     print "key doesn't exist, moving:"
     dstBucket.copy_key(new_key, 'noaa.nexradl2', key.name)
