import sqlite3
from Queue import Queue
from threading import Thread
import threading
import time
import os
import subprocess
import signal
import sys
import math
import boto
from filechunkio import FileChunkIO

from boto.s3.connection import S3Connection
conn = S3Connection(is_secure=False)
bucket = conn.get_bucket('noaa-nexrad-l2')

radar_path_name = "/snfs9/q2/levelii_tarfiles/"
db_name = "nexradl2aws.db"
nopath = False

#
#  sys.argv[1]: source path
#  sys.argv[2]: dbname
#  sys.argv[3]: nopath
#

if len(sys.argv) > 1:
  print sys.argv
  radar_path_name = sys.argv[1]
if len(sys.argv) > 2:
  db_name = sys.argv[2]
if len(sys.argv) > 3:
  if sys.argv[3] == "nopath":
    nopath = True

print radar_path_name
print db_name
print nopath



#####
#num_worker_threads=1
#num_worker_threads=50
num_worker_threads=100
conn = sqlite3.connect(db_name,check_same_thread=False)
c = conn.cursor()




def upload_file(i,item):
  print "dealing with: %s" % i
  print item

  source_path = radar_path_name  % item[0]

  filename = item[0]
  if nopath == True:
     #we need to change the path around
     print(item[0].split("/"))
     parts = item[0].split("/")
     filename = parts[-1]

  source_path = radar_path_name + filename
  source_size = os.stat(source_path).st_size
  #new_key =  /<Year>/<Month>/<Day>/<Nexrad Station>/ <filename>  NWS_NEXRAD_NXL2LG_KAKQ_20010101080000_20010101155959.tar
  parts = item[0].split("/")
  #print  parts
  year = parts[1][0:4]
  #print  year
  month = parts[1][4:6]
  #print  month
  day = parts[1][6:8]
  #print  day
  station = parts[2]
  #print  station
  fname = os.path.basename(source_path)
  new_key =  u"%s/%s/%s/%s/%s" % (year,month,day,station,fname)
  #a = new_key.split("/")
  #print a
  #print  "[%s]" % item[0] 
  #print  "[%s]" % new_key
  #boto.set_stream_logger('boto')
  #mp = bucket.initiate_multipart_upload(os.path.basename(source_path))
  print "before bucket"
  mp = bucket.initiate_multipart_upload(new_key)
  #mp = bucket.initiate_multipart_upload(item[0])
  print "after bucket"
  chunk_size = 52428800
  chunk_count = int(math.ceil(source_size / float(chunk_size)))
  print source_path
  print chunk_count
  # Send the file parts, using FileChunkIO to create a file-like object
  # that points to a certain byte range within the original file. We
  # set bytes to never exceed the original file size.
  for i in range(chunk_count):
    offset = chunk_size * i
    bytes = min(chunk_size, source_size - offset)
    with FileChunkIO(source_path, 'r', offset=offset,
                         bytes=bytes) as fp:
      mp.upload_part_from_file(fp, part_num=i + 1)

  # Finish the upload
  mp.complete_upload()
  print "finished uploading %s " % source_path

  return 1


def dbworker(i,dbq):
   print "DBQ worker" 
   while True:
    item = dbq.get()
    print "DBQ dealing with: " 
    print item
    c.execute("update files set aws='yes' where path = '%s'" % (item[0]))
    conn.commit()
    dbq.task_done()
    

def worker(i,q,dbq):
   while True:
    item = q.get()
    if upload_file(i,item) == 1:
      dbq.put(item)
    q.task_done()

q = Queue()
dbq = Queue()
for i in range(num_worker_threads):
  t = Thread(target=worker, args=(i, q,dbq))
  t.daemon = True
  t.start()

dbt = Thread(target=dbworker, args=(0,dbq))
dbt.daemon = True

#for row in c.execute("SELECT * FROM files where azure=='' limit 100"):
for row in c.execute("SELECT * FROM files where aws=='' "):
  q.put(row)

dbt.start()
#for item in source():
#  q.put(item)

q.join()       # block until all tasks are done
print "after q.join"
dbq.join()
print "after dbq.join"
conn.close()

