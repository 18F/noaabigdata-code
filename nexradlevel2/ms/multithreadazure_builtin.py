import sqlite3
from Queue import Queue
from threading import Thread
import threading
import time
import os
import subprocess
import signal
import sys
from azure.storage import BlobService
from azure import WindowsAzureError


radar_path_name = "/snfs9/q2/levelii_tarfiles/"
db_name = "nexradl2.db"
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

AZURE_STORAGE_CONNECTION_STRING = os.environ['AZURE_STORAGE_CONNECTION_STRING']

blob_service = BlobService(connection_string=AZURE_STORAGE_CONNECTION_STRING)


#####
#num_worker_threads=50
#num_worker_threads=10
num_worker_threads=20
#num_worker_threads=100
conn = sqlite3.connect(db_name,check_same_thread=False)
c = conn.cursor()


class Command(object):
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None

    def run(self, timeout):
        result = 1
        def target():
            #print 'Thread started'
            self.process = subprocess.Popen(self.cmd, shell=True,preexec_fn=os.setsid)
            self.process.communicate()
            #print 'Thread finished'

        thread = threading.Thread(target=target)
        thread.start()

        thread.join(timeout)
        if thread.is_alive():
            print 'Terminating process'
            try:
              os.killpg(self.process.pid, signal.SIGTERM)
              #self.process.terminate()
              thread.join()
            except:
              print "exception?"
            result = -1
        #print self.process.returncode
        return result

#command = Command("echo 'Process started'; sleep 2; echo 'Process finished'")
#print command.run(timeout=3)
#print command.run(timeout=1)


def upload_file(i,item):
  print "dealing with: %s" % i
  print item
  filename = item[0]
  if nopath == True:
     #we need to change the path around
     print(item[0].split("/"))
     parts = item[0].split("/")
     filename = parts[-1]
     
  file = radar_path_name + filename
  print file
  try:
    blob_service.put_block_blob_from_path( 'nexradl2', item[0],  file,  max_connections=5,)
    return 1
  except WindowsAzureError as ex:
    print "exception: %s" % ex
    print "exception: %s" % file
    sys.stdout.flush()
    return 0


def dbworker(i,dbq):
   print "DBQ worker" 
   while True:
    item = dbq.get()
    print "DBQ dealing with: " 
    print item
    c.execute("update files set azure='yes' where path = '%s'" % (item[0]))
    conn.commit()
    dbq.task_done()
    sys.stdout.flush()

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
for row in c.execute("SELECT * FROM files where azure=='' "):
  q.put(row)

dbt.start()
#for item in source():
#  q.put(item)

q.join()       # block until all tasks are done
print "after q.join"
dbq.join()
print "after dbq.join"
conn.close()

