from azure.storage import BlobService
from azure import WindowsAzureError
import sqlite3
import os
import sys


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

conn = sqlite3.connect(db_name)
c = conn.cursor()

AZURE_STORAGE_CONNECTION_STRING = os.environ['AZURE_STORAGE_CONNECTION_STRING']


blob_service = BlobService(connection_string=AZURE_STORAGE_CONNECTION_STRING)


q = []
query = "SELECT * FROM files"
if quick == True:
  query = "SELECT * FROM files where azure=''"

for row in c.execute(query):
   print row
   # check to see if this file is in azure, and that the file sizes match
   try:
     item = blob_service.get_blob_properties('nexradl2',row[0])
     print item['content-length']
     print row[1]
     if (int(item['content-length']) == int(row[1])):
       print "item matches"
       q.append("update files set azure='yes' where path = '%s'" % (row[0]))
     else:
       q.append("update files set azure='' where path = '%s'" % (row[0]))
   except WindowsAzureError as ex:
      q.append("update files set azure='' where path = '%s'" % (row[0]))
      print "exception: %s" % ex


   #print item

print q
for sql in q:
  c.execute(sql)

conn.commit()
conn.close()
exit()

