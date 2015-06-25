from azure.storage import BlobService
import sqlite3
import os


conn = sqlite3.connect('nexradl2.db')
c = conn.cursor()

AZURE_STORAGE_CONNECTION_STRING = os.environ['AZURE_STORAGE_CONNECTION_STRING']


blob_service = BlobService(connection_string=AZURE_STORAGE_CONNECTION_STRING)


total_length = 0
number = 0
next_marker = None
while True:
    blobs = blob_service.list_blobs('nexradl2', maxresults=5000, marker=next_marker)
    next_marker = blobs.next_marker
    #print(next_marker)
    print "length of blobs:"
    print(len(blobs))
    newcount = 0
    for blob in blobs:
       path = blob.name
       #print(blob.name)
       #print(blob.properties.content_length)
       c.execute("update files set azure='yes' where path = '%s'" % (path))
       total_length = total_length + blob.properties.content_length
       number = number + 1
       newcount = newcount  + 1
    print "length of newcount:"
    print newcount
    print "so far:"
    print "GB:"
    print total_length / 1024.0 / 1024 / 1024
    print number
    if not next_marker:
        break
print "done"
print total_length
print "GB:"
print total_length / 1024.0 / 1024 / 1024
print number


# Save (commit) the changes
conn.commit()
conn.close()

