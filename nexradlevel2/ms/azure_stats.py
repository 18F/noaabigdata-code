import subprocess
import threading
from azure.storage import BlobService
import os

AZURE_STORAGE_CONNECTION_STRING = os.environ['AZURE_STORAGE_CONNECTION_STRING']

blob_service = BlobService(connection_string=AZURE_STORAGE_CONNECTION_STRING)

#blobs = blob_service.list_blobs('nexradl2',maxresults=10)
#for blob in blobs:
#    print(blob.name)
#    print(blob.url)
#    #print(blob.properties)
#    print(blob.properties.content_length)

total_length = 0
number = 0
next_marker = None
while True:
    #blobs = blob_service.list_blobs('nexradl2', maxresults=100, marker=next_marker)
    blobs = blob_service.list_blobs('nexradl2', maxresults=5000, marker=next_marker)
    next_marker = blobs.next_marker
    #print(next_marker)
    #print(len(blobs))
    for blob in blobs:
       #print(blob.properties.content_length)
       total_length = total_length + blob.properties.content_length
       number = number + 1
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
