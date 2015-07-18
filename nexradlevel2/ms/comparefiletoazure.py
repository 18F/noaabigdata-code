from azure.storage import BlobService
from azure import WindowsAzureError
import os

import sys


#
#  sys.argv[1]: inventory.csv
#

if len(sys.argv) > 1:
  inventory_name = sys.argv[1]

AZURE_STORAGE_CONNECTION_STRING = os.environ['AZURE_STORAGE_CONNECTION_STRING']
blob_service = BlobService(connection_string=AZURE_STORAGE_CONNECTION_STRING)


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
   # check to see if this file is in azure, and that the file sizes match
       try:
         item = blob_service.get_blob_properties('nexradl2',path)
         #print item['content-length']
         #print size 
         if (int(item['content-length']) == int(size)):
           a =1 
           #print "item matches"
         else:
           print "size,%s,%s,%s," % (size,item['content-length'],path)
       except WindowsAzureError as ex:
          #print "exception: %s" % ex
          print "missing,%s,0,%s," % (size,path)
