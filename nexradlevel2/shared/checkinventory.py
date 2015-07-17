import sqlite3
import sys

db_name = 'nexradl2.db'

#
#  sys.argv[1]: dbname
#  sys.argv[2]: inventory.csv
#

if len(sys.argv) > 1:
  print sys.argv
  db_name = sys.argv[1]

if len(sys.argv) > 2:
  inventory_name = sys.argv[2]

print db_name
print inventory_name

conn = sqlite3.connect(db_name)
c = conn.cursor()

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
         rows = c.execute("SELECT size FROM files where path='%s' " % path )
       except:
         #print "select exception %s " % path
         print "exception,%s,%s,%s," % (size,0,path)
       #print rows
       data=c.fetchall()
       #print data
       if len(data) ==0:
          #print "=== file missing [%s]"%path
          print "missing,%s,0,%s," % (size,path)
       else:
         sizedb = data[0][0]
         if (int(size) != int(sizedb)):
          #print "===size doesn't match inventory %s db %s [%s]" % (size,sizedb,path)
          print "size,%s,%s,%s," % (size,sizedb,path)

