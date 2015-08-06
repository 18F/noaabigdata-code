import sqlite3
import sys

db_name = 'nexradl2.db'
filename = "i3.txt"
#filename = "2012-a.txt"
#filename = "2014-2.txt"

#
#  sys.argv[1]: dbname
#  sys.argv[2]: dbname
#  sys.argv[3]: nopath
#

if len(sys.argv) > 1:
  print sys.argv
  db_name = sys.argv[1]
if len(sys.argv) > 2:
  filename = sys.argv[2]

print db_name

conn = sqlite3.connect(db_name)
c = conn.cursor()


#open the file list and look for files

# Insert a row of data

with open(filename) as f:
  for line in f:
     com = line.strip().split(',')
     res = com[0].strip().split('/')
     size= com[1]
     #print "res:"
     #print res
     #print "size:"
     #print size
     if len(res) == 8:
       path = res[4]+'/'+res[5]+'/'+res[6]+'/'+res[7]
       #print path 
       #print "INSERT INTO files VALUES ('%s','%s','','','','','')" % (path,size)
       #
       # insert this entry into the DB - it will fail if it is already in the DB
       #
       try:
          c.execute("INSERT INTO files VALUES ('%s','%s','','','','','')" % (path,size))
       except:
          # this failure is ok, if it is already in the DB
          print "insert exception - already in db?"
          # 
          # check to see if the row matches the size, ie: if there was a partial file in the path last time, we should
          # double check
          try:
            rows = c.execute("SELECT size FROM files where path='%s' " % path )
          except:
            print "select exception"
          sizedb = c.fetchone()[0]
          #print "size %s %s " % (size,sizedb)
          if (int(size) != int(sizedb)):
             print "size doesn't match %s %s" % (size,sizedb)
             # update file set the size regardless.. if it is bigger maybe? hmm.. sql?
             c.execute("update files set size='%s',azure='', aws='' where path='%s' " % (size,path))

     else:
       print len(res)
     

# Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()

