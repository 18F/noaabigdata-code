import shlex
import subprocess
import sqlite3


#
# open connection to nexrad DB
#
conn = sqlite3.connect('nexradl2-realtime.db')
c = conn.cursor()


#
# First put the files into the database 
#

command_line='find  /export/brick-headnode-1/brick/anon-ftp/nmqtransfer/latest-data/ -printf "%h/%f,%s\n"'
args = shlex.split(command_line)
proc = subprocess.Popen(args, stdout=subprocess.PIPE)
(out, err) = proc.communicate()
#print "program output:", out
lines = out.split('\n')
#print lines
for line in lines:
     #print line
     try:
       com = line.strip().split(',')
       res = com[0].strip().split('/')
       size= com[1]
       #print "res:"
       #print res
       #print "size:"
       #print size
       if len(res) == 8:
          name = res[7]
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
          #print "INSERT INTO files VALUES ('%s','%s','','','','','')" % (path,size)
          c.execute("INSERT INTO files VALUES ('%s','%s','','','','','')" % (path,size))
       else:
         print "error:"
         print line
         print len(res)
     except:
         print "except"
         print line

# Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()

#
#  loop through the database and upload the files to each provider
#

# shell out to the standard uploader?
#
