import shlex
import subprocess
import sqlite3
import uuid
import os


#
# open connection to nexrad DB
#
dbfilename = 'nexradl2-realtime.db'

conn = sqlite3.connect(dbfilename)
c = conn.cursor()


#
# First put the files into the database 
#

command_line='find  /export/brick-headnode-1/brick/anon-ftp/nmqtransfer/latest-data/ -printf "%h/%f,%s\n"'
print command_line
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
          try:
            c.execute("INSERT INTO files VALUES ('%s','%s','','','','','')" % (path,size))
          except:
            print "insert exception"
          #print "here"
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
#  check the database against what is already in the cloud.. make sure the file sizes are correct
#


#
# MS
#
command_line = "python ../ms/comparedbtoazure.py "+dbfilename+" quick"
print command_line
args = shlex.split(command_line)
proc = subprocess.Popen(args, stdout=subprocess.PIPE)
(out, err) = proc.communicate()
print out

#
# AWS
#
command_line = "python ../aws/comparedbtos3.py "+dbfilename+" quick"
print command_line
args = shlex.split(command_line)
proc = subprocess.Popen(args, stdout=subprocess.PIPE)
(out, err) = proc.communicate()
print out


#
#  loop through the database and upload the files to each provider
#

# copy the database to the queue for each provider
unique_filename = uuid.uuid4()
print unique_filename

awsname = "aws_"+str(unique_filename)+".db"
msname = "ms_"+str(unique_filename)+".db"
import shutil



shutil.copyfile(dbfilename, awsname)
shutil.copyfile(dbfilename, msname)

#
# shell out to the standard uploader?
#
#os.spawnl(os.P_DETACH, 'some_long_running_command')
#  sys.argv[1]: source path
#  sys.argv[2]: dbname

mscommand = "python ../ms/multithreadazure_builtin.py /export/brick-headnode-1/brick/anon-ftp/nmqtransfer/latest-data/ "+msname+" nopath"
print mscommand
awscommand = "python ../aws/multithreads3.py /export/brick-headnode-1/brick/anon-ftp/nmqtransfer/latest-data/ "+awsname+" nopath"
print awscommand
#os.spawnl(os.P_NOWAIT, mscommand)

