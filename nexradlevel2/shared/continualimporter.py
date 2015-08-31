import shlex
import subprocess
import sqlite3
import uuid
import os
from threading import Thread
import threading
import datetime
import createdatabase
import traceback
import sys


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


#
# open connection to year DB
#
dbfilename = 'nexradl2-years.db'

conn = sqlite3.connect(dbfilename)
c = conn.cursor()


#
# read the years database and get a list of all the years that aren't done (done='no')
#
yearrows = c.execute("SELECT year,path FROM years where done='no' " )
years = []
for yrow in yearrows:
  years.append((yrow[0],yrow[1]))
# Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()





for year,path in years:
  print year
  print path 
  skip = False
  running = 0
  #
  # for each year
  #    - check to see if the database is being used, ie: there are any processes using it
  pids = [pid for pid in os.listdir('/proc') if pid.isdigit()]

  for pid in pids:
    try:
        cmdline = open(os.path.join('/proc', pid, 'cmdline'), 'rb').read()
        if "continualimporter.py" in cmdline:
           running = running + 1
        if "nexradl2-"+str(year)+".db" in cmdline:
            print cmdline
            skip = True 
         
    except IOError: # proc has already terminated
        continue

  if running < 2 and skip == False:


#      - make sure there is a database for each provider
     msdb = "../ms/nexradl2-"+str(year)+".db"
     awsdb = "../aws/nexradl2-"+str(year)+".db"
     if not os.path.isfile(awsdb):
        print "missing "+awsdb
        createdatabase.createdatabase(awsdb)
     if not os.path.isfile(msdb):
        createdatabase.createdatabase(msdb)
        print "missing "+msdb

      #open the databases
     print "opening: "+msdb
     msconn = sqlite3.connect(msdb)
     msc = msconn.cursor()

     print "opening: "+awsdb
     awsconn = sqlite3.connect(awsdb)
     awsc = awsconn.cursor()


#    - grab a list of files
     radarpath = "/snfs9/q2/levelii_tarfiles/"
     if path:
        radarpath = path + "/"
        
     radardatepath = "%s%s??" % (radarpath ,year)
     command_line = 'find  %s -printf "%%h/%%f,%%s\n"' % radardatepath
     print command_line
     args = shlex.split(command_line)
     #proc = subprocess.Popen(args, stdout=subprocess.PIPE)
     proc = subprocess.Popen(command_line, stdout=subprocess.PIPE,shell=True)
     (out, err) = proc.communicate()
     #print "program output:", out
     lines = out.split('\n')
     #print lines
     for line in lines:
          print line
#    - if there are files,
#      - do inserts to add the new files (right now this is lazy try to overwrite)
#
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
               print "INSERT INTO files VALUES ('%s','%s','','','','','')" % (path,size)


               # we have everything parsed about now do two inserts

               try:
                 msc.execute("INSERT INTO files VALUES ('%s','%s','','','','','')" % (path,size))
               except Exception,err:
                 print "insert exception"
                 print Exception, err
                 # this is already in our database
                 error = "insert exception"
               #print "here"
               try:
                 rows = msc.execute("SELECT size FROM files where path='%s' " % path )
                 sizedb = msc.fetchone()[0]
                 #print "size %s %s " % (size,sizedb)
                 if (int(size) != int(sizedb)):
                    print "size doesn't match %s %s" % (size,sizedb)
                    # update file set the size regardless.. if it is bigger maybe? hmm.. sql?
                    msc.execute("update files set size='%s',azure='', aws='' where path='%s' " % (size,path))
               except:
                 print "select exception"

               try:
                 awsc.execute("INSERT INTO files VALUES ('%s','%s','','','','','')" % (path,size))
               except:
                 #print "insert exception"
                 # this is already in our database
                 error = "insert exception"
               #print "here"
               try:
                 rows = awsc.execute("SELECT size FROM files where path='%s' " % path )
                 sizedb = awsc.fetchone()[0]
                 #print "size %s %s " % (size,sizedb)
                 if (int(size) != int(sizedb)):
                    print "size doesn't match %s %s" % (size,sizedb)
                    # update file set the size regardless.. if it is bigger maybe? hmm.. sql?
                    awsc.execute("update files set size='%s',azure='', aws='' where path='%s' " % (size,path))
               except:
                 print "select exception"



            else:
              print "error:"
              print line
              print len(res)



          except Exception,err:
              print "big except"
              print Exception, err
              print (traceback.format_exc())
              print line
              print "after big except"


     # close the database
     print "closing ms and aws db"+msdb+" "+awsdb
     msconn.commit()
     awsconn.commit()
     msconn.close()
     awsconn.close()

     #
     #  spawn the download here
     #
     mscommand = "/usr/local/uvcdat/2.0.0/bin/python ../ms/multithreadazure_builtin.py "+radarpath+" ./"+msdb+" >/dev/null &"
     print mscommand
     os.system(mscommand)
     awscommand = "/usr/local/uvcdat/2.0.0/bin/python ../aws/multithreads3.py "+radarpath+" ./"+awsdb+" > /dev/null &"
     print awscommand
     os.system(awscommand)


