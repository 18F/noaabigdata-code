import sqlite3
conn = sqlite3.connect('nexradl2.db')
c = conn.cursor()


#open the file list and look for files

# Insert a row of data
filename = "i3.txt"
with open(filename) as f:
  for line in f:
     res = line.strip().split('/')
     print res
     if len(res) == 8:
       path = res[4]+'/'+res[5]+'/'+res[6]+'/'+res[7]
       print path 
       c.execute("INSERT INTO files VALUES ('%s','','','','','')" % (path))
     else:
       print len(res)
     
#     exit()

# Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()

