import sqlite3
import sys

db_name = 'nexradl2-years.db'

#
#  sys.argv[1]: dbname
#

if len(sys.argv) > 1:
  print sys.argv
  db_name = sys.argv[1]

print db_name

conn = sqlite3.connect(db_name)
c = conn.cursor()

# Create table
c.execute('''CREATE TABLE years
                 (year integer, size numberoffiles, done text, path text, primary key(year))''')
c.execute('''CREATE INDEX year_index on years(year)''')

for year in range(1991, 2014):
  c.execute("INSERT INTO years VALUES ('%s','0','no')" % year )

# Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()

