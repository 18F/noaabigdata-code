import sqlite3
conn = sqlite3.connect('nexradl2-realtime.db')
c = conn.cursor()

# Create table
c.execute('''CREATE TABLE files
                 (path text, size integer, azure text, aws text, google text, ibm text, occ text)''')
c.execute('''CREATE INDEX path_index on files(path)''')
c.execute('''CREATE INDEX azure_index on files(azure)''')
c.execute('''CREATE INDEX aws_index on files(aws)''')
c.execute('''CREATE INDEX size_index on files(size)''')
# Insert a row of data
#c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")

# Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()

