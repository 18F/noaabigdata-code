# NEXRAD L2 Uploader

These are helper scripts to upload data from disk to multiple cloud providers. Each cloud provider has it's own directory. The shared directory mostly deals with things that are independant of provider.  

There are two workflows. One handles mass uploads, the other uploads today's data.

The workflow for the mass upload:
+ create a blank sqlite database
```
       createdatabase.py <nameofdatabasefile>
```
+ create a listing of the files to upload using find
```
       cicsfiledump.sh
```
+ insert the data from the file into a database:
 ```
       filldbfromcics.py <nameofdatabasefile> <nameoftextfile>
```
+ run each uploader:
```
       aws/multithreads3.py  <radarpath> <dbname>
       ms/multithreadazure_builtin.py <radarpath> <dbname>
```
Workflow for near realtime:
 + onetime, create a blank sqlite database
 ```
       createdatabase.py nexradl2-realtime.db
```
 + from cron, cron_update.sh is called 
 + cron calls hourlyimporter.py
  
 
   hourlyimporter does a find based on the date, then adds the files to the realtime database. It checks the size, and will update the database if the file got bigger on disk. It then runs a compare comand (comparedbtos3.py / comparedbtoazure.py) to set the files that were uploaded last time to the cloud. It then uploads the files to the cloud. 
