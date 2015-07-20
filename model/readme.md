get_hrrr.sh is a script that downloads the HRRR. Change it so it will download the whole thing or just parts.

I would suggest that it is put in cron twice, maybe 1 after, and 25 after the hour. It has a -N in wget, so it will only download new files.

A GFS downloader would be similar:

Use the base URL:
http://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.2015072018/
2015072018 :  in UTC: <YEAR><MONTH><DAY><HOUR> 
 HOUR is one of 4 runs: 00, 06, 12, or 18

Inside the directory you want to grab all of these files:

pgrb2.0p25
pgrb2b.0p25
sfluxgrb

gfs.t18z.pgrb2.0p25.f000 
t18z - is the model run, it needs to match the directory
pgrb2 - this is the type
0p25 - this is the resolution
f000 - this is the forecast hour, 000 to 384 - 3 hour increments through 240, then 12 hour increments through 384

It takes roughly 1.5 hours for all the files to be written, so I am not sure the best way to download it incrementally. Maybe use the -N and run the script every 10 minutes or something like that until you get all the pieces.

