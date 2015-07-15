#!/bin/bash
# run this script to take i3.txt and update each database 
# when it is done we can  launch the uploaders 
nohup python filldbfromcics.py  ../aws/nexradl2aws.db &
nohup python filldbfromcics.py ../ms/nexradl2.db &

