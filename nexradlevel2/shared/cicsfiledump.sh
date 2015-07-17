#!/bin/bash
#find /snfs9/q2/levelii_tarfiles/ -printf "%h/%f,%s\n" | grep "\.tar" | grep -v "_2012" > i3.txt
#find /snfs9/q2/levelii_tarfiles/2010??  -printf "%h/%f,%s\n" | grep "\.tar"  > i3.txt
find /snfs9/q2/levelii_tarfiles/2014??  -printf "%h/%f,%s\n" | grep "\.tar"  > i3.txt
