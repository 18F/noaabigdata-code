#!/bin/bash
find /snfs9/q2/levelii_tarfiles/ -printf "%h/%f,%s\n" | grep "\.tar" | grep -v "_2012" > i3.txt
