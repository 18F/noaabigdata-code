#!/bin/bash
find /snfs9/q2/levelii_tarfiles/ -printf "%h/%f,%s\n" | grep "\.tar" > i3.txt
