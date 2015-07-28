#!/bin/bash
source /home/asteremberg/.bashrc
cd /home/asteremberg/noaabigdata-code/nexradlevel2/shared
python hourlyimporter.py > hourly.out
