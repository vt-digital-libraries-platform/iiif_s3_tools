#!/bin/bash
while read line; do
   aws s3 --recursive mv s3://vtlib-store/SWVA/BHS/${line}/ s3://vtlib-store/SWVA/BHS/${line}/Access/
done <dir.txt
