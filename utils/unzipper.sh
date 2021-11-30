#!/bin/bash

#run from data directory, with scripts in ..
for VARIABLE in ./*.gz
do
	echo extracting $VARIABLE
	gzip -d $VARIABLE
	for DARSHAN_FILE in *.darshan
	do
		echo 'scanning'
		echo  $DARSHAN_FILE
		{
			darshan-parser $DARSHAN_FILE > $DARSHAN_FILE.txt
		}&&
		{
			python3 ../worker.py $DARSHAN_FILE.txt
		}
		echo 'cleaning up'
		rm -f $DARSHAN_FILE.txt
		rm -f $DARSHAN_FILE
	done
done
