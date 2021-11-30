#!/bin/bash
rm -f ./*.darshan
rm -f ./*.txt
# dangerous: 
find . -name "*.csv" -exec rm -f {} \;
