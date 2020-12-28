#!/usr/bin/env bash

## nohup ./bash/transformDataSetList.sh <List of derived datasets>  > /tmp/logs/testRun.log &
time ./bash/transformDataSetList.sh file0__tpcds__q2,file0__tpcds__q4

time ./bash/transformDataSetGroup.sh tpcds_file
## files and postgres db
time ./bash/transformDataSetGroup.sh mixed

## jdbc direct
# time ./bash/transformDataSetList.sh wscs1_direct_1
 


