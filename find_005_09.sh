#!/bin/bash

task_id=$1

echo task $task_id checking...

echo scan meta...

python /home/op/cheng/hbase-store/scan_meta.py $task_id > ${task_id}_meta.txt

echo scan table...

python /home/op/cheng/hbase-store/scan_table.py $task_id > ${task_id}_table.txt

meta_005_cnt=`grep -c 005_jpg ${task_id}_meta.txt`

meta_09_cnt=`grep -c 09_000_png ${task_id}_meta.txt`

table_005_cnt=`grep -c 005_jpg ${task_id}_table.txt`

table_09_cnt=`grep -c 09_000_png ${task_id}_table.txt`

echo $meta_005_cnt $meta_09_cnt $table_005_cnt $table_09_cnt
