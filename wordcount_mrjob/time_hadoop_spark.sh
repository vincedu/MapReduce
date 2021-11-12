#!/usr/bin/bash

#usage:  time_hadoop_spark.sh "-r local" | "-r spark" | "-r hadoop"

cd /home/azureuser/hadoop 
for file_to_count in $(cat filelist.txt) ; do

echo "start $file_to_count."

for c in {1..3}
do

  time python wordfrequency.py $1 file:///home/azureuser/data2count/$file_to_count > ./hdp_stream.txt 2>&1

done

echo "finish $file_to_count."
done

