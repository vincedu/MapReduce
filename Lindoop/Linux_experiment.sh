#!/bin/bash

datasetdir=/home/azedine/Downloads/dataset

echo "################### Linux processing ######################" >> processing_time.txt
for i in {1..3} ; do 
      
      for f in `ls $datasetdir/*.txt`; do
            msg="time for iteration $i for file `basename $f`"
            (time cat $f | tr " " "\n" |sort |uniq -c > $f.imp |grep user) &> pro_time.txt
            echo $msg" "`cat pro_time.txt | grep user` >> processing_time.txt
      done
done
