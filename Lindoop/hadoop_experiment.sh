#!/bin/bash

set -x

############# dataset experimention ########################################################################
hdfs dfs -rm -r -f inputdataset
hdfs dfs -mkdir inputdataset 

datasetdir=/home/azedine/Downloads/dataset
mkdir -p $datasetdir
rm -f $datasetdir/*

curl -sL -o $datasetdir/buchanj-midwinter-00-t.txt https://tinyurl.com/4vxdw3pa 
curl -sL -o $datasetdir/carman-farhorizons-00-t.txt https://tinyurl.com/kh9excea 
curl -sL -o $datasetdir/colby-champlain-00-t.txt https://tinyurl.com/dybs9bnk 
curl -sL -o $datasetdir/cheyneyp-darkbahama-00-t.txt https://tinyurl.com/datumz6m 
curl -sL -o $datasetdir/delamare-bumps-00-t.txt https://tinyurl.com/j4j4xdw6 
curl -sL -o $datasetdir/charlesworth-scene-00-t.txt https://tinyurl.com/ym8s5fm4 
curl -sL -o $datasetdir/delamare-lucy-00-t.txt https://tinyurl.com/2h6a75nk 
curl -sL -o $datasetdir/delamare-myfanwy-00-t.txt https://tinyurl.com/vwvram8 
curl -sL -o $datasetdir/delamare-penny-00-t.txt https://tinyurl.com/weh83uyn

echo "################### Hadoop processing ######################" >> processing_time.txt
for i in {1..3} ; do 

      for f in `ls $datasetdir/*.txt`; do
            filename=`basename $f`
            hdfs dfs -copyFromLocal $datasetdir/$filename inputdataset
            msg="time for iteration $i for file $filename"
            hdfs dfs -rm -r -f outputdataset
            (time hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.1.jar wordcount inputdataset/ outputdataset/) &> dataset_log_time.txt
            echo $msg" "`cat dataset_log_time.txt | grep ^user` >> processing_time.txt
            hdfs dfs -rm -r -f inputdataset
            
      done     
      
done  


