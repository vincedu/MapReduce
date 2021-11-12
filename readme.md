to run social/tp2_people.py:
- setup pyspark
- in the script:
- input_filepath = <filename of the input text file>
- output_filepath = <directory where the recommendation is saved>
- spark-submit tp2_people.py


workcount_mrjob:
- wordfrequency.py: the python mrjob word count example
- time_hadoop_spark.sh: for each file listed in filelist.txt, counts its words using wordfrequency.py

-  time_hadoop_spark.sh "-r local: run the MR word count locally
-  time_hadoop_spark.sh "-r hadoop: run the MR word cound on hadoop 
