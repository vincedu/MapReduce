from subprocess import PIPE, run
import sys
import shlex
import numpy as np 
import matplotlib.pyplot as plt
import shutil
import os
  
def wordcount(cmd):
    # Execute WordCount 3 times and compute the average execution time
    total_execution_time = 0
    for i in range(3):
    	# execute command and get output
        output = run(shlex.split(cmd), stdout = PIPE, stderr = PIPE, universal_newlines=True).stderr
        # get user time
        for line in output.splitlines():
            if "user " in line:
                user_index = line.find("user")
                user_time = float(line[:user_index])
                print("user_time for execution ", i + 1 , ": ", user_time)
                total_execution_time += user_time
        if os.path.isdir(output_directory):
            try:
                shutil.rmtree(output_directory)
            except OSError as e:
                print("Error: %s - %s." % (e.filename, e.strerror))
    average_time = round(total_execution_time / 3, 2)
    print("average time: ", average_time)
    return average_time

def plot(hadoop, spark):
    X = ['1', '2', '3', '4', '5', '6', '7', '8', '9']
    
    X_axis = np.arange(len(X))

    plt.bar(X_axis - 0.2, hadoop, 0.4, label = 'Hadoop')
    plt.bar(X_axis + 0.2, spark, 0.4, label = 'Spark')

    plt.xticks(X_axis, X)
    plt.xlabel("Datasets")
    plt.ylabel("Execution time of WordCount")
    plt.title("Execution time of WordCount on Hadoop and Spark for all datasets")
    plt.legend()
    plt.savefig('result.png')
  
if __name__ == '__main__':

    hadoop_execution_times = []
    spark_execution_times = []
    output_directory = '/home/azureuser/datasets/output'
    
    for i in range(1, 10):
        filename = 'dataset' + str(i) + '.txt'
        hadoop_cmd = 'time python3 /home/azureuser/wordcount_hadoop.py -r local /home/azureuser/datasets/' + filename
        spark_cmd = 'time python3 /home/azureuser/wordcount_spark.py /home/azureuser/datasets/' + filename + ' ' + output_directory
        print("starting hadoop wordcount on", filename)
        hadoop_execution_times.append(wordcount(hadoop_cmd))
        print("starting spark wordcount on", filename)
        spark_execution_times.append(wordcount(spark_cmd))
    print("Hadoop WordCount execution times: ", hadoop_execution_times)
    print("Spark WordCount execution times: ", spark_execution_times)
    plot(hadoop_execution_times, spark_execution_times)
