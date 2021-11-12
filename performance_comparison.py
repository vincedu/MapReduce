import subprocess  
import numpy as np 
import matplotlib.pyplot as plt
import shutil
  
def wordcount(cmd):
    # Execute WordCount 3 times and compute the average execution time
    total_execution_time = 0
    for i in range(3):
        temp = subprocess.Popen(cmd, stdout = subprocess.PIPE) 
        # get the output
        stdout, stderr = temp.communicate()
        # get user time
        stdout=stdout.split("\n")
        for line in stdout:
            if line.startswith("user"):
                print(line)
        # remove output directory
        try:
            shutil.rmtree(output_directory)
        except OSError as e:
            print("Error: %s - %s." % (e.filename, e.strerror))
    # # TODO: make sure float divide by int outputs float
    # average_time = total_execution_time / 3
    # # TODO: add more detail to log
    # print("average time: ", average_time)
    # return average_time

def plot(hadoop, spark):
    X = ['Dataset1', 'Dataset2', 'Dataset3', 'Dataset4', 'Dataset5', 'Dataset6', 'Dataset7', 'Dataset8', 'Dataset9']
    
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

    filename = 'dataset1.txt'
    output_directory = '/home/azureuser/datasets/output'

    hadoop_cmd = 'time hadoop jar /usr/local/hadoop-3.3.1/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.1.jar wordcount /home/azureuser/datasets/' + filename + ' ' + output_directory
    spark_cmd = 'time python3 /home/azureuser/wordcount.py /home/azureuser/datasets/' + filename + ' ' + output_directory

    wordcount(hadoop_cmd)
    wordcount(spark_cmd)