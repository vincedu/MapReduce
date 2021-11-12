
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("log8415tp2people.py")
sc = SparkContext(conf = conf)


def convertToBFS(line):
    fields = line.split("\t")
    user = int(fields[0])
    connections = fields[1].split(',')

    friends = [int(f) for f in connections if f != ""]
    workStat = 'INITIAL'
    distance = 9999

    if (user == processUserID):
        workStat = 'WIP'
        distance = 0

    return (user, (friends, distance, workStat))


def bfsMap(node):
    userID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    workStat = data[2]

    results = []

    #If this node needs to be expanded...
    if (workStat == 'WIP'):
        for connection in connections:
            newUserID = connection
            newDistance = distance + 1
            newWorkStat = 'WIP'

            newEntry = (newUserID, ([], newDistance, newWorkStat))
            results.append(newEntry)

        #We've processed this node, set workStat to DONE
        workStat = 'DONE'

    #Emit the new nodes
    results.append( (userID, (connections, distance, workStat)) )
    return results


def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    workStat1 = data1[2]
    workStat2 = data2[2]

    distance = 9999
    workStat = workStat1
    edges = []

    # See if one is the original node with its connections.
    # If so preserve them.
    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)

    # Preserve minimum distance
    if (distance1 < distance):
        distance = distance1

    if (distance2 < distance):
        distance = distance2

    # Preserve the most recent workStat
    if (workStat1 == 'INITIAL' and (workStat2 == 'WIP' or workStat2 == 'DONE')):
        workStat = workStat2

    if (workStat1 == 'WIP' and workStat2 == 'DONE'):
        workStat = workStat2

    if (workStat2 == 'INITIAL' and (workStat1 == 'WIP' or workStat1 == 'DONE')):
        workStat = workStat1

    if (workStat2 == 'WIP' and workStat1 == 'DONE'):
        workStat = workStat1

    return (edges, distance, workStat)



#Main starts:
lines = []  # to accumulate line of result, each line is <User><tab><recommendations>
# how far of the people to recommend. 0 = self; 1 = friend; 2 = mutual; 3 = mutual friend's friend, etc.
distance_between_people = 2

input_filepath = "file:///home/azureuser/social/soc-LiveJournal1Adj.txt"
output_filepath = "file:///home/azureuser/social/recommendations"

partitions = 10
input_rdd = sc.textFile(input_filepath, partitions)

# in case to generate recommendation for all users
#allUsers = input_rdd.map(lambda x : x[0]).collect()

to_test = [924, 8941, 8942, 9019, 9020, 9021, 9022, 9990, 9992, 9993]
#to_test = [924, 8941, 8942, 9019]
#to_test = [9020, 9021, 9022, 9990, 9992, 9993]
#to_test = [9992]
for processUserID in to_test:
    print("Processing recommendation for user " + str(processUserID))
    iterationRdd = input_rdd.map(convertToBFS)

    for iteration in range(0, distance_between_people):
        print("Running BFS iteration# " + str(iteration+1))

        # Create new vertices as needed to reduce distances in the reduce stage. 
        mapped = iterationRdd.flatMap(bfsMap)


        # Reducer combines data for each  userID, preserving the most recent workStat and shortest path.
        iterationRdd = mapped.reduceByKey(bfsReduce)


    # rdd (userID, ([friendID1, friendID2 ....], 2, <workStat>))
    friends_at_distance = iterationRdd.filter(lambda x: x[1][1] == distance_between_people)
    userID = iterationRdd.filter(lambda x: x[0] == processUserID).mapValues(lambda x: x[0])
    degree1 = userID.collect()
    direct = set(degree1[0][1])

    # rdd (num_mutual, friend_id)
    recommend_rdd = friends_at_distance.mapValues(lambda x : (len(set(x[0]).intersection(direct)))).map(lambda x: (x[1], x[0]))
    mutual_asd_lst = recommend_rdd.sortByKey(ascending=False).collect()
    top10 = sorted(mutual_asd_lst, key=lambda x: (-x[0], x[1]))[:10]

    print(processUserID, top10)
    print('-' * 88)

    suspect_lst = [str(suspect[1]) for suspect in top10]
    suspects = ",".join(suspect_lst)
    line = str(processUserID) + "\t" + suspects
    lines.append(line)
# output to a text file
sc.parallelize(lines, numSlices=1).saveAsTextFile(output_filepath)
print("Saved to: ", output_filepath)
