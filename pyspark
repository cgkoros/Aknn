# Databricks notebook source
#LOAD DATA INTO A DATAFRAME

#display(dbutils.fs.ls("dbfs:/FileStore/tables"))
sparkDF = SparkContext.read.format("csv").options(header="true",inferschema="true").load("/Iris.csv")
#display(sparkDF)
sparkDF.show(2)



# COMMAND ----------

# We'll hold out 80% for training and  leave 20% for testing
seed = 1800009193L
(split_80_df, split_20_df) = sparkDF.randomSplit([0.8,0.2],seed)

# Let's cache these datasets for performance
training_df = split_80_df.cache()
test_df = split_20_df.cache()

print('Training: {0}, test: {1}\n'.format(
  training_df.count(), test_df.count())
)
training_df.show(3)
test_df.show(3)

# COMMAND ----------

#FIND mean values
meanX =(sparkDF
         .groupBy().mean('x')
         .collect()[0][0])

print meanX

meanY =(sparkDF
         .groupBy().mean('y')
         .collect()[0][0])
print meanY

# COMMAND ----------

coordinates = training_df.drop("class")
training_list = [(row[0], row[1]) for row in coordinates.collect()]

def geticchid(list2):
  my_dictionary = {"icch1" : 0 , "icch2" : 0, "icch3" : 0, "icch4" : 0}
  for i in list2:
    if i[0] <= meanX and i[1] <= meanY:
      my_dictionary["icch1"] = my_dictionary["icch1"] + 1
    elif i[0] > meanX and i[1] < meanY:
      my_dictionary["icch2"] = my_dictionary["icch2"] + 1    
    elif i[0] >= meanX and i[1] >= meanY:
      my_dictionary["icch3"] = my_dictionary["icch3"] + 1
    elif i[0] < meanX and i[1] > meanY:
      my_dictionary["icch4"] = my_dictionary["icch4"] + 1
    else:
      print "Not valid coordinates"
  return my_dictionary    

geticchid(training_list)


# COMMAND ----------

def icchid(x,y):
  if x <= meanX and y <= meanY:
    return "icch1"
  elif x > meanX and y < meanY:
    return "icch2"
  elif x >= meanX and y >= meanY:
    return "icch3"
  elif x < meanX and y > meanY:
    return "icch4"
  else:
    return 0

trainingwithtuplesRDD = training_df.rdd.map(lambda x: [icchid(x[0],x[1]),(x[0],x[1],x[2])])
testwithtuplesRDD = test_df.rdd.map(lambda x: [icchid(x[0],x[1]),(x[0],x[1])])

trainingwithtuplesRDD.take(10)


# COMMAND ----------

training_list_with_icch_id = trainingwithtuplesRDD.groupByKey().mapValues(list).collect()
Lt = training_list_with_icch_id[0][1] + training_list_with_icch_id[1][1] + training_list_with_icch_id[2][1] +training_list_with_icch_id[3][1]

training_list_with_icch_id

# COMMAND ----------

test_list_with_icch_id = testwithtuplesRDD.groupByKey().mapValues(list).collect()


# COMMAND ----------

test_list_with_icch_id[0]

# COMMAND ----------

for i in test_list_with_icch_id:
    if i[0] == "icch4":
      Ltest4 = i[1]
    elif i[0] == "icch3":
      Ltest3 = i[1]
    elif i[0] == "icch2":
      Ltest2 = i[1]
    elif i[0] == "icch1":
      Ltest1 = i[1]
      
for i in training_list_with_icch_id:
    if i[0] == "icch4":
      Ltraining4 = i[1]
    elif i[0] == "icch3":
      Ltraining3 = i[1]
    elif i[0] == "icch2":
      Ltraining2 = i[1]
    elif i[0] == "icch1":
      Ltraining1 = i[1]      
      


# COMMAND ----------

#Arxikh ypologistikh fash
#euclidean distance
import math
def euclideanDistance(instance1, instance2, length):
	distance = 0
	for x in range(length):
		distance += pow((instance1[x] - instance2[x]), 2)
	return math.sqrt(distance)

  

# COMMAND ----------

import operator 
def getNeighbors(trainingSet, testInstance, k):
	distances = []
	length = len(testInstance)
	for x in range(len(trainingSet)):
		dist = euclideanDistance(testInstance, trainingSet[x], length)
		distances.append((trainingSet[x], dist))
	distances.sort(key=operator.itemgetter(1))
	neighbors = []
	for x in range(k):
		neighbors.append(distances[x])
	return neighbors


# COMMAND ----------

#create RDDs to compute knn_list
testicch4RDD=sc.parallelize(Ltest4)
testicch3RDD=sc.parallelize(Ltest3)
testicch2RDD=sc.parallelize(Ltest2)
testicch1RDD=sc.parallelize(Ltest1)


neighborsicch4RDD = testicch4RDD.map(lambda x : [(x[0],x[1]),getNeighbors(Ltraining4,(x[0],x[1]),10)])
neighborsicch3RDD = testicch3RDD.map(lambda x : [(x[0],x[1]),getNeighbors(Ltraining3,(x[0],x[1]),10)])
neighborsicch2RDD = testicch2RDD.map(lambda x : [(x[0],x[1]),getNeighbors(Ltraining2,(x[0],x[1]),10)])
neighborsicch1RDD = testicch1RDD.map(lambda x : [(x[0],x[1]),getNeighbors(Ltraining1,(x[0],x[1]),10)])
neighborsicch1RDD.take(10)
#testicch4RDD.collect()

# COMMAND ----------

#Elegxos epikalupsewn
# Return the key-value pairs in the RDD to the master as a dictionary

neighborsicch1RDDdictionary=neighborsicch1RDD.collectAsMap()
neighborsicch2RDDdictionary=neighborsicch2RDD.collectAsMap()
neighborsicch3RDDdictionary=neighborsicch3RDD.collectAsMap()
neighborsicch4RDDdictionary=neighborsicch4RDD.collectAsMap()
neighborsicch1RDDdictionary

# COMMAND ----------

#Fetch the maximum distance for every point in the test set

def getmaximumdistance (neighborsdictionary, Lmaxdistancelist):
  Lmaxdistancelist=[]
  for k,v in neighborsdictionary.items():
    x = v[-1]
    a, b =x
    Lmaxdistancelist.append((k,b))
  return Lmaxdistancelist
     

# COMMAND ----------

#create a list of the maximum distances for each point in each cell
Lmaxdistance1 = []
Lmaxdistance2 = []
Lmaxdistance3 = []
Lmaxdistance4 = []

Lmaxdistance1 = getmaximumdistance(neighborsicch1RDDdictionary,Lmaxdistance1)
Lmaxdistance2 = getmaximumdistance(neighborsicch2RDDdictionary,Lmaxdistance2)
Lmaxdistance3 = getmaximumdistance(neighborsicch3RDDdictionary,Lmaxdistance3)
Lmaxdistance4 = getmaximumdistance(neighborsicch4RDDdictionary,Lmaxdistance4)
Lmaxdistance4

# COMMAND ----------

#FIND OVERLAPPED CELLS FOR ALL POINTS IN ICCH1

#find distance from meanX
overlappedICCH2from1 = []
for i in range(len(Lmaxdistance1)):
  x, y = Lmaxdistance1[i][0]
  if meanX - x < Lmaxdistance1[i][1]:
    overlappedICCH2from1.append((x,y))
print overlappedICCH2from1

#find distance from meanY
overlappedICCH4from1 = []
for i in range(len(Lmaxdistance1)):
  x, y = Lmaxdistance1[i][0]
  if meanY - y < Lmaxdistance1[i][1]:
    overlappedICCH4from1.append((x,y))
print overlappedICCH4from1

#find distance from meanX,meanY
overlappedICCH3from1 = []
for i in range(len(Lmaxdistance1)):
  x, y = Lmaxdistance1[i][0]
  data1 = [x, y]
  data2 = [meanX,meanY]
  distance = euclideanDistance(data1, data2,2)
  if distance < Lmaxdistance1[i][1]:
    overlappedICCH3from1.append((x,y))
print overlappedICCH3from1

# COMMAND ----------

#FIND OVERLAPPED CELLS FOR ALL POINTS IN ICCH2

#find distance from meanX
overlappedICCH1from2 = []
for i in range(len(Lmaxdistance2)):
  x,y = Lmaxdistance2[i][0]
  if x - meanX <= Lmaxdistance2[i][1]:
    overlappedICCH1from2.append((x,y))
print overlappedICCH1from2

#find distance from meanY
overlappedICCH3from2 = []
for i in range(len(Lmaxdistance2)):
  x, y = Lmaxdistance2[i][0]
  if meanY - y <=Lmaxdistance2[i][1]:
    overlappedICCH3from2.append((x,y))
print overlappedICCH3from2

#find distance from meanX,meanY
overlappedICCH4from2 = []
for i in range(len(Lmaxdistance2)):
  x, y = Lmaxdistance2[i][0]
  data3 =[x, y]
  data4 =[meanX,meanY]
  distance = euclideanDistance(data3,data4,2)
  if distance <= Lmaxdistance2[i][1]:
    overlappedICCH4from2.append((x,y))
print overlappedICCH4from2


# COMMAND ----------

#FIND OVERLAPPED CELLS FOR POINTS IN ICCH3

#find distance from meanX
overlappedICCH4from3 = []
for i in range(len(Lmaxdistance3)):
  x,y = Lmaxdistance3[i][0]
  if x - meanX < Lmaxdistance3[i][1]:
    overlappedICCH4from3.append((x,y))
print overlappedICCH4from3    

#find distance from meanY
overlappedICCH2from3 = []
for i in range(len(Lmaxdistance3)):
  x,y = Lmaxdistance3[i][0]
  if y - meanY < Lmaxdistance3[i][1]:
    overlappedICCH2from3.append((x,y))
print overlappedICCH2from3    

#find distance from meanX,meanY
overlappedICCH1from3 = []
for i in range(len(Lmaxdistance3)):
  x,y = Lmaxdistance3[i][0]
  data5 =[x, y]
  data6 =[meanX,meanY]
  distance = euclideanDistance(data5,data6,2)
  if distance < Lmaxdistance3[i][1]:
    overlappedICCH1from3.append((x,y))
print overlappedICCH1from3  
 

# COMMAND ----------

#FIND OVERLAPPED CELLS FOR POINTS IN ICCH4

#find distance from meanX
overlappedICCH3from4 = []
for i in range(len(Lmaxdistance4)):
  x,y = Lmaxdistance4[i][0]
  if meanX - x <= Lmaxdistance4[i][1]:
    overlappedICCH3from4.append((x,y))
print overlappedICCH3from4     

#find distance from meanY
overlappedICCH1from4 = []
for i in range(len(Lmaxdistance4)):
  x,y = Lmaxdistance4[i][0]
  if y - meanY <= Lmaxdistance4[i][1]:
    overlappedICCH1from4.append((x,y))
print overlappedICCH1from4  

#find distance from meanX,meanY
overlappedICCH2from4 = []
for i in range(len(Lmaxdistance4)):
  x,y = Lmaxdistance4[i][0]
  data7 = [x,y]
  data8 = [meanX,meanY]
  distance4 = euclideanDistance(data7,data8,2)
  if distance4 < Lmaxdistance4[i][1]:
    overlappedICCH2from4.append((x,y))
print overlappedICCH2from4    


# COMMAND ----------

#Derive updates of k-nn lists for the ovelapped cells for the points in ICCH1

overlappedicch2from1RDD = sc.parallelize(overlappedICCH2from1)
overlappedicch4from1RDD = sc.parallelize(overlappedICCH4from1)
overlappedicch3from1RDD = sc.parallelize(overlappedICCH3from1) 

neighborsovericch2from1RDD = overlappedicch2from1RDD.map(lambda x : [(x[0],x[1]),getNeighbors(Ltraining2,(x[0],x[1]),10)])
neighborsovericch4from1RDD = overlappedicch4from1RDD.map(lambda x : [(x[0],x[1]),getNeighbors(Ltraining4,(x[0],x[1]),10)])
neighborsovericch3from1RDD = overlappedicch3from1RDD.map(lambda x : [(x[0],x[1]),getNeighbors(Ltraining3,(x[0],x[1]),10)])
neighborsovericch4from1RDD.take(2)

# COMMAND ----------

#Derive updates of k-nn lists for the ovelapped cells for the points in ICCH2
overlappedicch1from2RDD = sc.parallelize(overlappedICCH1from2)
overlappedicch3from2RDD = sc.parallelize(overlappedICCH3from2)
overlappedicch4from2RDD = sc.parallelize(overlappedICCH4from2)

neighborsovericch1from2RDD = overlappedicch1from2RDD.map(lambda x : [(x[0],x[1]),getNeighbors(Ltraining1,(x[0],x[1]),10)])
neighborsovericch3from2RDD = overlappedicch3from2RDD.map(lambda x : [(x[0],x[1]),getNeighbors(Ltraining3,(x[0],x[1]),10)])
neighborsovericch4from2RDD = overlappedicch4from2RDD.map(lambda x : [(x[0],x[1]),getNeighbors(Ltraining4,(x[0],x[1]),10)])

# COMMAND ----------

#Derive updates of k-nn lists for the ovelapped cells for the points in ICCH3
overlappedicch4from3RDD = sc.parallelize(overlappedICCH4from3)
overlappedicch2from3RDD = sc.parallelize(overlappedICCH2from3)
overlappedicch1from3RDD = sc.parallelize(overlappedICCH1from3)

neighborsovericch4from3RDD = overlappedicch4from3RDD.map(lambda x : [(x[0],x[1]),getNeighbors(Ltraining4,(x[0],x[1]),10)])
neighborsovericch2from3RDD = overlappedicch2from3RDD.map(lambda x : [(x[0],x[1]),getNeighbors(Ltraining2,(x[0],x[1]),10)])
neighborsovericch1from3RDD = overlappedicch1from3RDD.map(lambda x : [(x[0],x[1]),getNeighbors(Ltraining1,(x[0],x[1]),10)])

# COMMAND ----------

#Derive updates of k-nn lists for the ovelapped cells for the points in ICCH4
overlappedicch3from4RDD = sc.parallelize(overlappedICCH3from4)
overlappedicch1from4RDD =sc.parallelize(overlappedICCH1from4)

neighborsovericch3from4RDD = overlappedicch3from4RDD.map(lambda x : [(x[0],x[1]),getNeighbors(Ltraining3,(x[0],x[1]),10)])
neighborsovericch1from4RDD = overlappedicch1from4RDD.map(lambda x : [(x[0],x[1]),getNeighbors(Ltraining1,(x[0],x[1]),10)])

# COMMAND ----------

allneighborsforicch1RDD = sc.union([neighborsicch1RDD, neighborsovericch2from1RDD, neighborsovericch4from1RDD, neighborsovericch3from1RDD])

allneighborsforicch1grouped = allneighborsforicch1RDD.reduceByKey(lambda x,y: x + y).collect()


# COMMAND ----------


allneighborsforicch2RDD = sc.union([neighborsicch2RDD,neighborsovericch1from2RDD, neighborsovericch3from2RDD, neighborsovericch4from2RDD])

allneighborsforicch2grouped = allneighborsforicch2RDD.reduceByKey(lambda x,y: x + y).collect()
allneighborsforicch2grouped

# COMMAND ----------

allneighborsforicch3RDD = sc.union([neighborsicch3RDD, neighborsovericch1from3RDD, neighborsovericch2from3RDD, neighborsovericch4from3RDD ])

allneighborsforicch3grouped = allneighborsforicch3RDD.reduceByKey(lambda x,y: x + y).collect()
allneighborsforicch3grouped

# COMMAND ----------

allneighborsforicch4RDD = sc.union([neighborsicch4RDD, neighborsovericch3from4RDD, neighborsovericch1from4RDD])

allneighborsforicch4grouped = allneighborsforicch4RDD.reduceByKey(lambda x,y: x + y).collect()
allneighborsforicch4grouped

# COMMAND ----------

#sort distances by asceding order for the points in ICCH1
from operator import itemgetter
for i in allneighborsforicch1grouped:
  a, b = i
  b.sort(key=itemgetter(1),reverse = False)
  
allneighborsforicch1grouped

# COMMAND ----------

#sort distances by asceding order for the points in ICCH2
from operator import itemgetter
for i in allneighborsforicch2grouped:
  a, b = i
  b.sort(key=itemgetter(1),reverse = False)
  
print len(allneighborsforicch2grouped)


# COMMAND ----------

#sort distances by asceding order for the points in ICCH3
from operator import itemgetter
for i in allneighborsforicch3grouped:
  a, b = i
  b.sort(key=itemgetter(1),reverse = False)
  
print len(allneighborsforicch3grouped)


# COMMAND ----------

#sort distances by asceding order for the points in ICCH4
from operator import itemgetter
for i in allneighborsforicch4grouped:
  a, b =i
  b.sort(key=itemgetter(1),reverse = False)
  
print len(allneighborsforicch4grouped)  

# COMMAND ----------

#create the final k-nn list for points in icch1
allneighborsforicch1final = []
for i in allneighborsforicch1grouped:
  x, y = i
  y = y[:10]
  allneighborsforicch1final.append((x,y))
  
print len(allneighborsforicch1final)

# COMMAND ----------

#create the final k-nn list for points in icch2
allneighborsforicch2final = []
for i in allneighborsforicch2grouped:
  x, y = i
  y = y[:10]
  allneighborsforicch2final.append((x,y))
  
allneighborsforicch2final 

# COMMAND ----------

#create the final k-nn list for points in icch3
allneighborsforicch3final = []
for i in allneighborsforicch3grouped:
  x, y = i
  y = y[:10]
  allneighborsforicch3final.append((x,y))

print len(allneighborsforicch3final)

# COMMAND ----------

#create the final k-nn list for points in icch4
allneighborsforicch4final = []
for i in allneighborsforicch4grouped:
  x, y =i
  y = y[:10]
  allneighborsforicch4final.append((x,y))
  
allneighborsforicch3final

# COMMAND ----------

#create a list with the points and the k-nn lists without distance
def getpointwithneighbors(point_list):
  allneighborswithidpoint = []
  for i in point_list:
    x, y =i
    res_list = [item[0] for item in y]
    allneighborswithidpoint.append((x,res_list))
  return allneighborswithidpoint

allneighborsforicch4 = getpointwithneighbors(allneighborsforicch4final)
allneighborsforicch3 = getpointwithneighbors(allneighborsforicch3final)
allneighborsforicch2 = getpointwithneighbors(allneighborsforicch2final)
allneighborsforicch1 = getpointwithneighbors(allneighborsforicch1final)
print len(allneighborsforicch1)

# COMMAND ----------

allpointswithknnlists = allneighborsforicch4 + allneighborsforicch3 + allneighborsforicch2 + allneighborsforicch1
allpointswithknnlists

# COMMAND ----------

import operator
def getClass(neighbors_list):
  classVotes = {}
  for x in range(len(neighbors_list)):
    response = neighbors_list[x][-1]
    if response in classVotes:
      classVotes[response] += 1
    else:
      classVotes[response] = 1
  sortedVotes = sorted(classVotes.iteritems(),key=operator.itemgetter(1),reverse=True)
  return sortedVotes[0][0]



# COMMAND ----------

predictions = {}
final_list = []
for i in allpointswithknnlists:
  a, b = i
  x = getClass(b)
  final_list.append((a,x))
  predictions[a]=x
print len(final_list)  
predictions


# COMMAND ----------

test_points_with_class = test_df.rdd.map(lambda x: [(x[0],x[1]),x[2]])
list_with_test= test_points_with_class.collectAsMap()
list_with_test

# COMMAND ----------


def getAccuracy(testSet, predictions):
  correct = 0
  for x in testSet:
    if predictions.has_key(x):
      if testSet[x] == predictions[x]:
        #L23.append(x)
        correct += 1
  #return len(L23)    
  return (correct/float(len(testSet))) * 100.0

  
accuracy = getAccuracy(list_with_test,predictions)
print accuracy

