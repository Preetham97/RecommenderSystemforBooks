from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

import json
import numpy as np
import sys

sc = spark.sparkContext
text = sc.textFile(sys.argv[1])

df_fin =text.map(lambda x: json.loads(x))



def myFuncA(x):
    return ((x['reviewerID'], x['asin']),(x['overall'], x['unixReviewTime'], x['verified']))

OneRatingRDD = df_fin.map(lambda x: myFuncA(x)).groupByKey().map(lambda x: (x[0], list(x[1])[-1]))#.map(lambda x: func2(x))

#print(OneRatingRDD.take(2))

item25RDD = OneRatingRDD.map(lambda x: (x[0][1], (x[0][0],x[1][0]))).groupByKey().filter(lambda x: len(list(x[1]))>=25)

#print(item25RDD.take(2))

def interchange(x):
    result = []
    for i in x[1]:
        result.append((i[0], (x[0], i[1])))
    return result

User5itemsRDD = item25RDD.flatMap(lambda x: interchange(x)).groupByKey().filter(lambda x: len(list(x[1]))>=5)
#User5itemsRDD = OneRatingRDD.map(lambda x: (x[0][0], (x[0][1],x[1][0]))).groupByKey().filter(lambda x: len(list(x[1]))>=5)

#print(User5itemsRDD.take(1))



itemRDD = User5itemsRDD.flatMap(lambda x: interchange(x)).groupByKey()

#print(len((itemRDD.collect())))


def latFunc(x):
    #result = []
    count =0
    # userList = []
    # for i in fwBC.value:
    #     userList.append(i[0])
    for i in x[1]:
        if i[0] in fwBC.value:
            count+=1

    if count>1:    
        return x

def meanCenter(x):
    result = []
    total = 0
    for i in x[1]:
        #i2 = list(i)
        total+= i[1]
    mean = float(total)/len(x[1])

    for i in x[1]:
        i2 = list(i)
        i2[1] = i2[1]-mean
        result.append((i2[0], i2[1], i2[1]+mean))

    return (x[0],result)
    

def cosine(x):
    l = meanList.value
    item1 = l[0][0]
    item2 = x[0]
    dot =0
    sumA = 0
    sumB = 0
    sim =0
    if item1 != item2:
        for a in l[0][1]:
            for b in x[1]:
                if b[0] == a[0]:
                    dot += a[1]*b[1]
        for a in l[0][1]:
            sumA += a[1]*a[1]
        for b in x[1]:
            sumB += b[1]*b[1]
            #sumB = 1
        
        sim = float(dot)/(np.sqrt(sumA) * np.sqrt(sumB))

    return ((x[0], sim), x[1])
    
def filterUsers(x):
    myList = []
    for itemId in itemList.value:
        myList.append(itemId[0][0])
    #result = []
    count = 0
    if(x[0] not in fwBC.value):
        for i in x[1]:
            if i[0] in myList:
                count+=1
        if(count>=2):
            return x
    #return result
    

def utility(x):
    num =0
    den =0
    for a, b in itemList.value:
        for record in b:
            if(record[0]==x[0]):
                num += a[1]*record[2] 
                den += a[1]
    result = num/den

    return (x[0], result)
    
    
    

input  = sys.argv[2]
inputList = map(str, input.strip('[]').split(','))
for each in inputList:
    myItem = each.strip()
    myItem = myItem.replace("'", '')
    #print(myItem)
    broadv= itemRDD.filter(lambda x : x[0] == myItem).flatMap(lambda x: list(x[1])).map(lambda x: x[0]).collect()
    fwBC = sc.broadcast(broadv)
    neighborRDD = itemRDD.map(lambda x: (x[0], list(x[1]))).filter(lambda x: latFunc(x)).map(lambda x: meanCenter(x)) # a (ii)
    broadList = neighborRDD.filter(lambda x: x[0] == myItem).collect()
    meanList = sc.broadcast(broadList)
    cosineRDD = neighborRDD.map(lambda x: cosine(x)).filter(lambda x: x[0][1]>0)
    mynewRDD = sc.parallelize(cosineRDD.take(50))
    fItems = mynewRDD.collect()
    itemList = sc.broadcast(fItems)
    allUsers = User5itemsRDD.filter(lambda x: filterUsers(x))
    answerRDD = allUsers.map(lambda x: utility(x))#.sortByKey()
    #print(len(answerRDD.collect()))
    print('')
    print('Product id: '+myItem)
    for x in answerRDD.collect():
        print(x)
    print('\n \n')
    

