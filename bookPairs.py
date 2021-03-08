from pyspark import SparkContext
sc = SparkContext("local", "BookPairs")

data = sc.textFile("goodreads.user.books")
pairs = data.flatMap(lambda line: [(m,s) for m in line.split(":")[1].split(",") for s in line.split(":")[1].split(",") if s<m])
pairs1 = pairs.map(lambda p:(p,1))
pairs0 = pairs1.reduceByKey(lambda a,b:a+b)
pairs2 = pairs0.filter(lambda x: x[1] > 20)
pairs2.saveAsTextFile("output")
