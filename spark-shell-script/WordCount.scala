// WordCount.scala
// execute: cat WordCount.scala | spark-shell

// take five first lines
val textFile = sc.parallelize(sc.textFile("data.txt").take(5))
// map/reduce to count the number of occurrences of each word
val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
// print RDD
counts.collect().foreach(println)
// transform to sql dataframe
val columnNames = Seq("word", "count")
val df = counts.toDF(columnNames: _*)
df.createOrReplaceTempView("word_count")
// print sql table
val sqlResult = spark.sql("SELECT * FROM word_count")
sqlResult.show()
// display each word by decreasing order of occurrence
val sqlOrderResult = spark.sql("SELECT * FROM word_count ORDER BY count DESC")
sqlOrderResult.show()

// words in lowercase
val countsWithoutCaseTmp = textFile.map(word => (word.toLowerCase()))
// print RDD
countsWithoutCaseTmp.collect().foreach(println)
// map/reduce to count the number of occurrences of each word
val countsWithoutCase = countsWithoutCaseTmp.map(word => (word, 1)).reduceByKey(_ + _)
// transform to sql dataframe
val dfWithoutCase = countsWithoutCase.toDF(columnNames: _*)
dfWithoutCase.createOrReplaceTempView("word_count_without_case")
// print sql table
val sqlWithoutCaseResult = spark.sql("SELECT * FROM word_count_without_case")
sqlWithoutCaseResult.show()


