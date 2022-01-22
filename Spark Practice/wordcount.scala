val input = sc.textFile("G:/BigData-OLC/DataSet/word1.txt")
val words = input.flatMap(line => line.split(" "))
val mapWord = words.map(line => (line, 1))
val countWord = mapWord.reduceByKey(_+_)
countWord.collect()

countWord.saveAsTextFile("G:/BigData-OLC/DataSet/output/wordcount.txt")
System.exit(0)