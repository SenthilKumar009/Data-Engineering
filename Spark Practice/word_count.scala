import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object word_count extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR) // telling the machine to show if error exists, else show the output.
  
  val sc = new SparkContext("local[*]", "wordcount")
  /* Create Spark Context
   * local -> specify cluster is on local
   * * -> Use all the CPU cores in the current machine
   * wordcount -> application name
  */
  
  val input = sc.textFile("/Users/Dell/Downloads/Dataset/search_data.txt")
  /*
   * textFile -> specify the location of the file
   * input -> its a RDD  
   */
  
  val words = input.flatMap(x => x.split(" "))
  /*
   * flatmap() -> flatten out the structure to split all the sentance into word
   * 					 -> flatmap always returns the output value more than the input value
   * split() -> split the line by word by word
   */
  
  val wordsLower = words.map(x => x.toLowerCase())
  /*
   * toLowerCase() -> to convert the given string into lower case
   * map() -> return the exact number of output as the given input
   */
  
  val wordMap = wordsLower.map(x => (x, 1))
  // map the value 1 to all the given word present in the RDD
  
  val output = wordMap.reduceByKey((x,y) => x+y)
  /*
   * reduceByKey() -> always works on the two values
   * get the two same key at a time and sum it of and return it
   * input  -> (big, 1), (data, 1), (data, 1), (analysis, 1)
   * output -> (big, 1), (data, 2), (analysis, 1)
   */
  
  val reverseKeyValue = output.map(x => (x._2, x._1)) 
  // Change the key and value's position to sort the count based on the descending order
  
  /* val sortedResult = finalCount.sortBy(x => x._2)
   * sorBy() is better than sortByKey and we can pass the value directly as a parameter to sort the value
  */
  
  val sortOuttput = reverseKeyValue.sortByKey(false)  //this is not required
  // Sort the key in descending order by passing "false" as a parameter
  
  val finalOutput = sortOuttput.map(x => (x._2, x._1))//this is not required
  // Again change the key and value's position to displaying purpose
  
  val results = finalOutput.collect
  /*
   * Call the actions (collect) to run the complete execution. until we call collect() nothing will happen from spark side and once,
   * the compiler has seen the collect it will execute the job based in DAG
   */
  
  for (result <- results){
    val word = result._1
    val count = result._2
    println(s"$word : $count")
  }
  
  /* input
   * .flatMap(_.split(" "))
   * .map(_.toLowerCase())
   * .map((_, 1))
   * .reduceByKey(_ + _)
   * .collect.foreach(println)
   * This single like code is a way to combine the code together and execute it better way.
   * _ use as a variable to minimize the code further.
  */ 
  
  scala.io.StdIn.readLine()
  // It helps to run the job indefinitely to view the DAG map inorder to understand the process. 
}