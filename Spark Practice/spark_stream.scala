import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
val ssc = new StreamingContext(sc, Seconds(5))
val lines = ssc.socketTextStream("localhost", 9998)
val valpair = lines.flatMap(x => x.split(' '))
val words = valpair.map(x => (x,1))
val wordCounts = words .reduceByKey(_+_)
wordCounts.print()
ssc.start()