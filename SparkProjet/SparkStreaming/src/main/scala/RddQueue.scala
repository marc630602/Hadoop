import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable



object RddQueue {
  def main(args: Array[String]): Unit = {
    //1.create SparkConf and give this app a name
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDQueue")

    //2.create SparkStreamingContext and assign value with conf,set 3s
    val ssc = new StreamingContext(conf,Seconds(5))

    //3.创建RDD队列
    val rddQueue = new mutable.Queue[RDD[Int]]()

    //3.通过RDD队列创建DStream
    val rddStream = ssc.queueStream(rddQueue,oneAtATime = false)

    //4.累加值并打印
    val result = rddStream.reduce(_ + _)
    result.print()

    //5.开启ssc
    ssc.start()
    for (i <- 1 to 5){
      rddQueue += ssc.sparkContext.parallelize(Array(100))
      Thread.sleep(2000)
    }
    ssc.awaitTermination()
  }
}
