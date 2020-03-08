package wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    //1.create SparkConf and give this app a name
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    //2.create SparkStreamingContext and assign value with conf,set 3s
    val ssc = new StreamingContext(conf,Seconds(3))

    ssc.checkpoint("./ck2")
    //3.创建DStream
    val lineStreams: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop101", 9999)

    //4.切分数据,然后组成元组
    val wordAndOneStreams: DStream[(String, Int)] = lineStreams.flatMap(_.split(" ")).map((_,1))

    //5.统计个数(无状态转换)
    val countStreams: DStream[(String, Int)] = wordAndOneStreams.reduceByKey(_ + _)

    //6.打印
    countStreams.print()

    //7.启动流处理
    ssc.start()
    ssc.awaitTermination()


  }
}
