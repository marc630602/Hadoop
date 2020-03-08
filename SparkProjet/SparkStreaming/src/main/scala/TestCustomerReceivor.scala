import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestCustomerReceivor {
  def main(args: Array[String]): Unit = {
    //1.create SparkConf and give this app a name
    val conf = new SparkConf().setMaster("local[*]").setAppName("CustomerReceivor")

    //2.create SparkStreamingContext and assign value with conf,set 3s
    val ssc = new StreamingContext(conf,Seconds(3))

    //3.创建DStream
    val lineStreams = ssc.receiverStream(new CustomerReceivor("hadoop101", 888))

    //4.打印
    lineStreams.print()

    //5.启动SSC
    ssc.start()
    ssc.awaitTermination()
  }
}
