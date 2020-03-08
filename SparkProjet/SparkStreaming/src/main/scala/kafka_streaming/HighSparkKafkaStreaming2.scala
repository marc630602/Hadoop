package kafka_streaming

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *  从kafka拉取数据，且需要下线重启后从ck读取，需要经过zk读取checkpoint信息
 */
object HighSparkKafkaStreaming2 {
  def createSCC(): StreamingContext = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("HighKakfaStream")

    //创建Spark StreamingContext对象
    val ssc = new StreamingContext(conf,Seconds(3))

    //设置checkpoint
    ssc.checkpoint("./ck1")

    //kafka参数
    val brokers : String = "hadoop101:9092,hadoop102:9092,hadoop103:9092"
    val topic : String = "first"
    val group : String = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"

    //用map封装kafka参数
    val kafkaPara = Map[String, String](
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )

    //用kafka数据创建DStream
    val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaPara, Set(topic))

    //打印结果
    kafkaStream.print()

    //返回ssc
    ssc
  }

  def main(args: Array[String]): Unit = {
    //Spark StreamingContext
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck1", () => createSCC())

    ssc.start()
    ssc.awaitTermination()
  }
}
