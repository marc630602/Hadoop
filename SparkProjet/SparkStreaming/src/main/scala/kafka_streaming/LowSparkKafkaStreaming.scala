package kafka_streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable.HashMap


object LowSparkKafkaStreaming {

  //获取Offset方法
  def getOffset(kafkaCluster: KafkaCluster, group: String, topic: String): _root_.scala.Predef.Map[_root_.kafka.common.TopicAndPartition, Long] = {
    //定义最终返回值：主题分区，Offset 
    var partitionToLong = new HashMap[TopicAndPartition, Long]()

    //根据指定topic获取分区
    val topicAndPartitionsEither: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set(topic))

    //判断分区是否存在
    if (topicAndPartitionsEither.isRight) {
      //当分区存在，获取分区信息
      val topicAndPartitions: Set[TopicAndPartition] = topicAndPartitionsEither.right.get

      //从kafkaCluster获取消费者消费数据的进度
      val topicAndPartitionsToLongEither: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(group, topicAndPartitions)

      //判断offset是否为空
      if (topicAndPartitionsToLongEither.isLeft) {

        //遍历每个分区
        for (topicAndPartition <- topicAndPartitions) {
          //可以使用simpleConsumer获取最小分区数然后赋值
          
          partitionToLong += (topicAndPartition -> 0L)
        }
      }else{
        //取出offset
        val offsets: Map[TopicAndPartition, Long] = topicAndPartitionsToLongEither.right.get
        partitionToLong ++= offsets
      }
    }
    partitionToLong.toMap
  }

  //保存Offset方法
  def saveOffset(kafkaCluster: KafkaCluster, group: String, kafkaDstream: InputDStream[String]) = {

    kafkaDstream.foreachRDD(rdd => {
      //创建offset的map
      var partitionToLong = new HashMap[TopicAndPartition, Long]()

      //取出rdd所有分区的offset范围
      val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      //遍历数组
      for (range <- ranges) {
        partitionToLong += (range.topicAndPartition() -> range.untilOffset)
      }

      //保存数据
      kafkaCluster.setConsumerOffsets(group,partitionToLong)
    })
  }

  def main(args: Array[String]): Unit = {
    //创建配置信息
    val conf = new SparkConf().setMaster("local[*]").setAppName("HighKakfaStream")

    //创建Spark StreamingContext对象
    val ssc = new StreamingContext(conf,Seconds(3))

    //kafka参数
    val brokers : String = "hadoop101:9092,hadoop102:9092,hadoop103:9092"
    val topic : String = "marc"
    val group : String = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"

    //用map封装kafka参数
    val kafkaPara = Map[String, String](
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization,
      "zookeeper.connect" -> "hadoop101:9092,hadoop102:9092,hadoop103:9092"
    )

    //获取Kafkacluster对象
    val kafkaCluster = new KafkaCluster(kafkaPara)

    //获取当前消费的offset
    val fromOffset : Map[TopicAndPartition,Long] = getOffset(kafkaCluster,group,topic)

    //用kafka数据创建DStream
    val kafkaDstream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      ssc,
      kafkaPara,
      fromOffset,
      (messageHandler:MessageAndMetadata[String,String]) => messageHandler.message())

    //打印&保存offset
    kafkaDstream.print()
    saveOffset(kafkaCluster,group,kafkaDstream)

    //启动ssc
    ssc.start()
    ssc.awaitTermination()
  }
}
