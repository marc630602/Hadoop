import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object TestHBase {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf和SparkContetx
    val conf = new SparkConf().setMaster("local[*]").setAppName("TestMysqlRDD")
    val sc = new SparkContext(conf)

    //2.配置HBase配置
    val configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103")
    configuration.set(TableInputFormat.INPUT_TABLE,"rddtable")

    //3.创建RDD
    val hbaseRdd = sc.newAPIHadoopRDD[ImmutableBytesWritable,Result,TableInputFormat](configuration,
                                                                                      classOf[TableInputFormat],
                                                                                      classOf[ImmutableBytesWritable],
                                                                                      classOf[Result])
    //4.打印
    hbaseRdd.foreach(x=>{
      val value = x._2
      println(Bytes.toString(value.getRow))
    })
    //5.关闭
    sc.stop()
  }
}
