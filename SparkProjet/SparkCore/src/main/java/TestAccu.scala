import org.apache.spark.{SparkConf, SparkContext}

object TestAccu {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf和SparkContetx
    val conf = new SparkConf().setMaster("local[*]").setAppName("TestMysqlRDD")
    val sc = new SparkContext(conf)

    //2.创建RDD
    val numRDD = sc.parallelize(Array(1, 2, 3, 4, 5, 6), 2)

    //3.创建累加器变量
    var sum = sc.longAccumulator

    //注册(如果是自动的累加器)
    //sc.register(sum,"sum")
    //4.转换为元组并自增1变量
    val numToOne = numRDD.map(x => {
      sum.add(1)
      (x, 1)
    })

    //5.打印结果
    numToOne.foreach(println)
    println("*******************************")
    println(sum.value)

    //6.stop
    sc.stop()
  }
}
