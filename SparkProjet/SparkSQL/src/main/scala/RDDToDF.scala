import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


object RDDToDF {
  def main(args: Array[String]): Unit = {
    //1.创建spark对象
    val spark = SparkSession.builder().master("local[*]").appName("RDDToDF").getOrCreate()

    //2.导入隐式转换
    import spark.implicits._

    //3.创建RDD
    val rdd = spark.sparkContext.parallelize(Array(1, 2, 3, 4))

    //4.将RDD[Int]转化成RDD[Row]
    val rowRDD = rdd.map(x => {
      Row(x)
    })

    //5.创建结构信息
    val structType = StructType(StructField("id", IntegerType) :: Nil)

    //6.创建DF
    val df = spark.createDataFrame(rowRDD, structType)

    //7.打印
    df.show

    //8.关闭
    spark.stop()
  }
}
