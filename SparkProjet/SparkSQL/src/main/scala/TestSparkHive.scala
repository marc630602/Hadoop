import org.apache.spark.sql.SparkSession

object TestSparkHive {
  def main(args: Array[String]): Unit = {
    //1.创建spark对象
    val spark = SparkSession.builder().master("local[*]").appName("SparkHive").enableHiveSupport().getOrCreate()

    spark.sql("create table aaa(id int)")

    spark.stop()

  }
}
