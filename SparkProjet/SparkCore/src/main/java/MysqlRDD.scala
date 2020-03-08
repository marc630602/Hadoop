import java.sql.{DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object MysqlRDD {

  def main(args: Array[String]): Unit = {
    //1.创建SparkConf和SparkContetx
    val conf = new SparkConf().setMaster("local[*]").setAppName("TestMysqlRDD")
    val sc = new SparkContext(conf)

    //2.设置MySQL参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop101:3306/rdd"
    val userName = "root"
    val passWd = "123456"

    //3.创建MYSQL的RDD
    val jdbc = new JdbcRDD[Int](sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, userName, passWd)
    },
      "select * from emp where ? >= id and ? <= id;",
      1,
      5,
      1,
      (x: ResultSet) => x.getInt(1)
    )

    //4.打印结果
    jdbc.collect
    //5.关闭连接
    sc.stop()
  }

}
