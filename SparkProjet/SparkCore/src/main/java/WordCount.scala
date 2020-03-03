import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //1.create SparkConf and give this app a name
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //2.create SparkContext and assign value with conf
    val sc = new SparkContext(conf)

    //3.create RDD and executor transmissions and actions
    //sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_, 1).saveAsTextFile(args(1))
    sc.textFile("C:\\Users\\57621\\IdeaProjects\\SparkProjet\\SparkCore\\src\\main\\java\\WordCount.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _, 1).saveAsTextFile("C:\\Users\\57621\\Desktop\\hi")
    //4.close connection
    sc.stop()
  }
}
