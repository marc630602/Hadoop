import org.apache.spark.{SparkConf, SparkContext}

object AdvertisementClick {
  def main(args: Array[String]): Unit = {
    //1.creaye Sparkconf and Sparkcontetx
    val conf = new SparkConf().setAppName("Advertisement_Click").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //2.read file with those infos(timestamp, province, city, userId, adClick)
    val line = sc.textFile("C:\\Users\\57621\\IdeaProjects\\SparkProjet\\SparkCore\\src\\main\\java\\agent.log")

    //3.get infos in the way that we need as follow: ((province,adClick),1)
    val proAndAdToOne = line.map(x => {
      val fields = x.split(" ")
      ((fields(1), fields(4)), 1)
    })

    //4.calculate the total num of click for each advertisement in every province: ((province,ad),sum)
    val proAndAdToSum = proAndAdToOne.reduceByKey(_ + _)

    //5.change the form of RDD : take province as key and (ad,sum) as value
    val proToAdSum = proAndAdToSum.map(x => (x._1._1, (x._1._2, x._2)))

    //6.group all the advertisement by the same province
    val proGroup = proToAdSum.groupByKey()

    //7.Sort the collection of all ads by number of clicks in the same province and take the first 3
    val proAdTop3 = proGroup.mapValues(x => x.toList.sortWith((x, y) => x._2 > y._2).take(3))

    //8.print the result
    proAdTop3.collect().foreach(println)

    //9.close spark
    sc.stop()
  }
}

