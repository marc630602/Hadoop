
import org.apache.spark.Partitioner

class CustomerPartitioner(partitions:Int) extends Partitioner{
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    1
  }
}
