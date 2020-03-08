import org.apache.spark.util.AccumulatorV2

class CustomerAccu extends AccumulatorV2[Int,Int]{
  var sum = 0
  //判断是否为空
  override def isZero: Boolean = sum == 0
  //复制累加器
  override def copy(): AccumulatorV2[Int, Int] = {
    val accu = new CustomerAccu
    accu.sum = this.sum
    accu
  }
  //重置累加器
  override def reset(): Unit = sum = 0
  //累加
  override def add(v: Int): Unit = sum += v
  //合并结果
  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    this.sum += other.value
  }
  //返回结果
  override def value: Int = sum
}
