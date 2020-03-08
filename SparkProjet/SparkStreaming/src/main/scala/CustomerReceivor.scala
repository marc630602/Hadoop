import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver


class CustomerReceivor(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  //启动时调用***
  override def onStart(): Unit = {
    //创建接收数据的线程
    new Thread("CustomerReceivor"){

      override def run(): Unit = {
        receive()
      }
    }.start()

    //定义接收数据方法
    def receive(): Unit = {
      //定义一个Socket
      var socket : Socket = null
      var reader : BufferedReader = null
      try {
        socket = new Socket(host,port)
        //创建一个 BufferedReader 用于读取端口传来的数据
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))
        ////定义一个变量用于接收端口传来数据
        var input : String = reader.readLine()

        //当 receiver 没有关闭并且输入数据不为空，则循环发送数据给 Spark ***
        while (!isStopped() && input != null) {
          store(input)
          input = reader.readLine()
        }
      } catch {
        case e : Exception =>{
          socket.close()
          reader.close()
          restart("Restart")
        }
      }
    }
  }

  //关闭时调用
  override def onStop(): Unit = ???
}
