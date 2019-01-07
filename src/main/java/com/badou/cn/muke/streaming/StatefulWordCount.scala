package com.badou.cn.muke.streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds,StreamingContext}


object StatefulWordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\develop\\hadoop-common-2.2.0-bin-master")
    val sparkConf=new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("D:\\data\\checkpoint")
    def updateFunction(currentValues:Seq[Int],preValues:Option[Int]):Option[Int]={
      val current=currentValues.sum
      val pre=preValues.getOrElse(0)
      Some(current+pre)

    }

    val lines = ssc.socketTextStream("master",9999)

    val result = lines.flatMap(_.split(" ")).map((_,1))

    val state=result.updateStateByKey[Int](updateFunction _)
    //val state=result.updateStateByKey[Int](updateFunction)
    state.print()


    ssc.start()
    ssc.awaitTermination()
  }

}
