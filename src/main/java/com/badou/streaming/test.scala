package com.badou.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("window")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))
//    sc.setCheckpointDir("C:\\Users\\Administrator\\Desktop\\test")
    val ds = ssc.socketTextStream("192.168.174.134", 9999)
    //Seconds(5)表示窗口的宽度   Seconds(3)表示多久滑动一次(滑动的时间长度)
    val re = ds.flatMap(_.split(" ")).map((_, 1))
      .reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(20), Seconds(10))
    re.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
