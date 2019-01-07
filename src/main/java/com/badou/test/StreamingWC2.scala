package com.badou.test
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StateSpec, StreamingContext}

import scala.collection.mutable
object StreamingWC2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\develop\\hadoop-common-2.2.0-bin-master")
    val conf = new SparkConf().setAppName("wc").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))
    //    val lines = ssc.textFileStream("D:\\data")
    ssc.checkpoint("D:\\data\\checkpoint")
    //只打印error日志
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val lines = ssc.socketTextStream("master",9999)
    val words = lines.flatMap(_.split(" "))



    //    4.自定义map全局累加操作
    val map = new mutable.HashMap[String,Int]()
    words.map((_, 1)).foreachRDD{rdd=>
      val maps=rdd.collectAsMap()
      //      maps.map(x=>map.put(x._1,x._2))
      map++(maps)
    }
    println("#"*80)

   words.print()
    ssc.start()
    ssc.awaitTermination()








  }
}
