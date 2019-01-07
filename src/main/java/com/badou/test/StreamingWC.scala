package com.badou.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StateSpec, StreamingContext}

import scala.collection.mutable

object StreamingWC {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\develop\\hadoop-common-2.2.0-bin-master")
    val conf = new SparkConf().setAppName("wc").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))
   // val lines = ssc.textFileStream("D:\\data\\data\\a.txt")

    ssc.checkpoint("D:\\data\\checkpoint")

    //只打印error日志
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val addFunc = (currValues: Seq[Long], prevValueState: Option[Long]) => {
      println("*"*80)
      println("curr:",currValues)
      println("prevValue:",prevValueState)
      //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
      val currentCount = currValues.sum
      // 已累加的值

      //getTime【时间区间】用一个标识
     // val previousCount = None
      val flag=1

      val previousCount = prevValueState.getOrElse(0L)
       //返回累加后的结果，是一个Option[Int]类型
      Some(currentCount + previousCount)
    }

//    def func(c:String,b:String): String ={
//      c+b
//    }
//    val a=(c:String,b:String)=>{c+b}


    val lines = ssc.socketTextStream("master",9999)
    val words = lines.flatMap(_.split(" "))
    println(words)
//    1.只做wordcount
//    val wordCounts = words.map(x => (x, 1))
//      .reduceByKey(_ + _)

//    2.word count窗口操作
//    val wordCounts = words.map((_, 1L))
//          .reduceByKeyAndWindow(_ + _,_ - _,Seconds(4), Seconds(2))

//   3.全局累加操作

    val wordCounts = words.map((_, 1L)).updateStateByKey[Long](addFunc)
    wordCounts.print()
  //  val wordCounts = words.map((_, 1L)).mapWithState(StateSpec.function(w))



//    4.自定义map全局累加操作
//    val map = new mutable.HashMap[String,Int]()
//    words.map((_, 1)).foreachRDD{rdd=>
//      val maps=rdd.collectAsMap()
////      maps.map(x=>map.put(x._1,x._2))
//      map++(maps)
    //}
   // val wordCounts = words.map((_, 1L)).mapWithState()

    ssc.start()
    ssc.awaitTermination()
  }

}
