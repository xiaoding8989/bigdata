package com.badou.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(",")(1))
//    窗口大小10秒，滑动大小2秒
//    val wordCounts = words.map((_, 1L))
//      .reduceByKeyAndWindow(_ + _,_ - _,Seconds(10), Seconds(2))

    val addFunc = (currValues: Seq[Long], prevValueState: Option[Long]) => {
      //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
      val currentCount = currValues.sum
      // 已累加的值
      val previousCount = prevValueState.getOrElse(0L)
      // 返回累加后的结果，是一个Option[Int]类型
      Some(currentCount + previousCount)
    }
    val wordCounts = words.map((_, 1L)).updateStateByKey[Long](addFunc)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
