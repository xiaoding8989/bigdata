package com.badou.cn.muke
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object testKafka {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\develop\\hadoop-common-2.2.0-bin-master")

    val Array(zkQuorum, groupId, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("ImoocStatStreamingApp").setMaster("local[5]")
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val messages = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)
    messages.map(_._2).count().print
    ssc.start()
    ssc.awaitTermination()


  }




}
