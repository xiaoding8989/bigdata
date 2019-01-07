package com.imooc.stream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KakfaReceiverWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    if (args.length !=4){
      System.err.println("Usage: KafkaReceiverWordCount <zkQuorum> <group> <topics> <numThreads>")
    }
    val Array(zkQuorum,groupId,topics,numThreads)=args
    val topicMap=topics.split(",").map((_,numThreads.toInt)).toMap

   val sparkConf=new SparkConf()//.setAppName("KakfaReceiverWordCount")
     //.setMaster("local[2]")

    val ssc=new StreamingContext(sparkConf,Seconds(5))
    val message=KafkaUtils.createStream(ssc,zkQuorum,groupId,topicMap)

    message.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()


  }
}
