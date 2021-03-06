package com.imooc.stream

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KakfaDirectWordCount{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    if (args.length !=2){
      System.err.println("Usage: KafkaReceiverWordCount <kafkaParams> <topicSet> ")
      System.exit(1)
    }
    val Array(brokers,topics)=args

    val kafkaParams=Map[String,String]("metadata.broker.list"->brokers)

   val sparkConf=new SparkConf()//.setAppName("KakfaReceiverWordCount")
     //.setMaster("local[2]")

    val ssc=new StreamingContext(sparkConf,Seconds(5))

    val topicSet=topics.split(" ").toSet
    val message=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,kafkaParams,topicSet)

    message.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()


  }
}
