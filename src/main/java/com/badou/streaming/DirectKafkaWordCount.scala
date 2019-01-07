package com.badou.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object DirectKafkaWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
//    smallest和from beiginning是一样的
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "auto.offset.reset"->"smallest"
    )


//    生成Dstream
    val messages = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(",")(1))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // 开始计算
    ssc.start()
    ssc.awaitTermination()

  }

}
