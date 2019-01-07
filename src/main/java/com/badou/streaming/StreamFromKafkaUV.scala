package com.badou.streaming

import com.alibaba.fastjson.JSON
import com.badou.streaming.streamkafkajson.Order
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object StreamFromKafkaUV {
  case class Order(order_id: String,
                   user_id: String,
                   eval_set: String,
                   order_number: String,
                   order_dow: String,
                   hour: String,
                   day: String)

  def main (args: Array[String] ): Unit = {


    if (args.length < 3) {
      System.err.print("Usage: Collect log from Kafka <groupid> <topic> <Execution_time>")
      System.exit(1)
    }


    val Array(group_id, topic, exectime, dt,path) = args
    val zkHostIp = Array("5", "4", "3").map("192.168.95." + _)
    val ZK_QUORUM = zkHostIp.map(_ + ":2181").mkString(",")

    //      val dt = getNowDate()
    val numThreads = 1

    val conf = new SparkConf()
    val ssc = new StreamingContext(conf, Seconds(exectime.toInt))
    val topicSet = topic.split(",").toSet
    val topicMap = topicSet.map((_, numThreads.toInt)).toMap
    ssc.checkpoint("hdfs://master:9000/data/checkpoint")

    val mesR = KafkaUtils.createStream(ssc, ZK_QUORUM, group_id, topicMap).map(_._2)
    println("*"*80)
    mesR.print()
    def rdd2DataFrame(rdd: RDD[String]): DataFrame = {
      //对每个Rdd做处理
      val spark = SparkSession
        .builder()
        .appName("Streaming Form Kafka Static")
        .enableHiveSupport()
        .getOrCreate()
      import spark.implicits._
      rdd.map { x =>
        val mess = JSON.parseObject(x, classOf[Orders])
        Order(mess.order_id,
          mess.user_id,
          mess.eval_set,
          mess.order_number,
          mess.order_dow,
          mess.hour,
          mess.day)
      }.toDF()

    }
    def user_id(x:String):String={
      JSON.parseObject(x,classOf[Orders]).user_id
    }

    //10秒内uv的统计
    val log=mesR.map(x=>JSON.parseObject(x,classOf[Orders]).user_id)
        .map((_,1L))
        .reduceByKeyAndWindow(_+_,_-_,Seconds(10))
    log.print()
    log.saveAsTextFiles(path)


    ssc.start()
    ssc.awaitTermination()

  }

  }




