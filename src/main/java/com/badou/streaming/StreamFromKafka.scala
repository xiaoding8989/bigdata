package com.badou.streaming

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.streaming.{rdd, _}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.functions._


/**
  * Created by zheng on 2018/1/19.
  */
object StreamFromKafka {

  case class Order(order_id: String,
                   user_id: String,
                   eval_set: String,
                   order_number: String,
                   order_dow: String,
                   hour: String,
                   day: String)

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.print("Usage: Collect log from Kafka <groupid> <topic> <Execution_time>")
      System.exit(1)
    }
    val group_id="test_group"
    val topic="ding"
    val exectime=5
    val dt="20181111"

   // val Array(group_id, topic, exectime, dt) = args

    //      zookeeper IP:Port
    val zkHostIp = Array("5", "4", "3").map("192.168.95." + _)
    val ZK_QUORUM = zkHostIp.map(_ + ":2181").mkString(",")

    //      val dt = getNowDate()
    val numThreads = 1

    val conf = new SparkConf()
    val ssc = new StreamingContext(conf, Seconds(exectime.toInt))
    val topicSet = topic.split(",").toSet
    val topicMap = topicSet.map((_, numThreads.toInt)).toMap

    //      通过Receiver接收kafka数据
    val mesR = KafkaUtils.createStream(ssc, ZK_QUORUM, group_id, topicMap).map(_._2)
    mesR.print()

    val log=mesR.foreachRDD{rdd=>
      println("*"*80)
      rdd.collect()
      println("*"*80)
      val spark = SparkSession
        .builder()
        .appName("Streaming Form Kafka Static")
        .enableHiveSupport()
        .getOrCreate()
      import spark.implicits._
        val df=rdd.map(x=>x.split(",")).map(x=>Order(x(0),x(1),x(2),x(3),x(4),x(5),x(6))).toDF()
        df.withColumn("dt",lit(dt))
          .write.mode(SaveMode.Append)
          .insertInto("badou.order_partition2")
    }

    ssc.start()
    ssc.awaitTermination()

  }

}

