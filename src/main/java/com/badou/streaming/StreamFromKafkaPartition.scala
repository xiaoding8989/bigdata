package com.badou.streaming

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.streaming.{rdd, _}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.functions._

object StreamFromKafkaPartition {
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

    val Array(group_id, topic, exectime, dt) = args

    //      zookeeper IP:Port
    val zkHostIp = Array("5", "4", "3").map("192.168.95." + _)
    val ZK_QUORUM = zkHostIp.map(_ + ":2181").mkString(",")

    //      val dt = getNowDate()
    val numThreads = 1

    val conf = new SparkConf()
    val ssc = new StreamingContext(conf, Seconds(exectime.toInt))
    val topicSet = topic.split(",").toSet
    //numThreads.toInt一个topic对应多少个线程
    val topicMap = topicSet.map((_, numThreads.toInt)).toMap
    //ssc.checkpoint("")

    //      通过Receiver接收kafka数据
    val mesR = KafkaUtils.createStream(ssc, ZK_QUORUM, group_id, topicMap).map(_._2)
    println("*"*80)
    mesR.print()
    def rdd2DataFrame(rdd:RDD[String]):DataFrame={
      val spark = SparkSession
        .builder()
        .appName("Streaming Form Kafka Static")
        .config("hive.exec.dynamic.partition", "true") //分区表设定的参数
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
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


    val log = mesR.foreachRDD { rdd =>
      val df=rdd2DataFrame(rdd)
      df.withColumn("dt",lit(dt))
        .write.mode(SaveMode.Append)
        .insertInto("badou.order_partition")

    }

    ssc.start()
    ssc.awaitTermination()

  }


}
