package project.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import project.dao.CourseClickDAO
import project.dao.CourseSearchClickDAO
import project.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import project.utils.DateUtils

import scala.collection.mutable.ListBuffer

object ImoocStatStreamingApp {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\develop\\hadoop-common-2.2.0-bin-master")

    //master:2181 test streamingtopic 1
    val Array(zkQuorum, groupId, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("ImoocStatStreamingApp").setMaster("local[5]")
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val messages = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)

    val logs=messages.map(_._2)
    val cleandata=logs.map(line=>{
      val infos=line.split("\t")
      println(infos)
      val url=infos(2).split(" ")(1)
      var courseId=0
      if(url.startsWith("/class")){
        val courseIdHTML=url.split("/")(2)
        courseId=courseIdHTML.substring(0,courseIdHTML.lastIndexOf(".")).toInt
      }
      ClickLog(infos(0),DateUtils.parseToMinute(infos(1)),courseId,infos(3).toInt,infos(4))

    }).filter(clicklog=>clicklog.courseId!=0)

    cleandata.map(x=>{
      (x.time.substring(0,8)+"_"+x.courseId,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(partitionRecords=>{
        val list=new ListBuffer[CourseClickCount]
        partitionRecords.foreach(pair=>{
          list.append(CourseClickCount(pair._1,pair._2))
        })
        CourseClickDAO.save(list)
      })
    })



    ssc.start()
    ssc.awaitTermination()

  }

}
