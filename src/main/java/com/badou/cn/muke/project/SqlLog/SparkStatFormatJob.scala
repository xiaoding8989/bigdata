package com.badou.cn.muke.project.SqlLog

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark=SparkSession.builder().appName("SparkSQLproj")
      .master("local[2]").getOrCreate()
    val access=spark.sparkContext.textFile("file:///D:/bigdata/mooc_access.log")

    access.map(line=>{
      val splits=line.split(" ")
      var ip=""
      var url=""
      var time=""
      var traffic=""
      if (splits.length>11){
        val ip=splits(0)

        /**
          * 原始日志的第三个和第四个字段拼接起来就是完整的访问时间：
          * [10/Nov/2016:00:01:02 +0800] ==> yyyy-MM-dd HH:mm:ss
          */
        val time=splits(3)+" "+splits(4)
        val url=splits(11).replaceAll("\"","")
        //流量
        val traffic=splits(9)
        if (url !="-"){
          DateUtils.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip

        }
        else
          {""}


      }
      else {
        ""
      }

//filter(x=>x !="").saveAsTextFile("file:///D:/data/output3")


    }).filter(x=>x !="").saveAsTextFile("file:///D:/data/output")

    spark.stop()
  }


}
