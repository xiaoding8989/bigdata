package com.badou.cn.muke.project.SqlLog

import org.apache.spark.sql.expressions.Window
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession, expressions}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * TopN统计Spark作业,dataframe与sql方式进行对比
  */

object TopNStatJob {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val spark=SparkSession.builder().appName("SparkStatCleanJob")
      .config("spark.sql.warehouse.dir", "D:/scala_path/SparkStremBuChong/spark-warehouse")
      .master("local[2]").getOrCreate()
    val accessDF=spark.read.format("parquet").load("file:///D:/bigdata/output/clean2")
    val day="20161110"

  //  accessDF.show(100)

    StatDAO.deleteData(day)

//    accessDF.printSchema()
//    accessDF.show(false)
    //最受欢迎的TopN课程
  videoAccessTopNStat(spark,accessDF)
    //按照地市进行统计TopN课程
    cityAccessTopNStat(spark,accessDF)
    //按照流量进行统计
    videoTrafficsTopnStat(spark,accessDF,day)


  spark.stop()
  }

def videoTrafficsTopnStat(spark:SparkSession,accessDF:DataFrame,day:String):Unit={
  import spark.implicits._
  val cityAccessTopNDF=accessDF.filter($"day"===day && $"cmsType"==="video")
    .groupBy("day","cmsId").agg(sum("traffic").as("traffics"))
    .orderBy($"traffics".desc)
    //.show(false)

  try{
    cityAccessTopNDF.foreachPartition(partitionOfRecords=>{
      val list=new ListBuffer[DayVideoTrafficsStat]
      for (elem <- partitionOfRecords) {
        val day=elem(0).toString
        val cmsId=elem(1).asInstanceOf[Long]
        val traffics=elem(2).asInstanceOf[Long]
        list.append(DayVideoTrafficsStat(day,cmsId,traffics))
      }
      StatDAO.insertDayVideoTrafficsAccessTopN(list)
    })

  }catch {
    case e:Exception=>e.printStackTrace()
  }

}

  def cityAccessTopNStat(spark:SparkSession,accessDF:DataFrame):Unit={
    import spark.implicits._
    val cityAccessTopNDF=accessDF.filter($"day"==="20161110" && $"cmsType"==="video")
      .groupBy("day","city","cmsId")
      .agg(count("cmsId").as("times"))
    //cityAccessTopNDF.show(false)

    //Window函数在Spark SQL的使用在分组里面求排序以城市进行分区在内部进行排序
    val top3DF=cityAccessTopNDF.select(cityAccessTopNDF("day"),
      cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city"))
      .orderBy(cityAccessTopNDF("times").desc))
        .as("times_rank")
    ).filter("times_rank<=3")//.show(false)

    try{
      top3DF.foreachPartition(partitionOfRecords=>{
        val list=new ListBuffer[DayCityVideoAccessStat]
        partitionOfRecords.foreach(info=>{
          val day=info(0).toString
          val city=info(2).toString
          val cmsId=info(3).asInstanceOf[Long]
          val times=info(4).asInstanceOf[Long]
          val timesRank=info(5).asInstanceOf[Int]
          list.append(DayCityVideoAccessStat(day,cmsId,city,times,timesRank))

        })
        StatDAO.insertDayCityVideoAccessTopN(list)
      })

    }catch {
      case e:Exception=>e.printStackTrace()
    }


  }


  /**
    * 最受欢迎的TopN课程
    */
  def videoAccessTopNStat(spark:SparkSession,accessDF:DataFrame):Unit={
    /**
      * 使用DataFrame的方式进行统计
      */


   import  spark.implicits._
    val videoAccessTopNDF=accessDF.filter($"day"==="20161110" && $"cmsType"==="video")
      .groupBy("day","cmsId").agg(count("cmsId")
      .as("times")).orderBy($"times".desc)
   // videoAccessTopNDF.show(false)

    /**
      * 使用SQL的方式进行统计
      */

  //accessDF.createOrReplaceTempView("access_logs")
//    val videoAccessTopNDF=spark.sql("select day,cmsId,count(1) as times " +
//      "from access_logs " +
//      "where day='20161110' and cmsType='video'" +
//      " group by day,cmsId order by times desc")

  //  videoAccessTopNDF.show(false)

    /**
      * 将结果写入msyql中
      */

    try{
      videoAccessTopNDF.foreachPartition(partitionOfRecords=>{
        val list=new ListBuffer[DayVideoAccessStat]
        partitionOfRecords.foreach(info=>{

         // println(info(0).getClass,info(1).getClass,info(2).getClass)
          val day=info(0).toString
          val cmsId=info(1).asInstanceOf[Long]
          val times=info(2).asInstanceOf[Long]
          list.append(DayVideoAccessStat(day,cmsId,times))

        })
        StatDAO.insertDayVideoAccessTopN(list)
      })


    }
catch {
  case e:Exception=>e.printStackTrace()
}


  }

}
