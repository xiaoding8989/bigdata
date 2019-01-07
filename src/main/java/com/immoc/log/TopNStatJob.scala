package com.immoc.log

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object TopNStatJob {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val spark=SparkSession.builder().appName("SparkStatCleanJob")
      .config("spark.sql.warehouse.dir", "D:/scala_path/SparkStremBuChong/spark-warehouse")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled","false")
      .master("local[2]").getOrCreate()


    val accessDF=spark.read.format("parquet").load("file:///D:/data/imooc/clean")
    val day="20161110"

    import spark.implicits._
    val commonDF=accessDF.filter($"day"===day && $"cmsType"==="video")
    commonDF.cache()

    //删除课程
    StatDAO.deleteData(day)

    //最受欢迎的TopN课程
    videoAccessTopNStat(spark, accessDF, day)
    //按照地市进行统计TopN课程
   cityAccessTopNStat(spark, accessDF, day)


   videoTrafficsTopNStat(spark,accessDF,day)

    def videoTrafficsTopNStat(spark:SparkSession,accessDF:DataFrame,day:String):Unit={

      val cityAccessTopNDF=commonDF
        .groupBy("day","cmsId").agg(sum("traffic").as("traffics"))
        .orderBy($"traffics".desc)


      cityAccessTopNDF.foreachPartition(partitionOfRecords=>{
        val list=new ListBuffer[DayVideoTrafficsStat]
        try{
          partitionOfRecords.foreach(info=>{

            val day=info(0).toString
            val cmsId=info(1).asInstanceOf[Long]
            val traffics=info(2).asInstanceOf[Long]
            list.append(DayVideoTrafficsStat(day,cmsId,traffics))
          })
          StatDAO.insertDayVideoTrafficsAccessTopN(list)

        }
        catch {
          case e:Exception=>e.printStackTrace()
        }

      })


    }

    def  videoAccessTopNStat(spark:SparkSession,accessDF:DataFrame,day:String):Unit={
      /**
        * 使用DataFrame的方式进行统计
        */

      val videoAccessTopNDF=commonDF
        .groupBy("day","cmsId")
        .agg(count("cmsId").as("times")).orderBy($"times".desc)
     // videoAccessTopNDF.show(false)
      videoAccessTopNDF.foreachPartition(partitionsOfRecords=>{

        try{
          var list=new ListBuffer[DayVideoAccessStat]
          partitionsOfRecords.foreach(info=>{
            val day=info.getAs[String]("day")
            val cmsId=info(1).asInstanceOf[Long]
            val times=info(2).asInstanceOf[Long]
            list.append(DayVideoAccessStat(day,cmsId,times))
          })
          StatDAO.insertvideoAccessTopNStat(list)

        }
        catch{
          case e:Exception=>e.printStackTrace()
        }


      })

    }

    def cityAccessTopNStat(spark:SparkSession,accessDF:DataFrame,day:String):Unit={

      val cityAccessTopNStatDF=commonDF
        .groupBy("day","city","cmsId")
        .agg(count("cmsId").as("times"))
     // cityAccessTopNStatDF.show(false)
      val top3DF=cityAccessTopNStatDF.select(
        cityAccessTopNStatDF("day"),
        cityAccessTopNStatDF("city"),
        cityAccessTopNStatDF("cmsId"),
        cityAccessTopNStatDF("times"),
        row_number().over(Window.partitionBy(cityAccessTopNStatDF("city"))
        .orderBy(cityAccessTopNStatDF("times").desc))
          .as("times_rank")
      ).filter("times_rank<=3")

      try{
        top3DF.foreachPartition(partitionsOfRecords=>{

          val list=new ListBuffer[DayCItyAccessTopNStat]
          partitionsOfRecords.foreach(info=>{
            val day=info(0).toString
            val city=info(1).toString
            val cmsId=info(2).asInstanceOf[Long]
            val times=info(3).asInstanceOf[Long]
            val times_rank=info(4).asInstanceOf[Int]
            list.append(DayCItyAccessTopNStat(day,city,cmsId,times,times_rank))
          })
          StatDAO.insertDayCityAccessTopN(list)

        })


      }
      catch {
        case e:Exception=>e.printStackTrace()
      }



    }
    commonDF.unpersist(true)
  }

}
