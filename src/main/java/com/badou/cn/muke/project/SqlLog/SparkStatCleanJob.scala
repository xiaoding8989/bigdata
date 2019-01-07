package com.badou.cn.muke.project.SqlLog

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/***
  * 使用spark完成我们的数据清洗操作
  *
  */

object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val spark=SparkSession.builder().appName("SparkStatCleanJob")
      .config("spark.sql.warehouse.dir", "D:/scala_path/SparkStremBuChong/spark-warehouse")
      //.config("spark.sql.parquet.compression.codec","gzip")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .master("local[2]").getOrCreate()

    val accessRDD=spark.sparkContext.textFile("file:///D:/bigdata/access.log")

    val cleandata=accessRDD.map(x=>{
      AccessConvertUtil.parseLog(x)

    }).filter(data=>data.length != 1)

    val accessDF = spark.createDataFrame(cleandata,
      AccessConvertUtil.struct)

    accessDF.printSchema()
    accessDF.show(false)
    try{
      accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
        .partitionBy("day").save("file:///D:/bigdata/output/clean4")

    }
    catch {
      case e:Exception=>{
        println(e)
      }
    }


    spark.stop()


  }

}
