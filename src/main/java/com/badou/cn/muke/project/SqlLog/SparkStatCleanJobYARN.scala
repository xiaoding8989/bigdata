package com.badou.cn.muke.project.SqlLog

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/***
  * 使用spark完成我们的数据清洗操作运行在yarn之上的
  *
  */

object SparkStatCleanJobYARN {
  def main(args: Array[String]): Unit = {
    if(args.length !=2){
      println("Usage:SparkStatCleanJob<inputPath><outputPath>")
      System.exit(1)
    }
    val Array(inputPath,outputPath)=args

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val spark=SparkSession.builder()
      .getOrCreate()

    val accessRDD=spark.sparkContext.textFile(inputPath)

    val cleandata=accessRDD.map(x=>{
      AccessConvertUtil.parseLog(x)

    }).filter(data=>data.length != 1)

    val accessDF = spark.createDataFrame(cleandata,
      AccessConvertUtil.struct)

//    accessDF.printSchema()
//    accessDF.show(false)

    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
        .partitionBy("day").save(outputPath)

    spark.stop()


  }

}
