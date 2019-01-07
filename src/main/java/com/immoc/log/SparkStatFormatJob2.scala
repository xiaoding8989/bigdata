package com.immoc.log

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkStatFormatJob2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark=SparkSession.builder().appName("SparkStatCleanJob")
      .config("spark.sql.warehouse.dir", "D:/scala_path/SparkStremBuChong/spark-warehouse")
      //.config("spark.sql.parquet.compression.codec","gzip")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .master("local[2]").getOrCreate()
    val accessRDD=spark.sparkContext.textFile("file:///D:/bigdata/mooc_access.log")
 // accessRDD.take(10).foreach(println)
   accessRDD.map(
     line=>{
       try{


         val splits=line.split(" ")
         val ip=splits(0)
         var time=splits(3)+" "+splits(4)
         time=DateUtils.parse(time)
         val url=splits(11).replaceAll("\"","")
         val traffic=splits(9)
         //(ip,time,url,traffic)
         time+"\t"+url+"\t"+traffic+"\t"+ip

       }
       catch {
         case e:Exception=>{
           println("*"*90)


         }
       }


   }).saveAsTextFile("file:///D:/data/output9")

    spark.stop()


  }

}
