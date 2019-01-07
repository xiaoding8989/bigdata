package com.imooc.stream
import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ForeachRDDApp {


  /**
    * 获取mysql的连接
    * @return
    */

  def createConnection()={
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_project", "root", "123456")
  }



  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
   val sparkConf=new SparkConf().setAppName("ForeachRDDApp").setMaster("local[2]")
    val ssc=new StreamingContext(sparkConf,Seconds(10))
    //ssc.checkpoint("D:\\data\\checkpoint")

    val lines=ssc.socketTextStream("master",9999)
    val result=lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    result.print()
  //  val state=result.updateStateByKey[Int](updateFunction _)
    //state.print()
//    result.foreachRDD(rdd=>{
//      val connection=createConnection()
//            try{
//              rdd.foreach{
//                record=>
//                  val sql="insert into wordcount(word,wordcount) values('"+record._1 +"'," + record._2 +")"
//                  connection.createStatement().execute(sql)
//
//              }
//            }
//            catch {
//              case e:Exception=>e.printStackTrace()
//            }
//    })


    result.foreachRDD(rdd=>{
      rdd.foreachPartition(partitionOfRecords=>{
        val connection=createConnection()
        partitionOfRecords.foreach(
          record=>{
            val sql="insert into wordcount(word,wordcount) values('"+record._1 +"'," + record._2 +")"
            println(sql)
            connection.createStatement().execute(sql)
          })
        connection.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()


  }
}
