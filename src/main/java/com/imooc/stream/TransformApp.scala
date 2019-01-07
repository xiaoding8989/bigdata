package com.imooc.stream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformApp {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
   val sparkConf=new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc=new StreamingContext(sparkConf,Seconds(5))

    /**
      * 构建黑名单
      */


    /**
      *
      * (zs,20181231,zs)
      * (ls,20181231,ls)
      * (ww,20181231,ww)
      * ==================
      * (ww,(20181231,ww,None))
      * (ls,(20181231,ls,Some(true)))
      * (zs,(20181231,zs,Some(true)))
      *
      */

    val blacks=List("zs","ls")
    val blacksRDD=ssc.sparkContext.parallelize(blacks).map((_,true))
    blacksRDD.foreach(println)

    val lines=ssc.socketTextStream("master",9999)
    val clicklog=lines.map(x=>(x.split(",")(1),x))
        .transform(rdd=>{
          rdd.leftOuterJoin(blacksRDD).filter(x=>x._2._2.getOrElse(false)!=true)
            .map(x=>x._2._1)
        })
      clicklog.print()

    ssc.start()
    ssc.awaitTermination()




  }
}
