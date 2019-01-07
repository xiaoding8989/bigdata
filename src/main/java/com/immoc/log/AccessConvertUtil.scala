package com.immoc.log

import com.badou.cn.muke.project.SqlLog.IpUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object AccessConvertUtil {

  def strtoInt(str:String):String={
    val regex="""([0-9]+)""".r
    str match {
      case regex(str)=>str
      case _=>"0"
    }

  }


  val struct=StructType(
    Array(
      StructField("url",StringType),
      StructField("cmsType",StringType),
      StructField("cmsId",LongType),
      StructField("traffic",LongType),
      StructField("ip",StringType),
      StructField("city",StringType),
      StructField("time",StringType),
      StructField("day",StringType)

    )
  )

  /**
    * 根据输入的每一行信息转换成输出的样式
    *  输入的每一行记录信息
    */

  def parseLog(log:String)={

    try{
      val splits=log.split("\t")
      val url=splits(1)
      val traffic=splits(2).toLong
      var cmsType="-"
      var cmsId=0L
      var city="-"
      val ip=splits(3)

      if (url!="-"){
        val domain="http://www.imooc.com/"

        val cms=url.substring(url.indexOf(domain)+domain.length)

        val cmsTypeId=cms.split("/")

        if(cmsTypeId.length>1){

          cmsType=cmsTypeId(0)
//          cmsId=cmsTypeId(1).toLong
          cmsId=strtoInt(cmsTypeId(1)).toLong
        }

      }
      city=IpUtils.getCity(ip)
      val time=splits(0)
      val day=time.substring(0,10).replaceAll("-","")
      Row(url,cmsType,cmsId,traffic,ip,city,time,day)

    }

    catch {
      case e:Exception=>{
        println(log)
        Row(0)
      }
    }

  }

  def main(args: Array[String]): Unit = {
    val log="2016-11-10 00:01:02\thttp://www.imooc.com/course/program\t10760\t122.234.145.54"
    println(parseLog(log))
  }

}
