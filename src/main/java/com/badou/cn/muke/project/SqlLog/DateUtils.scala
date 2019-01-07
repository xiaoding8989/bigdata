package com.badou.cn.muke.project.SqlLog

import java.util.{Date, Locale}
import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {
  //输入文件日期时间格式
  //10/Nov/2016:00:01:02 +0800
  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  //目标日期格式
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")


  /**
    * 获取时间
    * @param time:yyyy-MM-dd HH:mm:ss
    */
  def parse(time:String)={
    TARGET_FORMAT.format(new Date(getTime(time)))

  }

  def getTime(time:String)={

    try {

      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1,
        time.lastIndexOf("]"))).getTime

    } catch {
      case e:Exception=>{
        0L

      }

    }

  }

  def main(args: Array[String]): Unit = {
  println(parse("[10/Nov/2016:00:01:02 +0800]"))


  }

}
