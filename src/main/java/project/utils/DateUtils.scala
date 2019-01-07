package project.utils

//import org.apache.commons.lang.time.FastDateFormat
import org.apache.commons.lang3.time.FastDateFormat
import java.util.Date

/**
  * 日期时间工具类
  */

object DateUtils {
val YYYYMMDDHHMMSS_FORMAT=FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TRAGE_FORMAT=FastDateFormat.getInstance("yyyyMMddHHmmss")

  def getTime(time:String)={
    YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
  }

  def parseToMinute(time:String)={
    TRAGE_FORMAT.format(new Date(getTime(time)))
  }

  def main(args: Array[String]): Unit = {
    println(parseToMinute("2018-11-26 23:38:01"))
  }


}
