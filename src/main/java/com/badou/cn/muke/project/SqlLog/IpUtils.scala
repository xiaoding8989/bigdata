package com.badou.cn.muke.project.SqlLog
import com.ggstar.util.ip.IpHelper


object IpUtils {
  def getCity(ip:String)={
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {

    println(getCity("218.75.35.226"))
  }

}
