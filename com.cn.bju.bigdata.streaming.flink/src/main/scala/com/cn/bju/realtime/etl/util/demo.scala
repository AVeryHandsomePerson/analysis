package com.cn.bju.realtime.etl.util

import org.apache.commons.lang3.time.DateUtils
import org.apache.flink.table.shaded.org.joda.time.DateTime

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author ljh
 * @version 1.0
 */
object demo {

  def main(args: Array[String]): Unit = {
     val dt = "1622457229000"

//    DateUtils.timeStampToDate()
    val dataTime = new DateTime(DateUtils.parseDate("2019-06-29 15:31:33", "yyyy-MM-dd HH:mm:ss"))
    val dataTime1 = new DateTime(DateUtils.parseDate("2019-06-29 15:31:33", "yyyy-MM-dd HH:mm:ss"))
    println(dataTime.getMillis)
    println(dataTime1.getMillis)
    println(((dataTime.getMillis/1000-dataTime1.getMillis/1000)/3600)/24)
//    dataTime
//    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    println(sdf.format(new Date(dt.toLong)))// 时间戳转换日期




  }

}
