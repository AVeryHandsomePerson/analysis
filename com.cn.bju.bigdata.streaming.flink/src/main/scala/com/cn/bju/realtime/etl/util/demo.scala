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
//    val dataTime = new DateTime(DateUtils.parseDate(dt, "yyyy-MM-dd HH:mm:ss"))
//    dataTime
//    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    println(sdf.format(new Date(dt.toLong)))// 时间戳转换日期
  }

}
