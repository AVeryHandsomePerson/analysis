package com.cn.bju.realtime.etl.bean

import com.alibaba.fastjson.{JSON, JSONObject}
import com.cn.bju.common.bean.CanalRowData
import com.cn.bju.realtime.etl.util.CommonUtils
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.DateUtils
import org.apache.flink.table.shaded.org.joda.time.DateTime

import java.text.SimpleDateFormat
import java.util.Date
import scala.beans.BeanProperty
import scala.util.parsing.json.JSONObject

/**
 * @author ljh
 * @version 1.0
 */
case class ClickLogEntity(@BeanProperty timeIn: String, // '流入时间',
                          @BeanProperty timeOut: String, // '流出时间',
                          @BeanProperty domain: String, // '域名',
                          @BeanProperty url: String, // 'URL',
                          @BeanProperty title: String, // '标题',
                          @BeanProperty referrer: String, // '上一个URL',
                          @BeanProperty sh: Long, // '高',
                          @BeanProperty sw: Long, // '宽',
                          @BeanProperty cd: Long, // '长',
                          @BeanProperty lang: String, // '语言',
                          @BeanProperty shopId: String, // '店铺ID',
                          @BeanProperty ip: String, // 'ip地址',
                          @BeanProperty var province: String, // '省份',
                          @BeanProperty var city: String, // '市',
                          @BeanProperty hour: String, //小时
                          @BeanProperty day: String //天
                         )


object ClickLogEntity {
  def apply(eventType: String): ClickLogEntity = {
    val nObject = JSON.parseObject(eventType)
    val inTime = new DateTime(DateUtils.parseDate(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(nObject.getLong("timeIn"))), "yyyy-MM-dd HH:mm:ss"))
    var outTime = ""
    if (StringUtils.isNotEmpty(nObject.getString("timeOut"))) {
      outTime = new DateTime(DateUtils.parseDate(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(nObject.getLong("timeOut"))), "yyyy-MM-dd HH:mm:ss")).toString("yyyy-MM-dd HH:mm:ss")
    }
    ClickLogEntity(
      inTime.toString("yyyy-MM-dd HH:mm:ss"),
      outTime,
      nObject.getString("domain"),
      nObject.getString("url"),
      nObject.getString("title"),
      nObject.getString("referrer"),
      nObject.getLong("sh"),
      nObject.getLong("sw"),
      nObject.getLong("cd"),
      nObject.getString("lang"),
      nObject.getString("shopId"),
      nObject.getString("ip"),
      "",
      "",
      inTime.toString("HH"),
      inTime.toString("yyyy-MM-dd")
    )
  }
}
