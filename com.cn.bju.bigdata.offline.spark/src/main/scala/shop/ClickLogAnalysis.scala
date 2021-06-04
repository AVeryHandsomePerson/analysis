package shop

import app.App
import org.apache.commons.lang3.time.DateUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import udf.UDFRegister

/**
 * @author ljh
 * @version 1.0
 */
class ClickLogAnalysis(spark: SparkSession, dt: String, timeFlag: String) extends WriteBase {

  val log = Logger.getLogger(App.getClass)
  var flag = "";
  {
    val startTime = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).minusWeeks(1).toString("yyyyMMdd")
    log.info("===========> 流量分析模块-开始注册UDF函数:")
    UDFRegister.FileIpMapping(spark)
    //    UDFRegister.FileIpMapping(spark)
    if (timeFlag.equals("day")) {
      log.info("===========> 流量分析模块-天:" + dt)
      spark
        .read
        .json(s"hdfs://bogon:8020/click_log/${dt}/")
        .createOrReplaceTempView("ods_click_log")
    } else if (timeFlag.equals("week")) {
      log.info("===========> 流量分析模块-周:" + startTime + "and" + dt)
      spark
        .read
        .json(s"hdfs://bogon:8020/click_log/${dt}/")
        .createOrReplaceTempView("click_log")
    }
    flag = timeFlag
  }

  override def process(): Unit = {
    //对 点击数据进行 扩维解析
    spark.sql(
      """
        |select
        |ip,
        |cd,
        |domain,
        |ip,
        |lang,
        |referrer,
        |sh,
        |shopId,
        |sw,
        |timeIn,
        |timeOut,
        |title,
        |url,
        |split(ip_mapping(ip),',')[0] as province,
        |split(ip_mapping(ip),',')[1] as city
        |from
        |ods_click_log
        |""".stripMargin).createOrReplaceTempView("dwd_click_log")


    val frame = spark.sql(
      s"""
        |
        |select
        |shopId,
        |count(1) as pv,
        |count(distinct ip) as uv,
        |$dt
        |from
        |dwd_click_log
        |group by shopId
        |""".stripMargin)
    writerMysql(frame, "shop_click_log_info", flag)
  }
}
