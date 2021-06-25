package shop

import app.App
import org.apache.commons.lang3.time.DateUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
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
      // 埋点
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_click_log
           |where dt = $dt
           |""".stripMargin).createOrReplaceTempView("dwd_click_log")
      //零售
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_orders_detail
           |where dt=$dt and po_type is null and order_source != 'PC'
           |""".stripMargin).createOrReplaceTempView("orders_retail")
    } else if (timeFlag.equals("week")) {
      log.info("===========> 流量分析模块-周:" + startTime + "and" + dt)
      //零售
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_orders_detail
           |where dt>= $startTime and dt<=$dt and po_type is null
           |""".stripMargin).createOrReplaceTempView("orders_retail")
      // 埋点
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_click_log
           |where dt>= $startTime and dt<=$dt
           |""".stripMargin).createOrReplaceTempView("dwd_click_log")
    }
    flag = timeFlag
  }

  override def process(): Unit = {
    //对 点击数据进行 扩维解析
    spark.sqlContext.uncacheTable("dwd_click_log")
    /** *
     * 平均停留事件
     * UV
     * 用户浏览： 对loginToken 不为null的用户计算停留时间
     * 游客浏览： 对loginToken 为null的用户过滤出来后使用IP 进行分组判断
     * 合并 游客和用户浏览 的时间的出来总时间
     * 1 有用户浏览 无游客浏览
     * 2 有游客浏览 无用户浏览
     * ---------------------------------------
     * 新访客数：
     * 使用用户ID维度
     * 任务时间维度为1天，所以一天后新近用户为新访客数，当天一个用户多次访问只算一次新访客数
     * ---------------------------------------
     * 跳失率：
     * 一天内只访问了一个页面就离开店铺的访客数(PV 为1的用户数)
     * /
     * 店铺总访客数。若所选时间超过1天，访客数跨天去重。
     */
    // 全部数据
    spark.sql(
      s"""
         |select
         |shopId,
         |sum(time) as time,
         |count(1) as uv,
         |sum(one_browse) as one_browse_number
         |from
         |(
         |select
         |shopId,
         |(max(timeIn) - min(timeIn))/1000 as time,
         |case when count(1) > 1 then 0 else 1 end as one_browse,
         |$dt as dt
         |from
         |dwd_click_log
         |where loginToken != '' and loginToken is not null
         |group by shopId,loginToken
         |) a
         |group by shopId
         |""".stripMargin).createOrReplaceTempView("user")
    spark.sql(
      s"""
         |select
         |shopId,
         |sum(time) as time,
         |count(1) as tourist_uv,
         |sum(one_browse) as one_browse_number
         |from
         |(
         |select
         |shopId,
         |(max(timeIn) - min(timeIn))/1000 as time,
         |case when count(1) > 1 then 0 else 1 end as one_browse
         |from
         |dwd_click_log
         |where loginToken == '' or loginToken is null
         |group by shopId,ip
         |) a
         |group by shopId
         |""".stripMargin).createOrReplaceTempView("tourist")
    spark.sql(
      """
        |select  --- 全是访客的数据 和 访客+游客的数据 不包含全是游客的数据
        |a.shopId,
        |case when b.tourist_uv is null
        |then a.uv else a.uv + b.tourist_uv end as uv,
        |case when b.time is null
        |then a.time else a.time + b.time end as time,
        |case when b.one_browse_number is null
        |then a.one_browse_number else a.one_browse_number + b.one_browse_number end as one_browse_number
        |from
        |user a
        |left join
        |tourist b
        |on a.shopId = b.shopId
        |union all
        |select --- 只包含 全是游客的数据
        |b.shopId,
        |b.tourist_uv as uv,
        |b.time as time,
        |b.one_browse_number
        |from
        |user a
        |right join
        |tourist b
        |on a.shopId = b.shopId where a.shopId is null
        |""".stripMargin).createOrReplaceTempView("browse_time")
    // 订单 全部
    spark.sql(
      """
        |select
        |shop_id,
        |count(distinct buyer_id) paid_user_number
        |from
        |orders_retail
        |where paid = 2 and refund = 0
        |group by shop_id
        |""".stripMargin).createOrReplaceTempView("order_paid_user_all_number")
    val pageInfoAll = spark.sql(
      s"""
         |select
         |a.shopId as shop_id,
         |'all' as page_source,
         |a.pv,
         |a.new_user,
         |b.uv,
         |c.paid_user_number,
         |b.one_browse_number / b.uv as lose_ratio,
         |b.time/b.uv as avg_time,
         |a.pv / b.uv as avg_pv,
         |c.paid_user_number / b.uv as visit_paid_ratio,
         |$dt as dt
         |from
         |(
         |select
         |shopId,
         |count(1) as pv,
         |count(distinct userId) as new_user
         |from
         |dwd_click_log
         |group by shopId
         |) a
         |left join
         |browse_time b
         |on a.shopId = b.shopId
         |left join
         |order_paid_user_all_number c
         |on a.shopId = c.shop_id
         |""".stripMargin)
    // 来源划分
    spark.sql(
      s"""
         |select
         |shopId,
         |page_source,
         |sum(time) as time,
         |count(1) as uv,
         |sum(one_browse) as one_browse_number
         |from
         |(
         |select
         |shopId,
         |page_source,
         |(max(timeIn) - min(timeIn))/1000 as time,
         |case when count(1) > 1 then 0 else 1 end as one_browse,
         |$dt as dt
         |from
         |dwd_click_log
         |where loginToken != '' and loginToken is not null
         |group by shopId,loginToken,page_source
         |) a
         |group by shopId,page_source
         |""".stripMargin).createOrReplaceTempView("user")
    spark.sql(
      s"""
         |select
         |shopId,
         |page_source,
         |sum(time) as time,
         |count(1) as tourist_uv,
         |sum(one_browse) as one_browse_number
         |from
         |(
         |select
         |shopId,
         |page_source,
         |(max(timeIn) - min(timeIn))/1000 as time,
         |case when count(1) > 1 then 0 else 1 end as one_browse
         |from
         |dwd_click_log
         |where loginToken == '' or loginToken is null
         |group by shopId,ip,page_source
         |) a
         |group by shopId,page_source
         |""".stripMargin).createOrReplaceTempView("tourist")
    spark.sql(
      """
        |select  --- 全是访客的数据 和 访客+游客的数据 不包含全是游客的数据
        |a.shopId,
        |a.page_source,
        |case when b.tourist_uv is null
        |then a.uv else a.uv + b.tourist_uv end as uv,
        |case when b.time is null
        |then a.time else a.time + b.time end as time,
        |case when b.one_browse_number is null
        |then a.one_browse_number else a.one_browse_number + b.one_browse_number end as one_browse_number
        |from
        |user a
        |left join
        |tourist b
        |on a.shopId = b.shopId and a.page_source = b.page_source
        |union all
        |select --- 只包含 全是游客的数据
        |b.shopId,
        |b.page_source,
        |b.tourist_uv as uv,
        |b.time as time,
        |b.one_browse_number
        |from
        |user a
        |right join
        |tourist b
        |on a.shopId = b.shopId and a.page_source = b.page_source
        |where a.shopId is null
        |""".stripMargin).createOrReplaceTempView("browse_time")
    // 订单来源划分
    spark.sql(
      """
        |select
        |shop_id,
        |order_source,
        |count(distinct buyer_id) paid_user_number
        |from
        |orders_retail
        |where paid = 2 and refund = 0
        |group by shop_id,order_source
        |""".stripMargin).createOrReplaceTempView("order_paid_user_source_number")
    val pageInfoSource = spark.sql(
      s"""
         |select
         |a.shopId as shop_id,
         |a.page_source,
         |a.pv,
         |a.new_user,
         |b.uv,
         |c.paid_user_number,
         |b.one_browse_number / b.uv as lose_ratio,
         |b.time/b.uv as avg_time,
         |a.pv / b.uv as avg_pv,
         |c.paid_user_number / b.uv as visit_paid_ratio,
         |$dt as dt
         |from
         |(
         |select
         |shopId,
         |page_source,
         |count(1) as pv,
         |count(distinct userId) as new_user
         |from
         |dwd_click_log
         |group by shopId,page_source
         |) a
         |left join
         |browse_time b
         |on a.shopId = b.shopId and a.page_source = b.page_source
         |left join
         |order_paid_user_source_number c
         |on a.shopId = c.shop_id and a.page_source = c.order_source
         |""".stripMargin)



    val frame =  pageInfoAll.union(pageInfoSource)
    writerMysql(frame, "shop_clicklog_info", flag)





  }
}
