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
class CommonAnalysis(spark: SparkSession, dt: String, timeFlag: String) extends WriteBase {
  val log = Logger.getLogger(App.getClass)
  var flag = "";
  {
    val startTime = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).minusWeeks(1).toString("yyyyMMdd")
    log.info("===========> 客户分析模块-开始注册UDF函数:")
    UDFRegister.shopMapping(spark, dt)
    if (timeFlag.equals("day")) {
      log.info("===========> 客户分析模块-天:" + dt)
      //区域
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_orders_receive_city
           |where dt=$dt
           |""".stripMargin).createOrReplaceTempView("orders_merge_detail")
    } else if (timeFlag.equals("week")) {
      log.info("===========> 客户分析模块-周:" + startTime + "and" + dt)
      //区域
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_orders_receive_city
           |where dt>= $startTime and dt<=$dt
           |""".stripMargin).createOrReplaceTempView("orders_merge_detail")
    }
    flag = timeFlag
  }

  override def process(): Unit = {
    spark.sqlContext.cacheTable("orders_merge_detail")
    /**
     * 商品省份TOP 10
     * 1.统计支付人数
     * 2.统计支付金额
     * 3.统计支付金额占总支付金额比例
     * 地势分布
     * 包含： 商家交易分析 模块
     */
    val shopProvinceInfoDF =  spark.sql(
      s"""
         |with t1 as(
         |select
         |shop_id,
         |province_name,
         |count(distinct buyer_id) as sale_user_count,
         |cast(sum(total_money) as decimal(10,2)) as sale_succeed_money
         |from
         |orders_merge_detail
         |where paid = 2 and refund = 0
         |group by shop_id,province_name
         |),
         |t2 as (select
         |shop_id,
         |province_name,
         |sale_user_count,
         |sale_succeed_money,
         |sum(sale_succeed_money) over(partition by shop_id) as total_province_money
         |from
         |t1
         |)
         |select
         |shop_id,
         |shop_mapping(shop_id) as shop_name,
         |'all' as order_type,
         |province_name,
         |sale_user_count,
         |sale_succeed_money,
         |cast(sale_succeed_money/total_province_money as decimal(10,4)) as sale_ratio,
         |$dt as dt
         |from
         |t2
         |""".stripMargin).union(spark.sql(
      s"""
         |with t1 as(
         |select
         |shop_id,
         |order_type,
         |province_name,
         |count(distinct buyer_id) as sale_user_count,
         |cast(sum(total_money) as decimal(10,2)) as sale_succeed_money
         |from
         |orders_merge_detail
         |where paid = 2 and refund = 0
         |group by shop_id,order_type,province_name
         |),
         |t2 as (select
         |shop_id,
         |order_type,
         |province_name,
         |sale_user_count,
         |sale_succeed_money,
         |sum(sale_succeed_money) over(partition by shop_id) as total_province_money,
         |row_number() over(partition by shop_id,order_type,province_name order by sale_succeed_money desc) as money_top
         |from
         |t1
         |)
         |select
         |shop_id,
         |shop_mapping(shop_id) as shop_name,
         |order_type,
         |province_name, --省份
         |sale_user_count, --支付人数
         |sale_succeed_money, -- 支付金额
         |cast(sale_succeed_money/total_province_money as decimal(10,4)) as sale_ratio, --支付比例
         |$dt as dt
         |from
         |t2
         |""".stripMargin))
    writerMysql(shopProvinceInfoDF, "shop_province_info", flag)
  }
}


