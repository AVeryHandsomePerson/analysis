package shop

import app.App
import common.StarvConfig
import org.apache.commons.lang3.time.DateUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import udf.UDFRegister

/**
 * @author ljh
 * @version 1.0
 */
class ClientAnalysis(spark: SparkSession,var dt: String, timeFlag: String) extends WriteBase {

  val log = Logger.getLogger(App.getClass)
  var flag = "";

  {
    val startTime = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).minusWeeks(1).toString("yyyyMMdd")
    log.info("===========> 客户分析模块-开始注册UDF函数:")
    UDFRegister.clientMapping(spark, dt)
    // 会员数据
    spark.sql(
      s"""
         |select
         |*
         |from
         |dwd.dwd_user_statistics
         |where dt = $dt
         |""".stripMargin).createOrReplaceTempView("user_statistics")
    // 客户
    spark.sql(
      s"""
         |select
         |*
         |from
         |dwd.dwd_shop_store
         |where dt = $dt
         |""".stripMargin).createOrReplaceTempView("shop_store")
    if (timeFlag.equals("day")) {
      log.info("===========> 客户分析模块-天:" + dt)
      //零售
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_orders_detail
           |where dt=$dt and po_type is null and paid = 2
           |""".stripMargin).createOrReplaceTempView("orders_retail")
      //采购
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_orders_detail
           |where dt=$dt and po_type = 'PO'
           |""".stripMargin).createOrReplaceTempView("purchase_tmp")
      //埋点
      spark.sql(
        s"""
           |select
           |shopId,
           |userId
           |from
           |dwd.dwd_click_log
           |where dt=$dt and userId is not null and userId != ''
           |""".stripMargin).createOrReplaceTempView("dwd_click_log")
    }
    else if (timeFlag.equals("week")) {
      log.info("===========> 客户分析模块-周:" + startTime + "and" + dt)
      //零售
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_orders_detail
           |where dt>= $startTime and dt<=$dt and po_type is null  and paid = 2
           |""".stripMargin).createOrReplaceTempView("orders_retail")
      //采购
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_orders_detail
           |where dt>= $startTime and dt<=$dt and po_type = 'PO'
           |""".stripMargin).createOrReplaceTempView("purchase_tmp")
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
    else if (timeFlag.equals("month")){
      val startTime = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).toString("yyyyMM")
      dt = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).dayOfMonth().withMinimumValue().toString("yyyyMMdd")
      log.info("===========> 客户分析模块-月:"+ dt)
      //零售
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_orders_detail
           |where dt like '$startTime%' and po_type is null  and paid = 2
           |""".stripMargin).createOrReplaceTempView("orders_retail")
      //采购
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_orders_detail
           |where dt like '$startTime%' and po_type = 'PO'
           |""".stripMargin).createOrReplaceTempView("purchase_tmp")
      //埋点
      spark.sql(
        s"""
           |select
           |shopId,
           |userId
           |from
           |dwd.dwd_click_log
           |where dt like '$startTime%'
           |""".stripMargin).createOrReplaceTempView("dwd_click_log")
    }
    flag = timeFlag
  }


  override def process(): Unit = {
    val intraDay = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).toString("yyyy-MM-dd")

    /**
     * 客户数-- 店铺维度:
     * 筛选时间内，付款成功的客户数，一人多次付款成功记为一人
     * 新成交客户数：过去1年没有购买，在筛选时间内首次在店铺付款的客户数量
     * 老成交客户数：过去1年购买过，在筛选时间内在店铺再次付款的客户数量
     * and
     * 客户数占比：全部成交客户占比：筛选时间成交客户数 / 累积所有客户数
     * 新成交客户占比：筛选时间新成交客户数 / 全部成交客户数
     * 老成交客户占比：筛选时间老成交客户数 / 全部成交客户数
     */
    spark.sql(
      s"""
         |select
         |a.shop_id, --店铺ID
         |a.order_id, -- 订单号
         |a.buyer_id, -- 买方用户ID
         |a.create_time, -- 创建时间
         |a.order_type,
         |a.paid,
         |a.refund,
         |a.payment_total_money,
         |b.final_time,
         |b.last_time,
         |case when datediff(b.final_time,b.last_time) <= 365 then 1
         |else 0 end as flag_years, --满足返回1为1年内购买过的,不满足返回0为1年内没购买的 或 用户为空的
         |case when a.buyer_id is not null and b.final_time is null then 1 else 0 end as present_day -- 1为新用户
         |from
         |orders_retail a
         |left join
         |(
         |select
         |*
         |from
         |dwd.dwd_user_order_locus
         |where dt = $dt and (final_time = '$intraDay' or first_time = '$intraDay')
         |) b
         |on a.buyer_id = b.buyer_id and a.shop_id = b.shop_id
         |""".stripMargin).createOrReplaceTempView("order_tmp")
    //分平台
    spark.sql(
      """
        |select
        |shop_id,
        |order_type,
        |count(distinct buyer_id) as user_dis_number, --累计所有的成交用户数
        |cast(sum(case when paid = 2 and refund = 0 then payment_total_money else 0 end)as  decimal(10,2)) as sale_succeed_money, --成交金额
        |cast(sum(case when paid = 2 and refund = 0 and present_day = 1 then payment_total_money else 0 end)as  decimal(10,2)) as new_user_succeed_money, --成交金额
        |cast(sum(case when paid = 2 and refund = 0 and present_day != 1 then payment_total_money else 0 end)as  decimal(10,2)) as aged_user_succeed_money, --成交金额
        |count(distinct case when paid = 2 then buyer_id end) as present_user_dis_number, --当天成交的用户数
        |count(distinct case when flag_years = 1 then buyer_id end) as aged_user_dis_number, -- 当天成交的老用户数
        |count(distinct case when flag_years = 0 and present_day = 1 then buyer_id end) as new_user_dis_number -- 当天成交的新用户数
        |from
        |order_tmp
        |where order_type='TC'
        |group by shop_id,order_type
        |""".stripMargin).createOrReplaceTempView("client_order_tc")
    spark.sql(
      """
        |select
        |shop_id,
        |order_type,
        |count(distinct buyer_id) as user_dis_number, --累计所有的订单用户数
        |cast(sum(case when paid = 2 and refund = 0 then payment_total_money else 0 end)as  decimal(10,2)) as sale_succeed_money, --成交金额
        |cast(sum(case when paid = 2 and refund = 0 and present_day = 1 then payment_total_money else 0 end)as  decimal(10,2)) as new_user_succeed_money, --成交金额
        |cast(sum(case when paid = 2 and refund = 0 and present_day != 1 then payment_total_money else 0 end)as  decimal(10,2)) as aged_user_succeed_money, --成交金额
        |count(distinct case when paid = 2 then buyer_id end) as present_user_dis_number, --当天成交的用户数
        |count(distinct case when flag_years = 1 then buyer_id end) as aged_user_dis_number, -- 当天成交的老用户数
        |count(distinct case when flag_years = 0 and present_day = 1 then buyer_id end) as new_user_dis_number -- 当天成交的新用户数
        |
        |from
        |order_tmp
        |where order_type='TB'
        |group by shop_id,order_type
        |""".stripMargin).createOrReplaceTempView("client_order_tb")
    //全平台
    spark.sql(
      """
        |select
        |shop_id,
        |'all' as order_type,
        |count(distinct buyer_id) as user_dis_number, --累计所有的成交用户数
        |cast(sum(case when paid = 2 and refund = 0 then payment_total_money else 0 end)as  decimal(10,2)) as sale_succeed_money, --成交金额
        |cast(sum(case when paid = 2 and refund = 0 and present_day = 1 then payment_total_money else 0 end)as  decimal(10,2)) as new_user_succeed_money, --成交金额
        |cast(sum(case when paid = 2 and refund = 0 and present_day != 1 then payment_total_money else 0 end)as  decimal(10,2)) as aged_user_succeed_money, --成交金额
        |count(distinct  case when paid = 2 then buyer_id end) as present_user_dis_number, --当天成交的用户数
        |count(distinct case when flag_years = 1 then buyer_id end) as aged_user_dis_number, -- 当天成交的老用户数
        |count(distinct case when flag_years = 0 and present_day = 1 then buyer_id end) as new_user_dis_number -- 当天成交的新用户数
        |from
        |order_tmp
        |group by shop_id
        |""".stripMargin).createOrReplaceTempView("client_order_tmp")

    val shopClientAnalysisDF = spark.sql(
      s"""
         |select
         |shop_id,
         |order_type,
         |user_dis_number,
         |present_user_dis_number,
         |new_user_succeed_money,
         |aged_user_succeed_money,
         |aged_user_dis_number,
         |new_user_dis_number,
         |sale_succeed_money,
         |round(present_user_dis_number/user_dis_number,2) as type_user_ratio,
         |round(new_user_dis_number/user_dis_number,2) as new_user_ratio,
         |round(aged_user_dis_number/user_dis_number,2) as aged_user_ratio,
         |case when cast(sale_succeed_money/present_user_dis_number as  decimal(10,2)) is not null
         |then cast(sale_succeed_money/present_user_dis_number as  decimal(10,2))
         |else 0 end as money, --客单价
         |$dt as dt
         |from
         |client_order_tc
         |""".stripMargin).union(
      spark.sql(
        s"""
           |select
           |shop_id,
           |order_type,
           |user_dis_number,
           |present_user_dis_number,
           |new_user_succeed_money,
           |aged_user_succeed_money,
           |aged_user_dis_number,
           |new_user_dis_number,
           |sale_succeed_money,
           |round(present_user_dis_number/user_dis_number,2) as type_user_ratio,
           |round(new_user_dis_number/user_dis_number,2) as new_user_ratio,
           |round(aged_user_dis_number/user_dis_number,2) as aged_user_ratio,
           |case when cast(sale_succeed_money/present_user_dis_number as  decimal(10,2)) is not null
           |then cast(sale_succeed_money/present_user_dis_number as  decimal(10,2))
           |else 0 end as money, --客单价
           |$dt as dt
           |from
           |client_order_tb
           |""".stripMargin)
    ).union(spark.sql(
      s"""
         |select
         |shop_id,
         |'all' as order_type,
         |user_dis_number,
         |present_user_dis_number,
         |new_user_succeed_money,
         |aged_user_succeed_money,
         |aged_user_dis_number,
         |new_user_dis_number,
         |sale_succeed_money,
         |round(present_user_dis_number/user_dis_number,2) as type_user_ratio,
         |round(new_user_dis_number/user_dis_number,2) as new_user_ratio,
         |round(aged_user_dis_number/user_dis_number,2) as aged_user_ratio,
         |case when cast(sale_succeed_money/present_user_dis_number as  decimal(10,2)) is not null
         |then cast(sale_succeed_money/present_user_dis_number as  decimal(10,2))
         |else 0 end as money, --客单价
         |$dt as dt
         |from
         |client_order_tmp
         |""".stripMargin))
    writerMysql(shopClientAnalysisDF, "shop_client_analysis", flag)
    /**
     * 访问-支付转化率 -- 需埋点
     * 全部成交客户-访问-支付转化率：全部支付成功客户数/店铺访客数
     * 新成交客户-访问-支付转化率： 新成交客户数/店铺访客数中近1年无购买记录的访客数
     * 老成交客户-访问-支付转化率：老成交客户数/店铺访客数中近1年购买过的访客数
     */
    /**
     * 统计分平台客户采购额，按购买次后金额排名统计
     */
    val shopClientSaleTopDF = spark.sql(
      s"""
         |with t1 as(
         |select
         |shop_id,
         |order_type,
         |buyer_id,
         |payment_total_money,
         |payment_total_money - (num * cost_price) as profit
         |from
         |purchase_tmp
         |where order_type='TC'
         |),
         |t2 as(
         |select
         |shop_id,
         |order_type,
         |buyer_id,
         |case when round(sum(profit),2)  is null
         |then 0 else round(sum(profit),2) end as sale_succeed_profit,
         |case when round(sum(payment_total_money),2) is null
         |then 0 else round(sum(payment_total_money),2)
         |end as sale_succeed_money
         |from
         |t1
         |group by shop_id,order_type,buyer_id
         |),t3 as (
         |select
         |shop_id,
         |order_type,
         |user_mapping(buyer_id) as user_name,
         |sale_succeed_money,
         |sale_succeed_profit,
         |row_number() over(partition by shop_id,order_type,buyer_id order by sale_succeed_money desc) as profit_top
         |from
         |t2
         |)
         |select
         |shop_id,
         |order_type,
         |user_name,
         |sale_succeed_money,
         |sale_succeed_profit,
         |$dt as dt
         |from
         |t3
         |where profit_top <=10
         |""".stripMargin).union(
      spark.sql(
        s"""
           |with t1 as(
           |select
           |shop_id,
           |order_type,
           |buyer_id,
           |payment_total_money,
           |payment_total_money - (num * cost_price) as profit
           |from
           |purchase_tmp
           |where order_type='TB'
           |),
           |t2 as(
           |select
           |shop_id,
           |order_type,
           |buyer_id,
           |case when round(sum(profit),2)  is null
           |then 0 else round(sum(profit),2) end as sale_succeed_profit,
           |case when round(sum(payment_total_money),2) is null
           |then 0 else round(sum(payment_total_money),2)
           |end as sale_succeed_money
           |from
           |t1
           |group by shop_id,order_type,buyer_id
           |),t3 as (
           |select
           |shop_id,
           |order_type,
           |user_mapping(buyer_id) as user_name,
           |sale_succeed_money,
           |sale_succeed_profit,
           |row_number() over(partition by shop_id,order_type,buyer_id order by sale_succeed_money desc) as profit_top
           |from
           |t2
           |)
           |select
           |shop_id,
           |order_type,
           |user_name,
           |sale_succeed_money,
           |sale_succeed_profit,
           |$dt as dt
           |from
           |t3
           |where profit_top <=10
           |""".stripMargin)
    ).union(
      spark.sql(
        s"""
           |with t1 as(
           |select
           |shop_id,
           |buyer_id,
           |payment_total_money,
           |payment_total_money - (num * cost_price) as profit
           |from
           |purchase_tmp
           |),
           |t2 as(
           |select
           |shop_id,
           |buyer_id,
           |case when round(sum(profit),2)  is null
           |then 0 else round(sum(profit),2) end as sale_succeed_profit,
           |case when round(sum(payment_total_money),2) is null
           |then 0 else round(sum(payment_total_money),2)
           |end as sale_succeed_money
           |from
           |t1
           |group by shop_id,buyer_id
           |),t3 as (
           |select
           |shop_id,
           |user_mapping(buyer_id) as user_name,
           |sale_succeed_money,
           |sale_succeed_profit,
           |row_number() over(partition by shop_id,buyer_id order by sale_succeed_money desc) as profit_top
           |from
           |t2
           |)
           |select
           |shop_id,
           |'all' as order_type,
           |user_name,
           |sale_succeed_money,
           |sale_succeed_profit,
           |$dt as dt
           |from
           |t3
           |where profit_top <=10
           |""".stripMargin)
    )
    writerMysql(shopClientSaleTopDF, "shop_client_sale_top", flag)
    spark.sql(
      s"""
        |select
        |shop_id,
        |count(1) as attention_number,
        |count( case when dt = $dt then 1 end) as new_attention_number
        |from
        |ods.ods_shop_user_attention
        |group by shop_id
        |""".stripMargin).createOrReplaceTempView("user_all_attention")
    // 会员信息
    val membersDataFrame = spark.sql(
      s"""
         |with t1 as (select
         |shop_id,
         |count(1)  as all_user,
         |count(case when regexp_replace(to_date(create_time),"-","") == $dt then user_id end) as new_user_number,
         |sum(case when b.userId is not null then 1 else 0 end) as user_access_number,
         |sum(vip_user_up) as vip_user_up
         |from
         |(
         |select
         |shop_id,
         |user_id,
         |create_time,
         |vip_user_up
         |from
         |user_statistics
         |) a
         |left join
         |(
         |select
         |shopId,
         |userId
         |from
         |dwd_click_log
         |group by shopId,userId
         |) b
         |on a.shop_id = b.shopId and a.user_id = b.userId
         |group by a.shop_id
         |)
         |select
         |case when t1.shop_id is null then b.shop_id else t1.shop_id end as shop_id,
         |t1.all_user,
         |t1.new_user_number,
         |t1.user_access_number,
         |t1.vip_user_up,
         |b.attention_number,
         |b.new_attention_number,
         |$dt as dt
         |from
         |t1
         |full outer join
         |user_all_attention b
         |on t1.shop_id = b.shop_id
         |""".stripMargin)
    writerMysql(membersDataFrame, "shop_client_members_info", flag)
    // 客户信息
    val storeDataFrame = spark.sql(
      s"""
         |select
         |shop_id,
         |count(1)  as all_user,
         |count(case when regexp_replace(to_date(create_time),"-","") == $dt then seller_id end) as new_user_number,
         |0 as user_access_number,
         |$dt as dt
         |from
         |shop_store
         |group by shop_id
         |""".stripMargin)
    writerMysql(storeDataFrame, "shop_client_store_info", flag)


  }
}
