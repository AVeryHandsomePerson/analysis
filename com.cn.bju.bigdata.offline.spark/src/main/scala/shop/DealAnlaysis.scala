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
class DealAnlaysis(spark: SparkSession, dt: String, timeFlag: String) extends WriteBase {
  val log = Logger.getLogger(App.getClass)
  var flag = "";
  {

    val startTime = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).minusWeeks(1).toString("yyyyMMdd")
    log.info("===========> 交易分析模块-开始注册UDF函数:")
    UDFRegister.shopMapping(spark, dt)
    UDFRegister.skuMapping(spark, dt)
    if (timeFlag.equals("day")) {
      log.info("===========> 交易分析模块-天:" + dt)
      //零售
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_orders_detail
           |where dt=$dt and po_type is null
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
      //退货
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_refund_detail
           |where dt=$dt and po_type is null
           |""".stripMargin).createOrReplaceTempView("refund_orders_tmp")
      // 取出当天退款申请时 第一次请求时间和最后一次响应时间
      spark.sql(
        s"""
           |
           |select
           |refund_id,
           |max(create_time) as max_time,
           |min(create_time) as min_time
           |from
           |ods.ods_refund_process
           |where dt= $dt
           |group by refund_id
           |""".stripMargin).createOrReplaceTempView("refund_process")
    } else if (timeFlag.equals("week")) {
      log.info("===========> 交易分析模块-周:" + startTime + "and" + dt)
      //零售
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_orders_detail
           |where dt>= $startTime and dt<=$dt and po_type is null
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
      //退货
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_refund_detail
           |where dt>= $startTime and dt<=$dt and po_type is null
           |""".stripMargin).createOrReplaceTempView("refund_orders_tmp")
      spark.sql(
        s"""
           |
           |select
           |refund_id,
           |max(create_time) as max_time,
           |min(create_time) as min_time
           |from
           |ods.ods_refund_process
           |where dt>= $startTime and dt<=$dt
           |group by refund_id
           |""".stripMargin).createOrReplaceTempView("refund_process")
    }
    flag = timeFlag
  }

  override def process(): Unit = {
    //解析hdfs_page -- 需埋点
    /**
     * 支付订单数，即成交单量：--合并successful_transaction
     * 统计时间内(按天、周、月统计)用户付款的总订单量，
     * 包括先款订单量(在线支付、公司转账、邮局汇款等)和货到付款订单量。
     */
    /**
     * 访客数：-- 需要埋点
     * 店铺各页面的访问人数。
     * 00:00-24:00内，同一访客多次访问只被计算一次。
     */
    /**
     * 浏览量：-- 需埋点
     * 店铺各页面被用户访问的次数。
     * 用户多次打开或刷新同一个页面，浏览量累加。
     */
    spark.sqlContext.cacheTable("orders_retail")
    //店铺下区分平台金额
    spark.sql(
      s"""
         |select
         |shop_id,
         |order_type,
         |round(sum(case when paid = 2 and refund = 0 then payment_total_money end),3) as sale_succeed_money,
         |count(case when paid = 2 and refund = 0 then 1 end) as orders_succeed_number, --即成交单量
         |count(distinct case when paid = 2 and refund = 0 then buyer_id end)  as sale_user_number, --支付人数
         |count(distinct buyer_id)  as user_number, --下单人数
         |round(sum(payment_total_money),3) as sale_money, --下单金额
         |count(1) as sale_order_number, --下单笔数
         |sum(case when paid = 2 and refund = 0 then num end) as paid_num
         |from
         |orders_retail
         |where order_type='TB'
         |group by shop_id,order_type
         |""".stripMargin).createOrReplaceTempView("succeed_tb")
    spark.sql(
      s"""
         |select
         |shop_id,
         |order_type,
         |round(sum(case when paid = 2 and refund = 0 then payment_total_money end),3) as sale_succeed_money,
         |count(case when paid = 2 and refund = 0 then 1 end) as orders_succeed_number, --即成交单量
         |count(distinct case when paid = 2 and refund = 0 then buyer_id end)  as sale_user_number, --支付人数
         |count(distinct buyer_id)  as user_number, --下单人数
         |round(sum(payment_total_money),3) as sale_money, --下单金额
         |count(1) as sale_order_number, --下单笔数
         |sum(case when paid = 2 and refund = 0 then num end) as paid_num
         |from
         |orders_retail
         |where order_type='TC'
         |group by shop_id,order_type
         |""".stripMargin).createOrReplaceTempView("succeed_tc")
    //店铺下全平台金额
    spark.sql(
      s"""
         |select
         |shop_id,
         |round(sum(case when paid = 2 and refund = 0 then payment_total_money end),3) as sale_succeed_money,
         |count(case when paid = 2 and refund = 0 then 1 end) as orders_succeed_number, --即成交单量
         |count(distinct case when paid = 2 and refund = 0 then buyer_id end)  as sale_user_number, --支付人数
         |count(distinct buyer_id)  as user_number, --下单人数
         |round(sum(payment_total_money),3) as sale_money, --下单金额
         |count(1) as sale_order_number, --下单笔数
         |sum(case when paid = 2 and refund = 0 then num end) as paid_num
         |from
         |orders_retail
         |group by shop_id
         |""".stripMargin).createOrReplaceTempView("shop_all_money")
      val shopSaleSucceedInfoDF = spark.sql(
      s"""
         |select
         |shop_id,
         |order_type,
         |user_number, --下单人数
         |sale_money, --下单金额
         |sale_user_number, --成交客户数
         |orders_succeed_number, --成交单量
         |sale_order_number, --下单笔数
         |case when sale_succeed_money is null then 0 else sale_succeed_money
         |end as sale_succeed_money, --成交金额
         |case when cast(sale_succeed_money/sale_user_number as  decimal(10,2)) is not null
         |then cast(sale_succeed_money/sale_user_number as  decimal(10,2))
         |else 0 end as money, --客单价
         |paid_num, --支付件数
         |$dt as dt
         |from
         |succeed_tb
         |""".stripMargin).union(spark.sql(
      s"""
         |select
         |shop_id,
         |order_type,
         |user_number, --下单人数
         |sale_money, --下单金额
         |sale_user_number, --成交客户数
         |orders_succeed_number, --成交单量
         |sale_order_number, --下单笔数
         |case when sale_succeed_money is null then 0 else sale_succeed_money
         |end as sale_succeed_money, --成交金额
         |case when cast(sale_succeed_money/sale_user_number as  decimal(10,2)) is not null
         |then cast(sale_succeed_money/sale_user_number as  decimal(10,2))
         |else 0 end as money, --客单价
         |paid_num, --支付件数
         |$dt as dt
         |from
         |succeed_tc
         |""".stripMargin)).union(spark.sql(
      s"""
         |select
         |shop_id,
         |'all' as source_type,
         |user_number, --下单人数
         |sale_money, --下单金额
         |sale_user_number, --成交客户数
         |orders_succeed_number, --成交单量
         |sale_order_number, --下单笔数
         |case when sale_succeed_money is null then 0 else sale_succeed_money
         |end as sale_succeed_money, --成交金额
         |case when cast(sale_succeed_money/sale_user_number as  decimal(10,2)) is not null
         |then cast(sale_succeed_money/sale_user_number as  decimal(10,2))
         |else 0 end as money, --客单价
         |paid_num, --支付件数
         |$dt as dt
         |from
         |shop_all_money
         |""".stripMargin))

    writerMysql(shopSaleSucceedInfoDF, "shop_deal_info", flag)

    spark.sql(
      """
        |select
        |a.*,
        |b.max_time,
        |b.min_time
        |from
        |refund_orders_tmp a
        |left join
        |refund_process b
        |on
        |a.id = b.refund_id
        |""".stripMargin).createOrReplaceTempView("refund_orders")
    spark.sqlContext.cacheTable("refund_orders")
    //全平台店铺下退款原因排行
    spark.sql(
      """
        |select
        |shop_id,
        |count(1) as all_number,
        |cast (sum(refund_num * refund_price) as  decimal(10,2))as all_money
        |from
        |refund_orders
        |group by shop_id
        |""".stripMargin).createOrReplaceTempView("shop_refund_tmp")
    spark.sql(
      s"""
         |select
         |shop_id,
         |refund_reason,
         |count(1) as refund_reason_number,
         |cast(sum(case when refund_status = 6 then refund_num * refund_price else 0 end) as decimal(10,2)) as refund_money, --成功退款金额
         |count(case when refund_status = 6 then 1 end) as refund_number --成功退款数量
         |from
         |refund_orders
         |group by shop_id,refund_reason
         |""".stripMargin).createOrReplaceTempView("shop_refund_reason")
    //分平台
    spark.sql(
      """
        |select
        |shop_id,
        |order_type,
        |count(1) as all_number,
        |cast (sum( refund_num * refund_price ) as  decimal(10,2))as all_money
        |from
        |refund_orders
        |where order_type = 'TB'
        |group by shop_id,order_type
        |""".stripMargin).createOrReplaceTempView("shop_refund_tb")
    spark.sql(
      s"""
         |select
         |shop_id,
         |refund_reason,
         |order_type,
         |count(1) as refund_reason_number,
         |cast(sum(case when refund_status = 6 then  refund_num * refund_price else 0 end) as decimal(10,2)) as refund_money, --成功退款金额
         |count(case when refund_status = 6 then 1 end) as refund_number --成功退款笔数
         |from
         |refund_orders
         |where  order_type = 'TB'
         |group by shop_id,order_type,refund_reason
         |""".stripMargin).createOrReplaceTempView("shop_refund_reason_tb")
    spark.sql(
      """
        |select
        |shop_id,
        |order_type,
        |count(1) as all_number,
        |cast (sum( refund_num * refund_price ) as  decimal(10,2))as all_money
        |from
        |refund_orders
        |where order_type = 'TC'
        |group by shop_id,order_type
        |""".stripMargin).createOrReplaceTempView("shop_refund_tc")
    spark.sql(
      s"""
         |select
         |shop_id,
         |refund_reason,
         |order_type,
         |count(1) as refund_reason_number,
         |cast(sum(case when refund_status = 6 then  refund_num * refund_price else 0 end) as decimal(10,2)) as refund_money, --成功退款金额
         |count(case when refund_status = 6 then 1 end) as refund_number --成功退款笔数
         |from
         |refund_orders
         |where order_type = 'TC'
         |group by shop_id,order_type,refund_reason
         |""".stripMargin).createOrReplaceTempView("shop_refund_reason_tc")
    val shopRefundReasonDF = spark.sql(
      s"""
         |select
         |t1.shop_id,
         |shop_mapping(t1.shop_id) as shop_name,
         |refund_reason,
         |'all' as order_type,
         |t1.refund_reason_number,
         |t1.refund_money,
         |t1.refund_number,
         |cast(t1.refund_number/t2.all_number as decimal(10,2)) as refund_number_ratio,
         |cast(t1.refund_money/t2.all_money as decimal(10,2)) as refund_money_ratio,
         |$dt as dt
         |from
         |shop_refund_reason t1
         |left join
         |shop_refund_tmp t2
         |on t1.shop_id = t2.shop_id
         |""".stripMargin).union(
      spark.sql(
        s"""
           |select
           |t1.shop_id,
           |shop_mapping(t1.shop_id) as shop_name,
           |refund_reason,
           |t1.order_type,
           |t1.refund_reason_number, --总退款笔数
           |t1.refund_money,--成功退款金额
           |t1.refund_number,--成功退款笔数
           |cast(t1.refund_number/t2.all_number as decimal(10,2)) as refund_number_ratio,
           |cast(t1.refund_money/t2.all_money as decimal(10,2)) as refund_money_ratio,
           |$dt as dt
           |from
           |shop_refund_reason_tb t1
           |left join
           |shop_refund_tb t2
           |on t1.shop_id = t2.shop_id and t1.order_type = t2.order_type
           |""".stripMargin)
    ).union(spark.sql(
      s"""
         |select
         |t1.shop_id,
         |shop_mapping(t1.shop_id) as shop_name,
         |refund_reason,
         |t1.order_type,
         |t1.refund_reason_number, --总退款笔数
         |t1.refund_money,--成功退款金额
         |t1.refund_number,--成功退款笔数
         |cast(t1.refund_number/t2.all_number as decimal(10,2)) as refund_number_ratio,
         |cast(t1.refund_money/t2.all_money as decimal(10,2)) as refund_money_ratio,
         |$dt as dt
         |from
         |shop_refund_reason_tc t1
         |left join
         |shop_refund_tc t2
         |on t1.shop_id = t2.shop_id and t1.order_type = t2.order_type
         |""".stripMargin))
//    writerMysql(shopRefundReasonDF, "shop_deal_refund_reason", flag)
    //--------------全平台店铺下退款商品排行
    spark.sql(
      s"""
         |select
         |shop_id,
         |sku_id,
         |count(1) as refund_reason_number, -- 店铺下每个商品的总退款单数
         |sum(case when refund_status = 6 then cast(refund_num * refund_price as decimal(10,2)) else 0 end) as refund_money, --成功退款金额
         |count(distinct refund_reason)as refund_sku_reason_number, -- 店铺下每个商品的总退款单数
         |count(case when refund_status = 6 then 1 end) as refund_number --店铺下每个商品的成功退款数量
         |from
         |refund_orders
         |group by shop_id,sku_id
         |""".stripMargin).createOrReplaceTempView("refund_sku_info")
    //获取商品的成交订单量和成交钱数，订单中间表获取
    spark.sql(
      """
        |select
        |shop_id,
        |sku_id,
        |count(case when paid = 2  then 1 end) as orders_succeed_number,
        |sum(case when paid = 2 then payment_total_money else 0 end) as orders_succeed_money
        |from
        |orders_retail
        |group by shop_id,sku_id
        |""".stripMargin).createOrReplaceTempView("order_sku_paid_tmp")
    //--------------划分平台店铺下退款商品排行
    spark.sql(
      s"""
         |select
         |shop_id,
         |order_type,
         |sku_id,
         |count(1) as refund_reason_number, -- 店铺下每个商品的总退款单数
         |sum(case when refund_status = 6 then cast(refund_num * refund_price as decimal(10,2)) else 0 end) as refund_money, --成功退款金额
         |count(distinct refund_reason)as refund_sku_reason_number, -- 店铺下每个商品的总退款单数
         |count(case when refund_status = 6 then 1 end) as refund_number --店铺下每个商品的成功退款数量
         |from
         |refund_orders
         |where order_type = 'TB'
         |group by shop_id,order_type,sku_id
         |""".stripMargin).createOrReplaceTempView("refund_sku_tb_info")
    //获取商品不同平台下成交订单量和成交钱数，订单中间表获取
    spark.sql(
      """
        |select
        |shop_id,
        |order_type,
        |sku_id,
        |count(case when paid = 2  then 1 end) as orders_succeed_number,
        |sum(case when paid = 2 then payment_total_money else 0 end) as orders_succeed_money
        |from
        |orders_retail
        |where  order_type = 'TB'
        |group by shop_id,order_type,sku_id
        |""".stripMargin).createOrReplaceTempView("order_sku_paid_tb_tmp")
    //--------------划分平台店铺下退款商品排行
    spark.sql(
      s"""
         |select
         |shop_id,
         |order_type,
         |sku_id,
         |count(1) as refund_reason_number, -- 店铺下每个商品的总退款单数
         |sum(case when refund_status = 6 then cast(refund_num * refund_price as decimal(10,2)) else 0 end) as refund_money, --成功退款金额
         |count(distinct refund_reason)as refund_sku_reason_number, -- 店铺下每个商品的总退款单数
         |count(case when refund_status = 6 then 1 end) as refund_number --店铺下每个商品的成功退款数量
         |from
         |refund_orders
         |where  order_type = 'TC'
         |group by shop_id,order_type,sku_id
         |""".stripMargin).createOrReplaceTempView("refund_sku_tc_info")
    //获取商品不同平台下成交订单量和成交钱数，订单中间表获取
    spark.sql(
      """
        |select
        |shop_id,
        |order_type,
        |sku_id,
        |count(case when paid = 2  then 1 end) as orders_succeed_number,
        |sum(case when paid = 2 then payment_total_money else 0 end) as orders_succeed_money
        |from
        |orders_retail
        |where   order_type = 'TC'
        |group by shop_id,order_type,sku_id
        |""".stripMargin).createOrReplaceTempView("order_sku_paid_tc_tmp")
    val shopRefundSku=  spark.sql(
      s"""
         |select
         |t1.shop_id,
         |shop_mapping(t1.shop_id) as shop_name,
         |t1.sku_id,
         |'all' as order_type,
         |sku_mapping(t1.sku_id) sku_name,
         |t1.refund_reason_number, --退款数量 后续排序使用
         |cast(t1.refund_money as decimal(10,2)) as refund_money,
         |cast(t2.orders_succeed_money as decimal(10,2)) orders_succeed_money, --订单金额
         |t1.refund_number, --店铺下每个商品的成功退款数量
         |cast(t1.refund_number/t2.orders_succeed_number as decimal(10,2)) as refund_ratio, --退款率
         |cast(refund_sku_reason_number/refund_reason_number as decimal(10,2)) as refund_reason_ratio, --退款原因比
         |$dt as dt
         |from
         |refund_sku_info t1
         |left join
         |order_sku_paid_tmp t2
         |on t1.shop_id = t2.shop_id and t1.sku_id = t2.sku_id
         |""".stripMargin).union(
      spark.sql(
        s"""
           |select
           |t1.shop_id,
           |shop_mapping(t1.shop_id) as shop_name,
           |t1.sku_id,
           |t1.order_type,
           |sku_mapping(t1.sku_id) sku_name,
           |t1.refund_reason_number, --退款数量 后续排序使用
           |cast(t1.refund_money as decimal(10,2)) as refund_money, --成功退款金额
           |cast(t2.orders_succeed_money as decimal(10,2)) orders_succeed_money, --订单金额
           |t1.refund_number, --店铺下每个商品的成功退款数量
           |cast(t1.refund_number/t2.orders_succeed_number as decimal(10,2)) as refund_ratio, --退款率
           |cast(refund_sku_reason_number/refund_reason_number as decimal(10,2)) as refund_reason_ratio, --退款原因比
           |$dt as dt
           |from
           |refund_sku_tb_info t1
           |left join
           |order_sku_paid_tb_tmp t2
           |on t1.shop_id = t2.shop_id and t1.sku_id = t2.sku_id and t1.order_type = t2.order_type
           |""".stripMargin)).union(
      spark.sql(
        s"""
           |select
           |t1.shop_id,
           |shop_mapping(t1.shop_id) as shop_name,
           |t1.sku_id,
           |t1.order_type,
           |sku_mapping(t1.sku_id) sku_name,
           |t1.refund_reason_number, --退款数量 后续排序使用
           |cast(t1.refund_money as decimal(10,2)) as refund_money, --成功退款金额
           |cast(t2.orders_succeed_money as decimal(10,2)) orders_succeed_money, --订单金额
           |t1.refund_number, --店铺下每个商品的成功退款数量
           |cast(t1.refund_number/t2.orders_succeed_number as decimal(10,2)) as refund_ratio, --退款率
           |cast(refund_sku_reason_number/refund_reason_number as decimal(10,2)) as refund_reason_ratio, --退款原因比
           |$dt as dt
           |from
           |refund_sku_tc_info t1
           |left join
           |order_sku_paid_tc_tmp t2
           |on t1.shop_id = t2.shop_id and t1.sku_id = t2.sku_id and t1.order_type = t2.order_type
           |""".stripMargin)
    )
//    writerMysql(shopRefundSku, "shop_deal_refund_sku", flag)
    /**
     * 成功退款金额
     * and 成功退款笔数
     * and 退款平均处理时间----运营响应时长
     * and 退款率
     * 退款数量/成交数量
     */
    //分平台
    spark.sql(
      """
        |select
        |shop_id,
        |order_type,
        |floor(avg(case when refund_status = 6 then
        |(unix_timestamp(max_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(min_time,'yyyy-MM-dd HH:mm:ss'))/60
        |else 0 end)) as avg_time, --平均处理时间
        |sum(case when refund_status = 6 then cast(refund_num * refund_price as decimal(10,2)) else 0 end) as refund_money, --成功退款金额
        |count(case when refund_status = 6 then 1 end) as refund_number --成功退款笔数
        |from
        |refund_orders
        |where   order_type = 'TC'
        |group by shop_id,order_type
        |""".stripMargin).createOrReplaceTempView("refund_tc")
    spark.sql(
      """
        |select
        |shop_id,
        |order_type,
        |floor(avg(case when refund_status = 6 then
        |(unix_timestamp(max_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(min_time,'yyyy-MM-dd HH:mm:ss'))/60
        |else 0 end)) as avg_time, --平均处理时间
        |sum(case when refund_status = 6 then cast(refund_num * refund_price as decimal(10,2)) else 0 end) as refund_money, --成功退款金额
        |count(case when refund_status = 6 then 1 end) as refund_number --成功退款笔数
        |from
        |refund_orders
        |where   order_type = 'TB'
        |group by shop_id,order_type
        |""".stripMargin).createOrReplaceTempView("refund_tb")
    //获取商铺不同平台下成交订单量和成交钱数，订单中间表获取
    spark.sql(
      """
        |select
        |shop_id,
        |order_type,
        |count(case when paid = 2  then 1 end) as orders_succeed_number --支付单数
        |from
        |orders_retail
        |where  order_type = 'TC'
        |group by shop_id,order_type
        |""".stripMargin).createOrReplaceTempView("order_paid_tc_tmp")
    spark.sql(
      """
        |select
        |shop_id,
        |order_type,
        |count(case when paid = 2  then 1 end) as orders_succeed_number --支付单数
        |from
        |orders_retail
        |where  order_type = 'TB'
        |group by shop_id,order_type
        |""".stripMargin).createOrReplaceTempView("order_paid_tb_tmp")
    //全平台
    spark.sql(
      """
        |select
        |shop_id,
        |'all' as order_type,
        |floor(avg(case when refund_status = 6 then
        |(unix_timestamp(max_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(min_time,'yyyy-MM-dd HH:mm:ss'))/60
        |else 0 end)) as avg_time, --平均处理时间
        |sum(case when refund_status = 6 then cast(refund_num * refund_price as decimal(10,2)) end) as refund_money, --成功退款金额
        |count(case when refund_status = 6 then 1 end) as refund_number --成功退款笔数
        |from
        |refund_orders
        |where
        |group by shop_id
        |""".stripMargin).createOrReplaceTempView("refund")
    //获取商铺的成交订单量和成交钱数，订单中间表获取
    spark.sql(
      """
        |select
        |shop_id,
        |count(case when paid = 2  then 1 end) as orders_succeed_number --支付单数
        |from
        |orders_retail
        |group by shop_id
        |""".stripMargin).createOrReplaceTempView("order_paid_tmp")
    val shopRefundInfo = spark.sql(
      s"""
         |select
         |t1.shop_id,
         |shop_mapping(t1.shop_id) as shop_name,
         |t1.order_type,
         |cast(t1.avg_time as decimal(10,2)) as avg_time, --平均处理时间
         |cast(t1.refund_money as decimal(10,2)) as refund_money, --成功退款金额
         |t1.refund_number, --成功退款笔数
         |t2.orders_succeed_number, --即成交单量
         |case when t2.orders_succeed_number is not null
         |then
         |cast(refund_number/t2.orders_succeed_number as decimal(10,2))
         |else 0
         |end as refund_ratio,--退款率
         |$dt as dt
         |from
         |refund_tc t1
         |left join
         |order_paid_tc_tmp t2
         |on t1.shop_id = t2.shop_id and t1.order_type = t2.order_type
         |""".stripMargin).union(spark.sql(
      s"""
         |select
         |t1.shop_id,
         |shop_mapping(t1.shop_id) as shop_name,
         |t1.order_type,
         |cast(t1.avg_time as decimal(10,2)) as avg_time, --平均处理时间
         |cast(t1.refund_money as decimal(10,2)) as refund_money, --成功退款金额
         |t1.refund_number, --成功退款笔数
         |t2.orders_succeed_number, --即成交单量
         |case when t2.orders_succeed_number is not null
         |then
         |cast(refund_number/t2.orders_succeed_number as decimal(10,2))
         |else 0
         |end as refund_ratio, --退款率
         |$dt as dt
         |from
         |refund t1
         |left join
         |order_paid_tmp t2
         |on t1.shop_id = t2.shop_id
         |""".stripMargin)).union(
      spark.sql(
        s"""
           |select
           |t1.shop_id,
           |shop_mapping(t1.shop_id) as shop_name,
           |t1.order_type,
           |cast(t1.avg_time as decimal(10,2)) as avg_time, --平均处理时间
           |cast(t1.refund_money as decimal(10,2)) as refund_money, --成功退款金额
           |t1.refund_number, --成功退款笔数
           |t2.orders_succeed_number, --即成交单量
           |case when t2.orders_succeed_number is not null
           |then
           |cast(refund_number/t2.orders_succeed_number as decimal(10,2))
           |else 0
           |end as refund_ratio,--退款率
           |$dt as dt
           |from
           |refund_tb t1
           |left join
           |order_paid_tb_tmp t2
           |on t1.shop_id = t2.shop_id and t1.order_type = t2.order_type
           |""".stripMargin)
    )
//    writerMysql(shopRefundInfo, "shop_deal_refund_info", flag)
  }
}