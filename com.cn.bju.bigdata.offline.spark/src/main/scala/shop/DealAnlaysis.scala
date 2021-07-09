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
class DealAnlaysis(spark: SparkSession,var dt: String, timeFlag: String) extends WriteBase {
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
      spark.sql(s"""
           |select
           |*
           |from
           |dwd.dwd_dim_orders_detail
           |where dt=$dt and po_type = 'PO'
           |""".stripMargin).createOrReplaceTempView("purchase_tmp")
      //退货
      spark.sql(s"""
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
      //出库信息
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_outbound_bill
           |where dt=$dt and shop_id is not null
           |""".stripMargin).createOrReplaceTempView("dim_outbound_bill")
      // 获取自提点信息
    }
    else if (timeFlag.equals("week")) {
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
    }else if (timeFlag.equals("month")) {
      val startTime = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).toString("yyyyMM")
      dt = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).dayOfMonth().withMinimumValue().toString("yyyyMMdd")
      log.info("===========> 交易分析模块-月:"+ dt)
      //零售
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_orders_detail
           |where dt like '$startTime%'  and po_type is null
           |""".stripMargin).createOrReplaceTempView("orders_retail")
      //采购
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_orders_detail
           |where dt like '$startTime%'  and po_type = 'PO'
           |""".stripMargin).createOrReplaceTempView("purchase_tmp")
      //退货
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_refund_detail
           |where dt like '$startTime%'  and po_type is null
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
           |where dt like '$startTime%'
           |group by refund_id
           |""".stripMargin).createOrReplaceTempView("refund_process")
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_outbound_bill
           |where dt like '$startTime%' and shop_id is not null
           |""".stripMargin).createOrReplaceTempView("dim_outbound_bill")
    }

    flag = timeFlag
  }

  override def process(): Unit = {
    spark.sqlContext.cacheTable("orders_retail")
    // TB 为渠道数据 先从订单表中取出来TB数据，在过滤出库单中的数据
    spark.sql(
      s"""
         |select
         |order_id,paid,refund,buyer_id,cost_price,item_id
         |from
         |orders_retail
         |where order_type='TB'
         |""".stripMargin).createOrReplaceTempView("order_tb")
    //店铺下区分平台金额
    val tbDataFrame = spark.sql(
      s"""
        |select
        |a.shop_id,
        |'TB' as order_type,
        |cast (sum(case when paid = 2 and refund = 0 then order_num * price end) as  decimal(10,2)) as sale_succeed_money, --订单支付成功金额
        |count(case when paid = 2 and refund = 0 then 1 end) as orders_succeed_number, --即成交单量
        |cast(sum(cost_price * order_num) as  decimal(10,2) ) as income_money, --收入金额
        |cast(sum(order_num * price) as  decimal(10,2)) as sale_money, --下单金额
        |count(1) as sale_order_number, --下单笔数
        |sum(case when paid = 2 and refund = 0 then order_num end) as paid_num,  --支付件数
        |cast(count(distinct case when paid = 2 and refund = 0 then buyer_id end) as  decimal(10,2))  as sale_user_number, --支付人数
        |count(distinct buyer_id)  as user_number, --下单人数
        |$dt as dt
        |from
        |(
        |select
        |*
        |from
        |dim_outbound_bill
        |where type != 9 and type != 10
        |) a
        |inner join
        |order_tb b
        |on a.order_id = b.order_id and a.item_id = b.item_id
        |group by shop_id
        |""".stripMargin)
    tbDataFrame.createOrReplaceTempView("succeed_tb")
    val tcDataFrame = spark.sql(
      s"""
         |select
         |shop_id,
         |'TC' as order_type,
         |cast(sum(case when paid = 2 and refund = 0 then payment_total_money end) as  decimal(10,2)) as sale_succeed_money,
         |count(case when paid = 2 and refund = 0 then 1 end) as orders_succeed_number, --即成交单量
         |cast(sum((item_original_price - cost_price) * num) as  decimal(10,2)) as income_money, --收入金额
         |cast(sum(payment_total_money) as  decimal(10,2)) as sale_money, --下单金额
         |count(1) as sale_order_number, --下单笔数
         |sum(case when paid = 2 and refund = 0 then num end) as paid_num,
         |cast(count(distinct case when paid = 2 and refund = 0 then buyer_id end) as  decimal(10,2))  as sale_user_number, --支付人数
         |count(distinct buyer_id)  as user_number, --下单人数
         |$dt as dt
         |from
         |orders_retail
         |where order_type='TC'
         |group by shop_id
         |""".stripMargin)
    tcDataFrame.createOrReplaceTempView("succeed_tc")
    //店铺下全平台金额
    val allDataFrame = spark.sql(
      s"""
         |select
         |a.shop_id,
         |'all' as source_type,
         |if(b.sale_succeed_money is null,0,b.sale_succeed_money) + if(c.sale_succeed_money is null,0,c.sale_succeed_money) as sale_succeed_money,
         |if(b.orders_succeed_number is null,0,b.orders_succeed_number) + if(c.orders_succeed_number is null,0,c.orders_succeed_number) as orders_succeed_number,
         |if(b.income_money is null,0,b.income_money) + if(c.income_money is null,0,c.income_money) as income_money,
         |if(b.sale_money is null,0,b.sale_money) + if(c.sale_money is null,0,c.sale_money) as sale_money,
         |if(b.sale_order_number is null,0,b.sale_order_number) + if(c.sale_order_number is null,0,c.sale_order_number) as sale_order_number,
         |if(b.paid_num is null,0,b.paid_num) + if(c.paid_num is null,0,c.paid_num) as paid_num,
         |a.sale_user_number, --支付人数
         |a.user_number, --下单人数
         |$dt as dt
         |from
         |(
         |select
         |shop_id,
         |count(distinct case when paid = 2 and refund = 0 then buyer_id end)  as sale_user_number, --支付人数
         |count(distinct buyer_id)  as user_number --下单人数
         |from
         |orders_retail
         |group by shop_id
         |) a
         |left join
         |succeed_tc b
         |on
         |a.shop_id = b.shop_id
         |left join
         |succeed_tb c
         |on
         |a.shop_id = c.shop_id
         |""".stripMargin)
    val shopSaleSucceedInfoDF = tbDataFrame.union(tcDataFrame).union(allDataFrame)
    writerMysql(shopSaleSucceedInfoDF, "shop_deal_info", flag)
    // ---------------------------------退款
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
    writerMysql(shopRefundReasonDF, "shop_deal_refund_reason", flag)
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
    val shopRefundSku = spark.sql(
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
    writerMysql(shopRefundSku, "shop_deal_refund_sku", flag)
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
      s"""
         |select
         |a.shop_id,
         |'TB' as order_type,
         |floor(avg(case when a.refund_status = 6 then
         |(unix_timestamp(a.max_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(a.min_time,'yyyy-MM-dd HH:mm:ss'))/60
         |else 0 end)) as avg_time, --平均处理时间
         |sum(case when refund_status = 6 then cast(refund_num * refund_price as decimal(10,2)) else 0 end) as refund_money, --成功退款金额
         |count(case when refund_status = 6 then 1 end) as refund_number --成功退款笔数
         |from
         |(
         |select
         |*
         |from
         |refund_orders
         |where order_type = 'TB'
         |) a
         |left join
         |(
         |select
         |*
         |from
         |dim_outbound_bill
         |where type = 9 or type = 10
         |) b
         |on a.refund_no = b.order_id
         |where b.id is not null
         |group by a.shop_id
         |""".stripMargin).createOrReplaceTempView("refund_tb")
    //获取商铺不同平台下成交订单量和成交钱数，订单中间表获取
    spark.sql("""
        |select
        |shop_id,
        |order_type,
        |count(case when paid = 2  then 1 end) as orders_succeed_number --支付单数
        |from
        |orders_retail
        |where  order_type = 'TC'
        |group by shop_id,order_type
        |""".stripMargin).createOrReplaceTempView("order_paid_tc_tmp")
    spark.sql("""
        |select
        |a.shop_id,
        |'TB' as order_type,
        |count(case when paid = 2  then 1 end) as orders_succeed_number --支付单数
        |from
        |(
        |select
        |* from
        |orders_retail
        |where  order_type = 'TB'
        |) a
        |left join
        |(
        |select
        |*
        |from
        |dim_outbound_bill
        |where type != 9 and type != 10
        |) b
        |on a.order_id = b.order_id and a.item_id = b.item_id
        |where b.order_id is not null
        |group by a.shop_id
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
    writerMysql(shopRefundInfo, "shop_deal_refund_info", flag)
    // 自提点信息
    spark.sql(
      s"""
        |select
        |*
        |from
        |dwd.dwd_dim_orders_self_pick
        |where dt = $dt
        |""".stripMargin).createOrReplaceTempView("orders_self_pick")
    spark.sql(
      s"""
         |select
         |shop_id,
         |'TC' as order_type,
         |count(distinct pick_id) as pick_number,
         |count(1) as pick_order_number,
         |cast(sum(payment_total_money) as  decimal(10,2)) as pick_order_money,
         |cast(sum((item_original_price - cost_price) * num) as  decimal(10,2)) as pick_income_money,
         |$dt as dt
         |from
         |orders_self_pick
         |where order_type = 'TC'
         |group by shop_id
         |""".stripMargin).createOrReplaceTempView("pickTCdataFrame")
    spark.sql(
      s"""
         |select
         |shop_id,
         |'TB' as order_type,
         |count(distinct pick_id) as pick_number,
         |count(1) as pick_order_number,
         |cast(sum(payment_total_money) as  decimal(10,2)) as pick_order_money,
         |cast(sum(cost_price * order_num) as  decimal(10,2)) as pick_income_money,
         |$dt as dt
         |from
         |orders_self_pick
         |where order_type = 'TB'
         |group by shop_id
         |""".stripMargin).createOrReplaceTempView("pickTBdataFrame")
    val pickDataFrame = spark.sql(
      s"""
         |select  --- A全部数据，B在A表中的数据  合并B表独有数据
         |a.shop_id,
         |a.pick_number + b.pick_number as pick_number,
         |a.pick_order_number+b.pick_order_number as pick_order_number,
         |a.pick_order_money + b.pick_order_money as pick_order_money,
         |a.pick_income_money + b.pick_income_money as pick_income_money,
         |$dt as dt
         |from
         |pickTCdataFrame a
         |left join
         |pickTBdataFrame b
         |on a.shop_id = b.shop_id
         |union all
         |select
         |a.shop_id,
         |a.pick_number,
         |a.pick_order_number,
         |a.pick_order_money,
         |a.pick_income_money,
         |$dt as dt
         |from
         |pickTBdataFrame a
         |left join
         |pickTCdataFrame b
         |on a.shop_id = b.shop_id
         |where b.shop_id is null
         |""".stripMargin)
    writerMysql(pickDataFrame, "shop_deal_self_pick_info", flag)
  }
}



//    spark.sql(
//      """
//        |select
//        |shop_id,
//        |order_type,
//        |floor(avg(case when refund_status = 6 then
//        |(unix_timestamp(max_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(min_time,'yyyy-MM-dd HH:mm:ss'))/60
//        |else 0 end)) as avg_time, --平均处理时间
//        |sum(case when refund_status = 6 then cast(refund_num * refund_price as decimal(10,2)) else 0 end) as refund_money, --成功退款金额
//        |count(case when refund_status = 6 then 1 end) as refund_number --成功退款笔数
//        |from
//        |refund_orders
//        |where   order_type = 'TB'
//        |group by shop_id,order_type
//        |""".stripMargin).createOrReplaceTempView("refund_tb")
// 关联出库表，过滤出出库表中退货的 信息