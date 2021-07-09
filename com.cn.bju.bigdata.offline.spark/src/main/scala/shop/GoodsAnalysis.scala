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
class GoodsAnalysis(spark: SparkSession, var dt: String, timeFlag: String) extends WriteBase {

  val log = Logger.getLogger(App.getClass)
  var flag = "";
  {

    val startTime = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).minusWeeks(1).toString("yyyyMMdd")
    log.info("===========> 商品分析模块-开始注册UDF函数:")
    UDFRegister.shopMapping(spark, dt)
    UDFRegister.skuMapping(spark, dt)
    UDFRegister.clientMapping(spark, dt)
    UDFRegister.cidThreeMapping(spark, dt)
    if (timeFlag.equals("day")) {
      log.info("===========> 商品分析模块-天:" + dt)
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
    } else if (timeFlag.equals("week")) {
      log.info("===========> 商品分析模块-周:" + startTime + "and" + dt)
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
    }else if (timeFlag.equals("month")) {
      val startTime = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).toString("yyyyMM")
      dt = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).dayOfMonth().withMinimumValue().toString("yyyyMMdd")
      log.info("===========> 商品分析模块-月:"+ dt)
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
    }

    flag = timeFlag
  }

  def process(): Unit = {
    //解析hdfs_page
    /**
     * 被访问商品品种量
     * 1.从url中解析出：sku_id,和item_id
     * 2. 统计item_id下每个sku_id个数
     */
    spark.sqlContext.cacheTable("orders_retail")

    /**
     * 动销商品数 = 有成交的商品的品种数
     * 成交订单为当日付款当日未取消的在线支付订单，
     * 或者当日下单当日未取消的货到付款订单。
     * 下单客户数
     * 下单笔数
     * 下单商品件数
     * 成交商品件数
     * 成交金额
     * 商品成交转化率 --需要埋点
     * 统计在架商品数据
     */
    spark.sql(
      s"""
         |select
         |shop_id,
         |shop_mapping(shop_id) as shop_name,
         |count(distinct case when paid = 2 then sku_id end) as sale_number,
         |count(distinct buyer_id) as orders_user_number,
         |count(distinct order_id) as orders_number,
         |cast(sum(case when num is null then 0 else num end )as  decimal(10,2)) as sale_goods_number,
         |cast(sum(case when paid = 2 and refund = 0 then num else 0 end)as  decimal(10,2)) as sale_succeed_number,
         |cast(sum(case when paid = 2 and refund = 0 then payment_total_money else 0 end)as  decimal(10,2)) as sale_succeed_money,
         |0.0 goods_transform_ratio,
         |$dt as dt
         |from
         |orders_retail
         |where po_type is null
         |group by shop_id
         |""".stripMargin).createOrReplaceTempView("shop_goods_sale_tmp")
    //统计在架商品数据
    spark.sql(
      s"""
         |select
         |shop_id,
         |count(distinct case when status = 4 then  item_id end) as item_number,
         |$dt as dt
         |from
         |dwd.fact_item
         |where end_zipper_time = '9999-12-31'
         |group by shop_id
        """.stripMargin).createOrReplaceTempView("putaway_goods")
    //----商品订单表销售信息表 汇总 在架商品表
    val shopGoodsSaleDF = spark.sql(
      s"""
         |select
         |a.shop_id,
         |a.item_number,
         |b.shop_name,
         |b.sale_number,
         |b.orders_user_number,
         |b.orders_number,
         |b.sale_goods_number,
         |b.sale_succeed_number,
         |b.sale_succeed_money,
         |b.goods_transform_ratio,
         |$dt as dt
         |from
         |putaway_goods as a
         |left join
         |shop_goods_sale_tmp as b
         |on a.shop_id = b.shop_id
         |""".stripMargin)
    writerMysql(shopGoodsSaleDF, "shop_goods_sale", flag)


    /**
     * 商品曝光率 --需要埋点
     * 有流量的商品的品种数占
     * 上架商品数量的比例
     */
    /**
     * 商品成交转化率   --需要埋点
     * 1.统计出商品访问数
     * 2.统计出成交客户数
     * 3.成交客户数/商品访问数
     */
    /**
     * 商品金额TOP 10
     * 1.统计支付人数
     * 2.统计支付金额
     * 3.统计支付金额占总支付金额比例
     */
    val shopGoodsMoneyTopDF = spark.sql(
      s"""
         |with t1 as(
         |select
         |shop_id,
         |item_name,
         |count(distinct buyer_id) as sale_user_count,
         |cast(sum(payment_total_money) as  decimal(10,2)) as sale_succeed_money
         |from
         |orders_retail
         |where paid = 2 and refund = 0
         |group by shop_id,item_name
         |),
         |t2 as (
         |select
         |shop_id,
         |item_name,
         |sale_user_count,
         |sale_succeed_money,
         |sum(sale_succeed_money) over(partition by shop_id) as total_money,
         |row_number() over(partition by shop_id,item_name order by sale_succeed_money desc) as money_top
         |from
         |t1
         |)
         |select
         |shop_id,
         |item_name,
         |sale_user_count,
         |sale_succeed_money,
         |cast(sale_succeed_money/total_money as  decimal(10,2)) as sale_ratio,
         |$dt as dt
         |from
         |t2
         |where money_top <=10
         |""".stripMargin)
    writerMysql(shopGoodsMoneyTopDF, "shop_goods_money_top", flag)
    /**
     * 商品利润TOP 10
     * 1.先计出每个订单的成本价
     * 2.统计每个商品总利润和访问人数
     * 3.统计商铺总利润和对每个商品的总利润排序
     * 4.取出利润最高的10个商品，并计算利润率(单个商品总利润/店铺总利润)
     */
    val shopGoodsProfitTopDF = spark.sql(
      s"""
         |with t1 as(
         |select
         |shop_id,
         |item_name,
         |buyer_id,
         |payment_total_money - (num * cost_price) as profit
         |from
         |orders_retail
         |where paid = 2 and refund = 0
         |),
         |t2 as(
         |select
         |shop_id,
         |item_name,
         |count(distinct buyer_id) as sale_user_count,
         |cast(sum(profit)as  decimal(10,2)) as sale_succeed_profit
         |from
         |t1
         |group by shop_id,item_name
         |),t3 as (
         |select
         |shop_id,
         |item_name,
         |sale_user_count,
         |sale_succeed_profit,
         |sum(sale_succeed_profit) over(partition by shop_id) as total_profit,
         |row_number() over(partition by shop_id,item_name order by sale_succeed_profit desc) as profit_top
         |from
         |t2
         |)
         |select
         |shop_id,
         |item_name,
         |sale_user_count,
         |cast(sale_succeed_profit as  decimal(10,2)) as sale_succeed_profit,
         |cast(sale_succeed_profit/total_profit as  decimal(10,2)) as sale_profit_ratio,
         |$dt as dt
         |from
         |t3
         |where profit_top <=10
         |""".stripMargin)
    writerMysql(shopGoodsProfitTopDF, "shop_goods_profit_top", flag)

    // 删除缓存表
    spark.sqlContext.uncacheTable("orders_retail")
    //1按商品
    //2按供应商
    //3按类目
    spark.sqlContext.cacheTable("purchase_tmp")
    val shopGoodsPurchaseInfoDF = spark.sql(
      s"""
         |select
         |shop_id,
         |sku_mapping(sku_id) as name,
         |'1' as pu_type, --1 为按商品
         |sum(num) as pu_num,
         |cast(sum(payment_total_money)as  decimal(10,2)) as pu_money,
         |$dt as dt
         |from
         |purchase_tmp
         |group by
         |shop_id,sku_id
         |""".stripMargin).union(spark.sql(
      s"""
         |select
         |shop_id,
         |user_mapping(seller_id) as name,
         |'2' as pu_type, -- 2供应商
         |sum(num) as pu_num,
         |cast(sum(payment_total_money)as  decimal(10,2)) as pu_money,
         |$dt as dt
         |from
         |purchase_tmp where sku_id is not null
         |group by
         |shop_id,seller_id
         |""".stripMargin)
    ).union(spark.sql(
      s"""
         |select
         |t1.shop_id,
         |cid_three_mapping(t3.cat_3d_id) as name,
         |'3' as pu_type, -- 3类目
         |sum(num) as pu_num,
         |cast(sum(payment_total_money)as  decimal(10,2)) as pu_money,
         |$dt as dt
         |from
         |purchase_tmp t1
         |left join
         |(select * from dwd.dim_goods_cat where dt=$dt) t3
         |on t1.cid = t3.cat_3d_id
         |group by t1.shop_id,t3.cat_3d_id
         |""".stripMargin)
    )
    writerMysql(shopGoodsPurchaseInfoDF, "shop_goods_purchase_info", flag)
  }
}
