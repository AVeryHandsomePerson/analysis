package whole

import common.StarvConfig
import org.apache.spark.sql.{SaveMode, SparkSession}
import udf.UDFRegister
import utils.BroadcastUtils

/**
 * @author ljh
 * @date 2021/3/29 9:36
 * @version 1.0
 */
object PaaSGoodsAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DealAnlysis")
      .config("hive.exec.dynamici.partition", true)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()

    val dt = args(0)

    //注册udf
    UDFRegister.skuMapping(spark,dt)
    UDFRegister.industryMapping(spark,dt)
    UDFRegister.shopMapping(spark,dt)
    UDFRegister.ordersStatusMapping(spark)

    //商铺和商铺行业表关联
    spark.sql(
      s"""
        |select
        |b.shop_id,
        |b.operation_status,
        |b.shop_type,
        |b.province_name,
        |c.industry,
        |c.industry_name
        |from
        |(select
        |*
        |from ods.shop_info where dt = $dt) b
        |left join
        |(select
        | *
        |from ods.ods_plat_industry_rel  where dt = $dt) c
        |on b.plat_industry_rel_id = c.id
        |""".stripMargin).createOrReplaceTempView("shop")
    spark.sqlContext.cacheTable("shop")
    // 订单和商铺维度表
    spark.sql(
      s"""
         |select
         |a.shop_id,
         |a.order_id,
         |a.order_source,
         |a.paid,
         |a.refund,
         |a.status,
         |a.payment_time,
         |b.industry,
         |b.industry_name
         |from
         |(select
         |shop_id,
         |order_id,
         |refund,
         |status,
         |payment_time,
         |paid,
         |order_source
         |from dwd.fact_orders
         |where parent_order_id != 0
         |) a
         |left join
         |shop b
         |on a.shop_id = b.shop_id
         |""".stripMargin).createOrReplaceTempView("dw_shop_order")
    //获取订单和订单明细的汇总表
    spark.sql(
      """
        |select
        |*
        |from
        |dwd.dw_orders_merge_detail
        |""".stripMargin).createOrReplaceTempView("order")
    spark.sqlContext.cacheTable("order")
    //店铺和商品表关联
    spark.sql(
      s"""
         |select
         |t1.shop_id,
         |t2.item_id,
         |t3.cat_3d_name,
         |t3.cat_2d_name,
         |t3.cat_1t_name,
         |t3.cat_3d_id,
         |t3.cat_2d_id,
         |t3.cat_1t_id
         |from
         |(select
         |*
         |from ods.shop_info
         |where dt=$dt
         |) t1
         |left join
         |(
         |select
         |*
         |dwd.fact_item
         |where dt = $dt
         |) t2
         |on t1.shop_id = t2.shop_id
         |left join
         |(
         |select
         |*
         |from dwd.dim_goods_cat
         |where dt = $dt
         |) t3
         |on t2.cid = t3.cat_3d_id
         |""".stripMargin).createOrReplaceTempView("shop_cid")
    /**
     *-------------------------------商品数据分析
     * */
    //商品销售情况
    spark.sql(
      s"""
         |select
         |sku_id,
         |sku_mapping(sku_id) as sku_name,
         |sum(case when paid = 2 and refund = 0 then payment_total_money end) as sale_succeed_money,
         |count(case when paid = 2 and refund = 0 then 1 end) as orders_succeed_number, --销量
         |sum(case when paid = 2 and refund = 0 then num end) as sale_order_number, -- 总销售额
         |count(distinct case when paid = 2 and refund = 0 then buyer_id end)  as sale_user_number --支付人数
         |from
         |order
         |group by sku_id
         |""".stripMargin).createOrReplaceTempView("shop_all_money")
    //商品信息展示
    spark.sql(
      s"""
        |select
        |t1.sku_id,
        |t1.item_name,
        |concat(t3.cat_3d_name,'/',t3.cat_2d_name,'/',t3.cat_1t_name) as cat_name, -- 分类
        |orders_status_mapping(t1.status) as status, -- 状态
        |case when t2.orders_succeed_number is null then 0
        |else t2.orders_succeed_number end orders_succeed_number,--销量
        |case when t2.sale_order_number is null then 0
        |else t2.sale_order_number end sale_order_number, --总销售额
        |case when t2.sale_succeed_money is null then 0 else t2.sale_succeed_money
        |end as sale_succeed_money, --成交金额
        |case when round(t2.sale_succeed_money/t2.sale_user_number,2) is not null
        |then round(t2.sale_succeed_money/t2.sale_user_number,2)
        |else 0 end as money, --客单价
        |$dt as dt
        |from
        |dwd.dwd_sku_name t1
        |left join
        |shop_all_money t2
        |on t1.sku_id = t2.sku_id
        |left join
        |(select * from dwd.dim_goods_cat where dt=$dt) t3
        |on t1.cid = t3.cat_3d_id
        |order by orders_succeed_number desc
        |""".stripMargin)
//      .write
//      .mode(SaveMode.Append)
//      .jdbc(properties.get("url").toString(),"platform_goods_info",properties)
      .write
      .mode(SaveMode.Append)
      .jdbc(StarvConfig.url,"platform_goods_info",StarvConfig.properties)



    /**
     *-------------------------------商品单品分析
     *下单笔数
     *下单金额
     *付款笔数
     *付款金额 = 支付金额
     *支付率
     */
    spark.sql(
      s"""
         |SELECT
         |sku_id,
         |sku_mapping(sku_id) as sku_name,
         |count(distinct order_id) as order_number, --下单笔数
         |sum(payment_total_money) as order_money_number, --下单金额
         |count(distinct case when paid = 2  then order_id end) as payment_number, --付款笔数
         |sum(case when paid = 2  then payment_total_money end) as payment_money, --付款金额
         |round(count(distinct case when paid = 2 then buyer_id end) / count(distinct order_id),2) as payment_ratio, --支付率
         |$dt as dt
         |from
         |order
         |group by
         |sku_id
         |""".stripMargin)
      .write
      .mode(SaveMode.Append)
      .jdbc(StarvConfig.url,"goods_sale",StarvConfig.properties)
    //------------------------平台订单分布
    //平台订单信息
    spark.sql(
      s"""
        |select
        |count(distinct order_id) as order_number, --交易订单
        |round(sum(payment_total_money),2) as order_money_money, --订单金额
        |round(sum( case when paid = 2 and refund = 0 then payment_total_money end)
        |/
        |count(distinct case when paid = 2 and refund = 0 then buyer_id end),2)
        |as client_alone_money, --客单价
        |count(distinct case when paid = 2 then order_id end) as pay_number, --支付单数
        |$dt as dt
        |from
        |order
        |""".stripMargin)
      .write
      .mode(SaveMode.Append)
      .jdbc(StarvConfig.url,"platform_orders_sale",StarvConfig.properties)

    /**
     * 订单状态分布
     */
    spark.sql(
      s"""
        |select
        |status,
        |orders_status_mapping(status) as status_name,
        |count(distinct order_id) as status_type_number,
        |$dt as dt
        |from
        |dw_shop_order
        |group by
        |status
        |""".stripMargin)
      .write
      .mode(SaveMode.Append)
      .jdbc(StarvConfig.url,"platform_orders_status",StarvConfig.properties)
    /**
     * 消费时间段分布
     */
    spark.sql(
      s"""
         |select
         |hour(payment_time) as payment_time,
         |count(distinct order_id) as payment_order_number,
         |$dt as dt
         |from
         |dw_shop_order
         |where paid = 2
         |group by
         |hour(payment_time)
         |""".stripMargin)
      .write
      .mode(SaveMode.Append)
      .jdbc(StarvConfig.url,"platform_orders_time",StarvConfig.properties)
     /**
     * 交易成功：渠道比例
     */
     //统计总交易订单数
    val paymentUserNumber = spark.sql("select order_id from dw_shop_order where paid = 2 ").count()
    spark.sql(
      s"""
         |select
         |order_source,
         |round(count(order_id) / $paymentUserNumber,2) as payment_place_ratio,
         |$dt as dt
         |from
         |dw_shop_order
         |where paid = 2
         |group by
         |order_source
         |""".stripMargin)
      .write
      .mode(SaveMode.Append)
      .jdbc(StarvConfig.url,"platform_orders_source",StarvConfig.properties)
    /**
     * 包含：
     * 商家交易分析
     * ----------------
     * 商铺级别：下单成功行业分布比例
     * 全平台维度：后端做sum
     */
    spark.sql(
      s"""
         |select
         |shop_id,
         |industry,
         |industry_mapping(industry) as industry_name,
         |count(order_id) as payment_order_number,
         |$dt as dt
         |from
         |dw_shop_order
         |where paid = 2
         |group by
         |shop_id,industry
         |""".stripMargin)
      .write
      .mode(SaveMode.Append)
      .jdbc(StarvConfig.url,"shop_orders_industry",StarvConfig.properties)

    //---------------商家营收分析+商家交易分析:
    /***
     * 商家交易:
     * 店铺级别，平台级别 由后端sum得出
     * 总营业额：指商家的总营业额；
     * 待结算金额：指商家等待结算的金额（有些平台会将资金控制在平台内，等待结算后才到账）； -- 后续开发
     * 待退款金额：指商家店铺下待退款的金额；
     * 已退款金额：指商家店铺下已成功退款的金额；
     * 已到账金额：指商家已到账金额(一般用于提现，属于商家真实收入）； -- 后续添加
     * 退款率：指商家收到退款的订单笔数与同期成功交易（付款）的订单笔数的比率。
     */
    //订单成功总金额
    spark.sql(
      """
        |select
        |shop_id,
        |sum(payment_total_money) as all_payment_money
        |from
        |order
        |where paid = 2 and refund = 0
        |group by
        |shop_id
        |""".stripMargin).createOrReplaceTempView("all_money")
    //店铺下支付成功的人数
    spark.sql(
      """
        |select
        |shop_id,
        |count(1) as count_shop_number
        |from dw_shop_order
        |where paid = 2
        |group by shop_id
        |""".stripMargin).createOrReplaceTempView("shop_number")
    spark.sql(
      s"""
        |with t1 as (select
        |shop_id,
        |sum(case when refund_status != 5 and refund_status != 6 then refund_money end) as refund_moneys,
        |sum(case when refund_status = 6 then refund_money end) as refund_succeed_moneys,
        |count(case when refund_status = 6 then 1 end) as refund_succeed_number
        |from
        |dwd.dw_refund_orders
        |group by shop_id
        |)
        |select
        |a.shop_id,
        |shop_mapping(a.shop_id) shop_name,
        |b.all_payment_money, --成功总金额
        |a.refund_moneys,  -- 待退款金额
        |a.refund_succeed_moneys, --已退款金额
        |a.refund_succeed_number, --退款成功单数
        |c.count_shop_number, --店铺下支付成功的人数
        |case when a.refund_succeed_number/c.count_shop_number is null
        |then 0 else round(a.refund_succeed_number/c.count_shop_number,2) end as refund_ratio,
        |$dt as dt
        |from
        |t1 a
        |left join
        |all_money b
        |on a.shop_id = b.shop_id
        |left join
        |shop_number c
        |on a.shop_id = c.shop_id
        |""".stripMargin)
      .write
      .mode(SaveMode.Append)
      .jdbc(StarvConfig.url,"shop_refund_analysis",StarvConfig.properties)
    //商家交易:销售排行
    spark.sql(
      s"""
        |select
        |shop_id,
        |shop_mapping(shop_id) shop_name,
        |all_payment_money,
        |$dt as dt
        |from
        |all_money
        |order by all_payment_money desc
        |limit 10
        |""".stripMargin)
      .write
      .mode(SaveMode.Append)
      .jdbc(StarvConfig.url,"shop_sale_top",StarvConfig.properties)





    //--------------------商家数据分析:
    // 商家数量,营业数量，打烊数量, 商家类型
    spark.sql(
      s"""
        |select
        |count(1) as shop_number,
        |count(case when operation_status = 1 then 1 end) as operation_number,
        |count(case when operation_status = 2 then 1 end) as no_operation_number,
        |sum(case when shop_type = "own_account_shop" then 1 end) as own_shop_number, --自营店
        |sum(case when shop_type = "flagship_shop" then 1 end) as flagship_shop_number, --旗舰店
        |sum(case when shop_type = "monopolized_store" then 1 end) as alone_shop_number, --专营店
        |$dt as dt
        |from shop
        |""".stripMargin)
      .write
      .mode(SaveMode.Append)
      .jdbc(StarvConfig.url,"shop_analysis_number",StarvConfig.properties)
    //经营品类分布
   spark.sql(
      s"""
         |select
         |cat_1t_name,
         |count(1) as shop_number,
         |$dt as dt
         |from shop_cid
         |group by cat_1t_name
         |""".stripMargin)
      .write
      .mode(SaveMode.Append)
      .jdbc(StarvConfig.url,"shop_cid_number",StarvConfig.properties)
    //全平台行业分布比例
    spark.sql(
      s"""
         |select
         |industry,
         |industry_mapping(industry) as industry_name,
         |count(1) as payment_order_number,
         |$dt as dt
         |from
         |shop
         |group by
         |industry
         |""".stripMargin)
      .write
      .mode(SaveMode.Append)
      .jdbc(StarvConfig.url,"shop_industry_number",StarvConfig.properties)
    /**
     * 商家地域分布
     * 商家省份top 10 放置后端排序
     */
    spark.sql(
      s"""
         |select
         |province_name,
         |count(1) as province_number,
         |$dt as dt
         |from
         |shop
         |group by province_name
         |""".stripMargin)
      .write
      .mode(SaveMode.Append)
      .jdbc(StarvConfig.url,"shop_province_number",StarvConfig.properties)
//      .write
//      .mode(SaveMode.Append)
//      .jdbc(properties.get("url").toString(),"shop_province_number",properties)



    //----------------------商家商品数据
    //商铺维度：动销商品
    spark.sql(
      """
        |select
        |shop_id,
        |count(distinct sku_id) as sale_number
        |from
        |order
        |where paid = 2
        |group by shop_id
        |""".stripMargin).createOrReplaceTempView("shop_sale_number")
    //商铺维度：商品总数
    spark.sql(
      s"""
         |with t1 as(
         |select
         |shop_id,
         |count(distinct item_id) as item_number,
         |count(distinct case when status = 4 then  item_id end) as shelf_item_number
         |from
         |dwd.fact_item
         |where dt = $dt
         |group by shop_id
         |)
         |select
         |t1.shop_id,
         |shop_mapping(t1.shop_id) as shop_name,
         |t1.item_number,
         |t1.shelf_item_number,
         |t2.sale_number,
         |$dt as dt
         |from t1
         |left join
         |shop_sale_number t2
         |on t1.shop_id = t2.shop_id
         |""".stripMargin)
      .write
      .mode(SaveMode.Append)
      .jdbc(StarvConfig.url,"shop_goods_number",StarvConfig.properties)
    //全平台动销商品
    val saleNumber = spark.sql(
      """
        |select
        |sku_id
        |from
        |order
        |where paid = 2
        |""".stripMargin).distinct().count()
    //全平台商品总数
    spark.sql(
      s"""
        |select
        |count(1) as item_number,
        |count(case when status = 4 then  item_id end) as shelf_item_number,
        |$saleNumber as sale_number,
        |$dt as dt
        |from
        |dwd.fact_item
        |where dt = $dt
        |""".stripMargin)
      .write
      .mode(SaveMode.Append)
      .jdbc(StarvConfig.url,"platform_goods_number",StarvConfig.properties)
    //被访问商品数需要提前进行埋点

    //平台下的类目下商品统计
    spark.sql(
      s"""
         |select
         |'2' status_type,
         |cat_2d_name,
         |count(distinct item_id)
         |from
         |shop_cid
         |group by cat_2d_id,cat_2d_name
         |""".stripMargin).union(spark.sql(
      s"""
         |select
         |'3' status_type,
         |cat_3d_name,
         |count(distinct item_id)
         |from
         |shop_cid
         |group by cat_2d_id,cat_3d_name
         |""".stripMargin)).union(spark.sql(
      s"""
         |select
         |'1' status_type,
         |cat_1t_name,
         |count(distinct item_id)
         |from
         |shop_cid
         |group by cat_1t_id,cat_1t_name
         |""".stripMargin))
      .write
      .mode(SaveMode.Append)
      .jdbc(StarvConfig.url,"platform_cat_number",StarvConfig.properties)




    spark.sql(
      s"""
        |select
        |count(distinct cat_1t_id) as cat_1t_number,
        |count(distinct cat_2d_id) as cat_2d_number,
        |count(cat_3d_id) as cat_3d_number,
        |$dt as dt
        |from
        |dwd.dim_goods_cat
        |where dt =$dt
        |""".stripMargin)
      .write
      .mode(SaveMode.Append)
      .jdbc(StarvConfig.url,"platform_cat_number",StarvConfig.properties)
    //店铺下的类目统计
    spark.sql(
      s"""
         |select
         |shop_id,
         |shop_mapping(shop_id) shop_name,
         |count(distinct cat_1t_id) as cat_1t_number,
         |count(distinct cat_2d_id) as cat_2d_number,
         |count(cat_3d_id) as cat_3d_number,
         |$dt as dt
         |from
         |shop_cid
         |group by shop_id
         |""".stripMargin)
      .write
      .mode(SaveMode.Append)
      .jdbc(StarvConfig.url,"shop_cat_number",StarvConfig.properties)







  }
}
