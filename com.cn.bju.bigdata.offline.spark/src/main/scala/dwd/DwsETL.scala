package dwd

import common.StarvConfig
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime
import shop.WriteOffLineBase

/**
 * @authorljh
 * @version1.0
 *
 * 对ODS层数据进行清洗/转换/过滤,统一规划字段名称,统一字段格式
 */
class DwsETL(spark: SparkSession, dt: String) extends WriteOffLineBase{
  def process(): Unit = {
    val partitionDt = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).toString("yyyy-MM-dd")
    // DWS 店铺用户订单维度 汇总 insert overwrite table dws.dws_shop_deal_info
    val dwsShopDealInfoDF = spark.sql(
      s"""
         |select
         |shop_id,
         |buyer_id,
         |buyer_name,
         |sku_id,item_name,pick_id,pick_name,province_name,city_name,country_name,sku_pic_url,paid,refund,po_type,
         |'TC' as order_type,
         |count(1) as sale_order_number, --下单笔数
         |count(distinct pick_order_id) as sale_pick_order_number, --自提点订单数
         |nvl(sum(case when paid = 2 and refund = 0 then payment_num end),0) as payment_num,  --支付件数
         |nvl(cast(sum(case when paid = 2 and refund = 0 then (payment_num * payment_price)+freight_money end) as  decimal(10,2)),0) as payment_succeed_money, -- 支付成功金额
         |count(case when paid = 2 and refund = 0 then 1 end) as payment_succeed_number, --支付成功数据(成交单量)
         |cast(sum(cost_price * payment_num) as  decimal(10,2) ) as income_money, -- 收入金额
         |cast(sum((payment_num * payment_price)+freight_money) as  decimal(10,2)) as payment_money, --下单金额
         |'$partitionDt' as dt
         |from
         |dwd.dwd_fact_order_info
         |where  dt = $dt and  order_type = 'TC'
         |group by shop_id,buyer_id,buyer_name,sku_id,item_name,pick_id,pick_name,province_name,city_name,country_name,sku_pic_url,paid,refund,po_type
         |union all
         |select
         |shop_id,
         |buyer_id,
         |buyer_name,
         |sku_id,item_name,pick_id,pick_name,province_name,city_name,country_name,sku_pic_url,paid,refund,po_type,
         |'ALL' as order_type,
         |count(1) as sale_order_number, --下单笔数
         |count(distinct pick_order_id) as sale_pick_order_number, --自提点订单数
         |nvl(sum(case when paid = 2 and refund = 0 then payment_num end),0) as payment_num,  --支付件数
         |nvl(cast(sum(case when paid = 2 and refund = 0 then (payment_num * payment_price)+freight_money end) as  decimal(10,2)),0) as payment_succeed_money, -- 支付成功金额
         |count(case when paid = 2 and refund = 0 then 1 end) as payment_succeed_number, --支付成功数据(成交单量)
         |cast(sum(cost_price * payment_num) as  decimal(10,2) ) as income_money, -- 收入金额
         |cast(sum((payment_num * payment_price)+freight_money) as  decimal(10,2)) as payment_money, --下单金额
         |'$partitionDt' as dt
         |from
         |dwd.dwd_fact_order_info
         |where  dt = $dt
         |group by shop_id,buyer_id,buyer_name,sku_id,item_name,pick_id,pick_name,province_name,city_name,country_name,sku_pic_url,paid,refund,po_type
         |union all
         |select
         |shop_id,
         |buyer_id,
         |buyer_name,
         |sku_id,item_name,pick_id,pick_name,province_name,city_name,country_name,sku_pic_url,paid,type as refund,po_type,
         |'TB' as order_type,
         |count(1) as sale_order_number, --下单笔数
         |count(distinct pick_order_id) as sale_pick_order_number, --自提点订单数
         |nvl(sum(case when paid = 2 and type != 9 and type != 10 then payment_num end),0) as payment_num,  --支付件数
         |nvl(cast(sum(case when paid = 2 and type != 9 and type != 10 then (payment_num * payment_price)+freight_money end) as  decimal(10,2)),0) as payment_succeed_money, -- 支付成功金额
         |count(case when paid = 2 and type != 9 and type != 10 then 1 end) as payment_succeed_number, --支付成功数据(成交单量)
         |cast(sum(cost_price * payment_num) as  decimal(10,2) ) as income_money, -- 收入金额
         |cast(sum((payment_num * payment_price)+freight_money) as  decimal(10,2)) as payment_money, --下单金额
         |'$partitionDt' as dt
         |from
         |dwd.dwd_fact_outbound_bill_info
         |where dt = $dt
         |group by shop_id,buyer_id,buyer_name,sku_id,item_name,pick_id,pick_name,province_name,city_name,country_name,sku_pic_url,paid,type,po_type
         |""".stripMargin)
   writerClickHouse(dwsShopDealInfoDF,"dws_shop_deal_info")
    /***
     * refund_ratio, --退款率 (refund_number --成功退款笔数/orders_succeed_number --即成交单量)
     * refund_reason_ratio 退款原因比 :
     * count(distinct refund_reason) 商品的退款原因数量 / refund_reason_number 总退款数量
     */
    // 店铺,商品退货维度
    val orderRefundInfoDF = spark.sql(
      s"""
         |select
         |shop_id,
         |refund_reason,
         |sku_id,
         |sku_pic_url,
         |item_name,
         |'TB' as order_type,
         |count(1) as refund_reason_number, -- 店铺下每个商品的总退款单数
         |count(case when refund_status = 6 then 1 end) as refund_number, --成功退款数量
         |cast(sum(case when refund_status = 6 then refund_num * refund_price else 0 end) as decimal(10,2)) as refund_money, --成功退款金额
         |cast (sum(refund_num * refund_price) as  decimal(10,2))as all_money, -- 申请退款金额
         |avg(avg_time) as avg_time,
         |'$partitionDt' as dt
         |from
         |dwd.dwd_fact_order_refund_info
         |where  dt=$dt and order_type = 'TB'
         |group by shop_id,refund_reason,sku_id,item_name,sku_pic_url
         |union all
         |select
         |shop_id,
         |refund_reason,
         |sku_id,
         |sku_pic_url,
         |item_name,
         |'TC' as order_type,
         |count(1) as refund_reason_number, -- 店铺下每个商品的总退款单数
         |count(case when refund_status = 6 then 1 end) as refund_number, --成功退款数量
         |cast(sum(case when refund_status = 6 then refund_num * refund_price else 0 end) as decimal(10,2)) as refund_money, --成功退款金额
         |cast (sum(refund_num * refund_price) as  decimal(10,2))as all_money, -- 申请退款金额
         |avg(avg_time) as avg_time,
         |'$partitionDt' as dt
         |from
         |dwd.dwd_fact_order_refund_info
         |where  dt=$dt and order_type = 'TC'
         |group by shop_id,refund_reason,sku_id,item_name,sku_pic_url
         |union all
         |select
         |shop_id,
         |refund_reason,
         |sku_id,sku_pic_url,item_name,
         |'ALL' as order_type,
         |count(1) as refund_reason_number, -- 店铺下每个商品的总退款单数
         |count(case when refund_status = 6 then 1 end) as refund_number, --成功退款数量
         |cast(sum(case when refund_status = 6 then refund_num * refund_price else 0 end) as decimal(10,2)) as refund_money, --成功退款金额
         |cast (sum(refund_num * refund_price) as  decimal(10,2))as all_money, -- 申请退款金额
         |avg(avg_time) as avg_time,
         |'$partitionDt' as dt
         |from
         |dwd.dwd_fact_order_refund_info
         |where dt=$dt
         |group by shop_id,refund_reason,sku_id,item_name,sku_pic_url
         |""".stripMargin)
//    writerClickHouse(orderRefundInfoDF,"dws_shop_deal_refund_info")
    // 流量点击
    spark.sql(
      s"""
        |select
        |*
        |from
        |dwd.dwd_click_log
        |where dt = $dt
        |""".stripMargin).createOrReplaceTempView("dwd_click_log")
    val clickLogDF = spark.sql(
      s"""
         |select
         |shopId as shop_id,
         |loginToken,
         |ip,
         |userId,
         |page_source as order_type,
         |count(*) as pv,
         |(max(timeIn) - min(timeIn))/1000 as time,
         |case when count(1) > 1 then 0 else 1 end as one_visit_page,  --只访问一次页面
         |'$partitionDt' as dt
         |from
         |dwd_click_log
         |where (loginToken != '' or ip is not null)
         |group by shopId,loginToken,ip,userId,page_source
         |""".stripMargin)
//    writerClickHouse(clickLogDF,"dws_shop_clicklog_info")
    //--------------- 用户
    // 订单用户轨迹
    val userLocusInfo = spark.sql(
      s"""
         |select
         |shop_id,
         |order_type,
         |po_type,
         |paid,
         |buyer_id,
         |first_time,
         |last_time,
         |final_time,
         |'$partitionDt' as dt
         |from
         |dwd.dwd_dim_order_user_locus
         |where dt = $dt and final_time = '$partitionDt'
         |""".stripMargin)
//    writerClickHouse(userLocusInfo,"dws_shop_client_info")
    // 会员 关注店铺 客户
    spark.sql(
      s"""
         |select
         |*
         |from
         |dwd.dwd_dim_user_statistics
         |where dt = $dt
         |""".stripMargin).createOrReplaceTempView("user_statistics")
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
    spark.sql(
      s"""
         |select
         |*
         |from
         |dwd.dwd_dim_shop_store
         |where dt = $dt
         |""".stripMargin).createOrReplaceTempView("shop_store")
    val userInfoDF = spark.sql(
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
         |),
         |t2 as (
         |select
         |shop_id,
         |count(1)  as all_client_user,
         |count(case when regexp_replace(to_date(create_time),"-","") == $dt then seller_id end) as new_client_user_number,
         |0 as user_access_number,
         |$dt as dt
         |from
         |shop_store
         |group by shop_id
         |)
         |select
         |case when t1.shop_id is null and t2.shop_id is null then b.shop_id
         |     when t1.shop_id is null and b.shop_id is null then t2.shop_id
         |     else t1.shop_id end as shop_id,
         |nvl(t1.all_user,0) as all_vip_user_number,
         |nvl(t1.new_user_number,0) as new_vip_user_number,
         |nvl(t1.user_access_number,0) as vip_user_access_number,
         |nvl(t1.vip_user_up,0) as vip_user_up,
         |nvl(b.attention_number,0) as attention_number,
         |nvl(b.new_attention_number,0) as new_attention_number,
         |nvl(t2.all_client_user,0) as all_client_user_number,
         |nvl(t2.new_client_user_number,0) as new_client_user_number,
         |'$partitionDt' as dt
         |from
         |t1
         |full outer join
         |user_all_attention b
         |on t1.shop_id = b.shop_id
         |full outer join
         |t2
         |on t1.shop_id = t2.shop_id
         |""".stripMargin)
//    writerClickHouse(userInfoDF,"shop_client_vip_info")

    spark.stop()
  }
}
