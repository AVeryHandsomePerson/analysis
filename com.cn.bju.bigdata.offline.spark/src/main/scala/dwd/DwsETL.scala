package dwd

import org.apache.spark.sql.SparkSession

/**
 * @authorljh
 * @version1.0
 *
 * 对ODS层数据进行清洗/转换/过滤,统一规划字段名称,统一字段格式
 */
class DwsETL(spark: SparkSession, dt: String) {
  def process(): Unit = {

    // DWS 店铺下商品
    spark.sql(
      """
        |insert overwrite table dwd.dwd_fact_order_info
        |select
        |a.order_id,
        |a.shop_id,
        |a.shop_name,
        |a.order_type,
        |a.buyer_id,
        |a.order_source,
        |a.paid,
        |b.cid,
        |b.brand_id,
        |b.item_id,
        |b.sku_id,
        |b.item_name,
        |b.sku_pic_url,
        |b.create_time, --订单时间
        |a.freight_money, --订单总运费
        |b.cost_price,--订单成本价
        |b.payment_price, --订单支付价
        |cast(b.num as decimal(24,2)) as payment_num -- 支付数量
        |from
        |orders a
        |left join
        |orders_detail b
        |where a.order_id = b.order_id
        |""".stripMargin).show()







  }
}
