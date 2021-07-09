package dwd

import app.App
import org.apache.commons.lang3.time.DateUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import udf.UDFRegister

/**
 * @authorljh
 * @version1.0
 *
 * 对ODS层数据进行清洗/转换/过滤,统一规划字段名称,统一字段格式
 */
class DwdETL(spark: SparkSession, dt: String) {
  def process(): Unit = {
    /**
     * 订单表
     * 订单和订单明细关联
     * 全部订单维度 和TC 零售订单维度从 该表计算
     **/
    spark.sql(
      s"""
        |select
        |*
        |from
        |dwd.fact_orders
        |where dt = $dt and parent_order_id != 0 and end_zipper_time = '9999-12-31'
        |""".stripMargin).createOrReplaceTempView("orders")
    spark.sql(
      s"""
         |select
         |*
         |from
         |ods.ods_orders_detail
         |where dt =$dt
         |""".stripMargin).createOrReplaceTempView("orders_detail")
    // dwd_fact_order_info 订单明细事务表 insert overwrite table dwd.dwd_fact_order_info
    spark.sql(
      """
        |select
        |a.order_id,
        |a.shop_id,
        |a.shop_name,
        |a.order_type,
        |a.buyer_id,
        |a.order_source,
        |a.paid,
        |case when order_type = 1 then "TC" else "TB" end as order_type,
        |case when (order_type = 6 or order_type = 8) then "PO" end as po_type, -- 采购
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
    /**
     * 出库订单表
     * 订单 和 出库表关联 过滤出渠道订单信息
     * TB 渠道订单信息 从该表输出
     * */
    spark.sql(
      s"""
        |select
        |*
        |from
        |dwd.fact_outbound_bill
        |where dt=$dt and shop_id is not null and end_zipper_time = '9999-12-31'
        |""".stripMargin).createOrReplaceTempView("outbound_bill")
    // dwd.fact_outbound_bill_info   insert overwrite table dwd.fact_outbound_bill_info
    spark.sql(
      s"""
        |
        |select
        |a.id,
        |a.shop_id,
        |a.type,
        |b.freight_money,
        |c.order_id,
        |c.order_detail_id,
        |c.cid,
        |c.brand_id,
        |c.item_id,
        |c.item_name,
        |c.sku_id,
        |c.order_num,
        |c.price,
        |d.cost_price
        |from
        |outbound_bill a
        |inner join
        |(
        |select
        |order_id,
        |freight_money
        |from
        |orders
        |where order_type != 1
        |) b
        |on a.order_id = b.order_id
        |inner join
        |(
        |select
        |*
        |from
        |ods.ods_outbound_bill_detail
        |where dt=$dt
        |) c
        |on a.id=c.outbound_bill_id
        |inner join
        |orders_detail d
        |on c.order_detail_id = d.id
        |""".stripMargin)












    /***
     *商品 类目维度聚合
     */
    spark.sql(
      s"""
         |select
         |t3.cid as cat_3d_id,   --三级类目id
         |t3.name as cat_3d_name,  --三级类目名称
         |t2.cid as cat_2d_id,  --二级类目id
         |t2.name as cat_2d_name, --二级类目名称
         |t1.cid as cat_1t_id, --一级类目id
         |t1.name as cat_1t_name --一级类目名称
         |from
         |(select
         |*
         |from ods.ods_item_category
         |where level = 3 and  dt=$dt) t3
         |left join
         |(select
         |*
         |from ods.ods_item_category
         |where level = 2 and  dt=$dt) t2
         |on t3.parent_cid = t2.cid
         |left join
         |(select
         |*
         |from ods.ods_item_category
         |where level = 1 and  dt=$dt) t1
         |on t2.parent_cid = t1.cid
         |""".stripMargin)

  }
}
