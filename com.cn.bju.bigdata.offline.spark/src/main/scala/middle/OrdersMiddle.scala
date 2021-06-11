package middle

import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

object OrdersMiddle {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Orders_Middle")
      .config("hive.exec.dynamici.partition", true)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    val dt = args(0)
    val yesterDay = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).minusDays(1).toString("yyyyMMdd")

    //获取订单表数据
    spark.sql(
      s"""
        |select
        |shop_id,
        |case when order_type = 1 then "TC" else "TB" end as order_type,
        |case when (order_type = 6 or order_type = 8) then "PO" end as po_type,
        |buyer_id,
        |order_id,
        |paid,
        |refund,
        |create_time,
        |order_source,
        |payment_time,
        |status,
        |actually_payment_money,
        |total_money,
        |seller_id
        |from
        |dwd.fact_orders
        |where parent_order_id != 0 and dt = $dt and end_zipper_time = '9999-12-31'
        |""".stripMargin).createOrReplaceTempView("orders")
    spark.sqlContext.cacheTable("orders")
    /**
     * 订单表和订单维度关联 生成订单维度表
     * 1.订单明细表关联存在多对一
     * 2.订单表中order_id 唯一
     *   订单明细表中 一个商品一条数据
     *   多个商品对应订单表的一个order_id
     */
    spark.sql(
      s"""
        |insert overwrite table dwd.dwd_dim_orders_detail
        |select
        |a.order_id,
        |a.payment_total_money,
        |a.item_name,
        |a.cost_price,
        |a.sku_id,
        |a.cid,
        |a.num,
        |b.shop_id,
        |b.order_type,
        |b.po_type,
        |b.buyer_id,
        |b.paid,
        |b.refund,
        |b.seller_id,
        |b.create_time,
        |b.payment_time,
        |b.status,
        |b.order_source,
        |a.item_id,
        |a.item_original_price,
        |$dt
        |from
        |(select
        |order_id,
        |if(item_original_price is null,0,item_original_price)
        |*
        |if(num is null,0,num) as payment_total_money,
        |item_name,
        |item_id,
        |cost_price,
        |sku_id,
        |cid,
        |item_original_price,
        |num
        |from dwd.fact_orders_detail
        |where dt=$dt
        |) a
        |left join
        |orders b
        |on a.order_id = b.order_id
        |""".stripMargin)
    /**
     * 订单和收货表关联
     * 订单和收货表为 1对1关系
     * 省市维度表
     */
    spark.sql(
      s"""
        |insert overwrite table dwd.dwd_dim_orders_receive_city
        |select
        |a.order_id,
        |a.shop_id,
        |a.order_type,
        |a.po_type,
        |a.buyer_id,
        |a.seller_id,
        |a.paid,
        |a.status,
        |a.create_time,
        |a.payment_time,
        |a.order_source,
        |a.total_money,
        |b.province_name,
        |b.city_name,
        |b.country_name,
        |a.refund,
        |$dt
        |from
        |orders a
        |left join
        |(
        |select
        |order_id,
        |province_name,
        |city_name,
        |country_name
        |from dwd.fact_orders_receive
        |where dt=$dt
        |) b
        |on a.order_id = b.order_id
        |""".stripMargin)
    //退货表
    spark.sql(
      s"""
        |select
        |id,
        |shop_id,
        |case when type = 1 then "TC" else "TB" end as order_type,
        |case when (type = 2 or type = 4) then "PO" end as po_type,
        |order_id,
        |refund_status,
        |refund_reason,
        |create_time,
        |modify_time
        |from dwd.fact_refund_apply
        |where dt = $dt and end_zipper_time = '9999-12-31'
        |""".stripMargin).createOrReplaceTempView("refund_apply")
    //退货明细表
    spark.sql(
      s"""
        |select
        |refund_id as id,
        |order_id,
        |refund_id,
        |sku_id,
        |refund_num,
        |refund_price
        |from dwd.fact_refund_detail
        |where dt = $dt and end_zipper_time = '9999-12-31'
        |""".stripMargin).createOrReplaceTempView("refund_detail")
    /**
     * 生成退货明细维度表
     */
    spark.sql(
      s"""
        |insert overwrite table dwd.dwd_dim_refund_detail
        |select
        |a.id,
        |b.shop_id,
        |a.order_id,
        |a.sku_id,
        |a.refund_num,
        |a.refund_price,
        |b.create_time,
        |b.modify_time,
        |b.refund_status,
        |b.refund_reason,
        |b.po_type,
        |b.order_type,
        |$dt
        |from
        |refund_detail a
        |left join
        |refund_apply b
        |on a.id = b.id
        |""".stripMargin)
    //生成sku 信息和item 商城表 关联出 sku 和 商品名称总表 按天分区
    spark.sql (
      s"""
        |insert overwrite table dwd.dwd_sku_name
        |select
        |a.sku_id,
        |b.item_name,
        |a.status,
        |b.cid,
        |$dt
        |from
        |(select
        |*
        |from
        |ods.ods_item_sku
        |where dt = $dt) a
        |left join
        |(
        |select
        |*
        |from
        |dwd.fact_item
        |where end_zipper_time = '9999-12-31'
        |) b
        |on a.item_id = b.item_id
        |""".stripMargin)
    //商品表维度数据拉宽
    spark.sql(
      s"""
        |insert overwrite table dwd.dim_goods_cat partition(dt=$dt)
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




//    where refund_id=315

    /**
     *不同店铺下的订单用户
     */
    // 前一天数据
    spark.sql(
      s"""
        |select
        |shop_id,
        |buyer_id,
        |first_time,
        |last_time,
        |final_time
        |from
        |dwd.dwd_user_order_locus
        |where dt=$yesterDay
        |""".stripMargin).createOrReplaceTempView("dwd_user_order_locus")
    // 当天数据
    spark.sql(
      s"""
         |select
         |distinct
         |shop_id,
         |buyer_id,
         |to_date(create_time) as first_time,
         |null last_time,
         |null final_time,
         |$dt as dt
         |from
         |dwd.fact_orders
         |where parent_order_id != 0 and dt = $dt and end_zipper_time = '9999-12-31' and buyer_id is not null
         |""".stripMargin).createOrReplaceTempView("order_tmp")
    // 1. 先更新已存在的用户  2。 过滤新用户  3. 跟旧用户 合并
    spark.sql(
      s"""
         |insert overwrite table dwd.dwd_user_order_locus
         |select  --更新 用户
         |a.shop_id,
         |a.buyer_id,
         |b.first_time,
         |b.final_time as last_time,
         |a.first_time as final_time,
         |$dt
         |from
         |order_tmp a
         |left join
         |dwd_user_order_locus b
         |on a.buyer_id = b.buyer_id  and a.shop_id = b.shop_id
         |where b.buyer_id is not null
         |union all
         |select --查询当天新用户
         |a.shop_id,
         |a.buyer_id,
         |a.first_time,
         |null last_time,
         |null final_time,
         |$dt
         |from
         |order_tmp a
         |left join
         |dwd_user_order_locus b
         |on a.buyer_id = b.buyer_id  and a.shop_id = b.shop_id
         |where b.buyer_id is null
         |union all
         |select  -- 合并老用户
         |a.shop_id,
         |a.buyer_id,
         |a.first_time,
         |a.last_time,
         |a.final_time,
         |$dt
         |from
         |dwd_user_order_locus a
         |left join
         |order_tmp b
         |on a.buyer_id = b.buyer_id and a.shop_id = b.shop_id
         |where  b.buyer_id is null
         |""".stripMargin)

  }
}
