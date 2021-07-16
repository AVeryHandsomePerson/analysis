package middle

import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.joda.time.DateTime
import udf.UDFRegister

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
    //udf
    UDFRegister.FileIpMapping(spark)
    //获取订单表数据
    spark.sql(
      s"""
         |select
         |shop_id,
         |case when order_type = 1 then "TC" else "TB" end as order_type,
         |case when (order_type = 6 or order_type = 8) then "PO" end as po_type, -- 采购
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
     * 订单明细表中 一个商品一条数据
     * 多个商品对应订单表的一个order_id
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
     * 省市维度表 insert overwrite table dwd.dwd_dim_orders_receive_city
     */
    spark.sql(
      s"""
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
         |refund_no,
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
         |b.refund_no,
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
    spark.sql(
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
    /**
     * 不同店铺下的订单用户
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
    // 会员表合并每日新增用户 insert overwrite table dwd.dwd_user_statistics
    spark.sql(
      s"""
         |select
         |shop_id,
         |vip_name,
         |user_id,
         |user_grade_code,
         |vip_user_up,
         |vip_user_down,
         |grade_name,
         |vip_status,
         |create_time
         |from dwd.dwd_user_statistics
         |where dt=$yesterDay
         |""".stripMargin).createOrReplaceTempView("user_statistics")
    spark.sql(
      s"""
         |select
         |shop_id,
         |vip_name,
         |user_id,
         |user_grade_code,
         |grade_name,
         |vip_status,
         |create_time,
         |$dt
         |from
         |ods.ods_user_statistics
         |where dt = $dt and shop_id is not null
         |""".stripMargin).createOrReplaceTempView("ods_user_statistics")
    spark.sql(
      s"""
         |insert overwrite table dwd.dwd_user_statistics
         |select  -- 更新老用户
         |a.shop_id,
         |a.vip_name,
         |a.user_id,
         |a.user_grade_code,
         |case when b.user_grade_code < a.user_grade_code then 1 else 0 end as vip_user_up,
         |case when b.user_grade_code > a.user_grade_code then 1 else 0 end as vip_user_down,
         |a.grade_name,
         |a.vip_status,
         |b.create_time,
         |$dt
         |from
         |ods_user_statistics a
         |inner join
         |user_statistics b
         |on a.shop_id = b.shop_id and a.vip_name =b.vip_name and a.user_id = b.user_id
         |union all
         |select  --只有新用户数据
         |a.shop_id,
         |a.vip_name,
         |a.user_id,
         |a.user_grade_code,
         |0 vip_user_up,
         |0 vip_user_down,
         |a.grade_name,
         |a.vip_status,
         |a.create_time,
         |$dt
         |from
         |ods_user_statistics a
         |left join
         |user_statistics b
         |on a.shop_id = b.shop_id and a.vip_name =b.vip_name and a.user_id = b.user_id
         |where b.shop_id is null
         |union all
         |select  --老用户在今天没有出现的数据
         |b.shop_id,
         |b.vip_name,
         |b.user_id,
         |b.user_grade_code,
         |b.vip_user_up,
         |b.vip_user_down,
         |b.grade_name,
         |b.vip_status,
         |b.create_time,
         |$dt
         |from
         |ods_user_statistics a
         |right join
         |user_statistics b
         |on a.shop_id = b.shop_id and a.vip_name =b.vip_name and a.user_id = b.user_id
         |where a.shop_id is null
         |""".stripMargin)
    // 客户表合并每日新增用户
    spark.sql(
      s"""
         |insert overwrite table dwd.dwd_shop_store
         |select
         |shop_id,
         |seller_id,
         |store_seller_id,
         |store_shop_id,
         |store_shop_name,
         |status,
         |type,
         |create_time,
         |$dt
         |from
         |ods.ods_shop_store
         |where dt =$dt
         |union all
         |select
         |shop_id,
         |seller_id,
         |store_seller_id,
         |store_shop_id,
         |store_shop_name,
         |status,
         |type,
         |create_time,
         |$dt
         |from
         |dwd.dwd_shop_store
         |where dt = '$yesterDay'
         |""".stripMargin)
    //仓库明细表 关联 仓库表
    spark.sql(
      s"""
         |insert overwrite table dwd.dwd_dim_outbound_bill
         |select
         |b.id,
         |b.shop_id,
         |b.type,
         |a.order_id,
         |a.order_detail_id,
         |a.cid,
         |a.brand_id,
         |a.item_id,
         |a.sku_id,
         |a.order_num,
         |a.price,
         |$dt as dt
         |from
         |(
         |select
         |*
         |from
         |ods.ods_outbound_bill_detail
         |where dt=$dt
         |) a
         |left join
         |(
         |select
         |*
         |from
         |dwd.fact_outbound_bill
         |where dt=$dt
         |) b
         |on
         |a.outbound_bill_id = b.id
         |""".stripMargin)
    // 自提点信息和订单，订单明细中间表，仓库表
    spark.sql(
      s"""
         |insert overwrite table dwd.dwd_dim_orders_self_pick
         |select
         |a.order_id,
         |a.shop_id,
         |b.pick_id,
         |a.status,
         |a.order_type,
         |a.cost_price,
         |a.item_original_price,
         |a.payment_total_money,
         |a.num,
         |c.order_num,
         |$dt as dt
         |from
         |(
         |select
         |*
         |from
         |dwd.dwd_dim_orders_detail
         |where dt=$dt
         |) a
         |inner join
         |(
         |select
         |*
         |from
         |dwd.fact_orders_self_pick
         |where dt=$dt
         |) b
         |on
         |a.order_id = b.order_id
         |left join
         |(
         |select
         |*
         |from
         |dwd.dwd_dim_outbound_bill
         |) c
         |on
         |a.order_id = c.order_id and a.item_id=c.order_id
         |""".stripMargin)

    // 埋点
    spark.read.json(s"hdfs://bogon:8020/click_log/${dt}/")
      .where(
        (col("event") =!= "null") &&
          (col("shopId") =!= "") &&
          (col("shopId") =!= "null"))
      .createOrReplaceTempView("ods_page_log")
    spark.sql(
      s"""
         |insert overwrite table dwd.dwd_click_log
         |select
         |domain,
         |ip,
         |referrer,
         |shopId,
         |timeIn,
         |title,
         |url,
         |event,
         |split(event,"_")[1] as page_source,
         |split(event,"_")[2] as page_type,
         |loginToken,
         |itemId,
         |skuId,
         |userId,
         |split(ip_mapping(ip),',')[0] as province,
         |split(ip_mapping(ip),',')[1] as city,
         |$dt
         |from
         |ods_page_log
         |""".stripMargin)
  }
}
