package dwd

import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import udf.UDFRegister

/**
 * @authorljh
 * @version1.0
 *
 * 对ODS层数据进行清洗/转换/过滤,统一规划字段名称,统一字段格式
 */
class DwdETL(spark: SparkSession, dt: String){
  def process(): Unit = {
    UDFRegister.FileIpMapping(spark)
    val yesterDay = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).minusDays(1).toString("yyyyMMdd")
    /**
     * 订单表
     * 订单和订单明细关联
     * 之后关联上 自提点 维度
     * 全部订单维度 和TC 零售订单维度从 该表计算
     **/
    //订单
    spark.sql(
      s"""
        |select
        |*
        |from
        |dwd.fact_orders
        |where dt = $dt and parent_order_id != 0 and end_zipper_time = '9999-12-31'
        |""".stripMargin).createOrReplaceTempView("orders")
    //订单明细
    spark.sql(
      s"""
         |select
         |*
         |from
         |ods.ods_orders_detail
         |where dt =$dt
         |""".stripMargin).createOrReplaceTempView("orders_detail")
    //自提点
    spark.sql(
      s"""
        |select
        |*
        |from
        |dwd.fact_orders_self_pick
        |where dt=$dt
        |""".stripMargin).createOrReplaceTempView("orders_self_pick")
    //省市区
    spark.sql(
      s"""
        |select
        |order_id,
        |province_name,
        |city_name,
        |country_name
        |from ods.ods_orders_receive
        |where dt=$dt
        |""".stripMargin).createOrReplaceTempView("orders_receive")
    //用户
    spark.sql(
      """
        |select
        |id,
        |name
        |from
        |dwd.dim_user
        |where end_zipper_time='9999-12-31'
        |""".stripMargin).createOrReplaceTempView("user")
    spark.sqlContext.cacheTable("user")
    // dwd.dwd_fact_order_info 订单明细事务表 dwd.dwd_fact_order_info
    spark.sql(
      s"""
        |insert overwrite table dwd.dwd_fact_order_info
        |select
        |a.order_id,
        |a.shop_id,
        |a.shop_name,
        |a.buyer_id,
        |user.name as buyer_name,
        |a.order_source,
        |a.paid,
        |a.refund,
        |d.province_name,
        |d.city_name,
        |d.country_name,
        |case when order_type = 1 then "TC"
        |     when order_type != 1 and order_type != 11 then "TB" end as order_type,
        |case when (order_type = 6 or order_type = 8) then "PO" end as po_type, -- 采购
        |b.cid,
        |b.brand_id,
        |b.item_id,
        |b.sku_id,
        |b.item_name,
        |b.sku_pic_url,
        |c.pick_id, -- 自提点id
        |c.pick_name, -- 自提点Name
        |c.order_id as pick_order_id, -- 自提点订单
        |b.create_time, --订单时间
        |a.freight_money, --订单总运费
        |b.cost_price,--订单成本价
        |b.payment_price, --订单支付价
        |cast(b.num as decimal(24,2)) as payment_num, -- 支付数量
        |$dt
        |from
        |orders a
        |left join
        |orders_detail b
        |on a.order_id = b.order_id
        |left join
        |orders_self_pick c
        |on a.order_id = c.order_id
        |left join
        |orders_receive d
        |on a.order_id = d.order_id
        |left join
        |user
        |on a.buyer_id = user.id
        |""".stripMargin)
    /**
     * 出库订单表
     * 订单 和 出库表关联 过滤出渠道订单信息
     * 关联上自提点维度
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
    // dwd.dwd_fact_outbound_bill_info
    spark.sql(
      s"""
        |insert overwrite table dwd.dwd_fact_outbound_bill_info
        |select
        |a.id,
        |a.shop_id,
        |a.type,
        |f.province_name,
        |f.city_name,
        |f.country_name,
        |b.buyer_id,
        |user.name as buyer_name,
        |b.paid,
        |case when (b.order_type = 6 or b.order_type = 8) then "PO" end as po_type,
        |b.freight_money, --订单总运费
        |c.order_id,
        |c.order_detail_id,
        |c.cid,
        |c.brand_id,
        |c.item_id,
        |c.item_name,
        |c.sku_id,
        |e.pick_id, -- 自提点id
        |e.pick_name, -- 自提点Name
        |e.order_id as pick_order_id, -- 自提点订单
        |c.order_num as payment_num,
        |c.price as payment_price,
        |d.cost_price,
        |d.sku_pic_url,
        |$dt
        |from
        |outbound_bill a
        |inner join
        |(
        |select
        |order_id,
        |freight_money,
        |buyer_id,
        |order_type,
        |paid
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
        |left join
        |orders_self_pick e
        |on a.order_id = e.order_id
        |left join
        |orders_receive f
        |on a.order_id = f.order_id
        |left join
        |user
        |on b.buyer_id = user.id
        |""".stripMargin)
    //------------------------------------------------
    // 退货表
    spark.sql(
      s"""
         |select
         |id,
         |shop_id,
         |refund_no,
         |buyer_id,
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
    // 退货明细表
    spark.sql(
      s"""
         |select
         |refund_id as id,
         |order_id,
         |refund_id,
         |sku_id,
         |item_id,
         |sku_pic_url,
         |item_name,
         |refund_num,
         |refund_price
         |from
         |ods.ods_refund_detail
         |where dt =$dt
         |""".stripMargin).createOrReplaceTempView("refund_detail")
    // 退款流程表
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
    // dwd.dwd_fact_order_refund_info
    spark.sql(
      s"""
        |insert overwrite table dwd.dwd_fact_order_refund_info
        |select
        |a.id,
        |a.shop_id,
        |a.order_id,
        |a.refund_no,
        |a.buyer_id,
        |b.sku_id,
        |b.sku_pic_url,
        |b.item_name,
        |b.refund_num,
        |b.refund_price,
        |a.create_time,
        |a.modify_time,
        |a.refund_status,
        |a.refund_reason,
        |a.po_type,
        |a.order_type,
        |nvl(cast((unix_timestamp(max_time,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(min_time,'yyyy-MM-dd HH:mm:ss'))/60 as decimal(10,2)),0) as avg_time, --平均处理时间
        |$dt
        |from
        |refund_apply a
        |left join
        |refund_detail b
        |on a.id = b.id
        |left join
        |refund_process c
        |on a.id = c.refund_id
        |""".stripMargin)
    // dwd.dwd_click_log
    // 埋点
    spark.read.json(s"hdfs://bogon:8020/click_log/${dt}/")
      .where(
        (col("event") =!= "null") &&
          (col("shopId") =!= "") &&
          (col("shopId") =!= "null"))
      .createOrReplaceTempView("ods_page_log")
//    insert overwrite table dwd.dwd_click_log
    spark.sql(
      s"""
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
         |where (loginToken != '' and loginToken is not null) or (ip is not null and ip != '')
         |""".stripMargin)

    /**
     *  用户购物轨迹 表
     *     1. 先根据订单得到今日 订单用户
     *     2. 获取昨天 用户轨迹数据
     *     3. 更新用户购物时间
     *     4. 合并购物记录
     */
    spark.sql(
      s"""
         |select
         |distinct
         |shop_id,
         |case when order_type = 1 then "TC"
         |     when order_type != 1 and order_type != 11 then "TB" end as order_type,
         |case when (order_type = 6 or order_type = 8) then "PO" end as po_type,
         |paid,
         |buyer_id,
         |to_date(create_time) as first_time,
         |null as last_time,
         |to_date(create_time) final_time,
         |$dt as dt
         |from
         |orders
         |where  buyer_id is not null
         |""".stripMargin).createOrReplaceTempView("order_user")
    spark.sql(
      s"""
         |select
         |shop_id,
         |order_type,
         |po_type,
         |paid,
         |buyer_id,
         |first_time,
         |last_time,
         |final_time
         |from
         |dwd.dwd_dim_order_user_locus
         |where dt=$yesterDay
         |""".stripMargin).createOrReplaceTempView("dwd_dim_order_user_locus")
    spark.sql(
      s"""
        |insert overwrite table dwd.dwd_dim_order_user_locus
        |select
        |a.shop_id,
        |a.order_type,
        |a.po_type,
        |a.paid,
        |a.buyer_id,
        |a.first_time,
        |nvl(a.final_time,a.first_time) as last_time,
        |b.first_time as final_time,
        |$dt
        |from
        |dwd_dim_order_user_locus  a
        |left join
        |order_user b
        |on a.shop_id = b.shop_id and a.buyer_id=b.buyer_id
        |where b.buyer_id is not null
        |union all
        |select
        |a.shop_id,
        |a.order_type,
        |a.po_type,
        |a.paid,
        |a.buyer_id,
        |a.first_time,
        |null as last_time,
        |nvl(a.final_time,a.first_time) as final_time,
        |$dt
        |from
        |dwd_dim_order_user_locus  a
        |left join
        |order_user b
        |on a.shop_id = b.shop_id and a.buyer_id=b.buyer_id
        |where b.buyer_id is null
        |union all
        |select
        |b.shop_id,
        |b.order_type,
        |b.po_type,
        |b.paid,
        |b.buyer_id,
        |b.first_time,
        |b.last_time,
        |b.final_time,
        |$dt
        |from
        |dwd_dim_order_user_locus  a
        |right join
        |order_user b
        |on a.shop_id = b.shop_id and a.buyer_id=b.buyer_id
        |where a.buyer_id is null
        |""".stripMargin)

    // 会员数据
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
         |from dwd.dwd_dim_user_statistics
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
         |insert overwrite table dwd.dwd_dim_user_statistics
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
    // 客户
    spark.sql(
      s"""
         |insert overwrite table dwd.dwd_dim_shop_store
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
         |dwd.dwd_dim_shop_store
         |where dt = '$yesterDay'
         |""".stripMargin)

    /***
     *商品 类目维度聚合
     */
    //    spark.sql(
//      s"""
//         |select
//         |t3.cid as cat_3d_id,   --三级类目id
//         |t3.name as cat_3d_name,  --三级类目名称
//         |t2.cid as cat_2d_id,  --二级类目id
//         |t2.name as cat_2d_name, --二级类目名称
//         |t1.cid as cat_1t_id, --一级类目id
//         |t1.name as cat_1t_name --一级类目名称
//         |from
//         |(select
//         |*
//         |from ods.ods_item_category
//         |where level = 3 and  dt=$dt) t3
//         |left join
//         |(select
//         |*
//         |from ods.ods_item_category
//         |where level = 2 and  dt=$dt) t2
//         |on t3.parent_cid = t2.cid
//         |left join
//         |(select
//         |*
//         |from ods.ods_item_category
//         |where level = 1 and  dt=$dt) t1
//         |on t2.parent_cid = t1.cid
//         |""".stripMargin)
  }
}
