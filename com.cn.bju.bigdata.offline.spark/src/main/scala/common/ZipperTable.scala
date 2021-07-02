package common

import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

object ZipperTable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Zipper_Table")
      .config("hive.exec.dynamici.partition", true)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()


    val dt = args(0)
    val yesterDayDateTime = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).minusDays(1).toString("yyyy-MM-dd")
    val yesterDay = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).minusDays(1).toString("yyyyMMdd")

    /**
     * 订单拉链表
     */
    spark.sql(
      s"""
         |select
         |*
         |from
         |ods.ods_orders
         |where dt =$dt and parent_order_id != 0
         |""".stripMargin).createOrReplaceTempView("oders_tmp")
    spark.sql(
      s"""
         |insert overwrite table dwd.fact_orders
         |select
         |a.order_id                ,
         |a.parent_order_id         ,
         |a.order_type              ,
         |a.status                  ,
         |a.buyer_status            ,
         |a.seller_type             ,
         |a.order_platform          ,
         |a.order_source            ,
         |a.total_money             ,
         |a.freight_money           ,
         |a.discount_money          ,
         |a.cut_money               ,
         |a.other_fee               ,
         |a.round_down              ,
         |a.actually_payment_money  ,
         |a.buyer_id                ,
         |a.buyer_name              ,
         |a.buyer_shop_id           ,
         |a.buyer_shop_name         ,
         |a.seller_id               ,
         |a.seller_name             ,
         |a.shop_id                 ,
         |a.shop_name               ,
         |a.`option`                ,
         |a.paid                    ,
         |a.payment_source          ,
         |a.payment_time            ,
         |a.refund                  ,
         |a.exchange                ,
         |a.invoice                 ,
         |a.buyer_memo              ,
         |a.seller_memo             ,
         |a.is_change_price         ,
         |a.settle_flag             ,
         |a.evaluation              ,
         |a.create_time             ,
         |a.modify_time             ,
         |a.create_user             ,
         |a.modify_user             ,
         |a.deposit                 ,
         |a.retainage               ,
         |a.retainage_order_id      ,
         |a.presell_id              ,
         |a.presell_pay_type        ,
         |a.order_credit            ,
         |a.yn                      ,
         |a.manage_user_id          ,
         |a.manage_username         ,
         |a.buyer_manage_user_id    ,
         |a.buyer_manage_username   ,
         |a.purchase_date           ,
         |a.warehouse_code          ,
         |a.warehouse_name          ,
         |a.reason                  ,
         |a.audit_time              ,
         |a.audit_user_id           ,
         |a.audit_username          ,
         |a.remark                  ,
         |a.seller_org_code         ,
         |a.seller_org_parent_code  ,
         |a.buyer_org_code          ,
         |a.buyer_org_parent_code   ,
         |a.delivery_type           ,
         |a.print_price             ,
         |a.consignment             ,
         |a.store_complete          ,
         |a.balance_amount          ,
         |a.balance_flag            ,
         |a.issue_flag              ,
         |a.self_pick_flag          ,
         |a.expect_receive_time     ,
         |a.delivery_remark         ,
         |a.create_zipper_time,
         |case when b.order_id is not null and a.end_zipper_time ='9999-12-31'
         |then '$yesterDayDateTime'
         |else a.end_zipper_time
         |end as end_zipper_time,
         |--动态分区需要的字段
         |a.dt
         |from
         |(select * from dwd.fact_orders where dt > $yesterDay ) a
         |left join
         |oders_tmp b
         |on a.order_id = b.order_id
         |union all
         |select
         |order_id                ,
         |parent_order_id         ,
         |order_type              ,
         |status                  ,
         |buyer_status            ,
         |seller_type             ,
         |order_platform          ,
         |order_source            ,
         |total_money             ,
         |freight_money           ,
         |discount_money          ,
         |cut_money               ,
         |other_fee               ,
         |round_down              ,
         |actually_payment_money  ,
         |buyer_id                ,
         |buyer_name              ,
         |buyer_shop_id           ,
         |buyer_shop_name         ,
         |seller_id               ,
         |seller_name             ,
         |shop_id                 ,
         |shop_name               ,
         |`option`                ,
         |paid                    ,
         |payment_source          ,
         |payment_time            ,
         |refund                  ,
         |`exchange`              ,
         |invoice                 ,
         |buyer_memo              ,
         |seller_memo             ,
         |is_change_price         ,
         |settle_flag             ,
         |evaluation              ,
         |create_time             ,
         |modify_time             ,
         |create_user             ,
         |modify_user             ,
         |deposit                 ,
         |retainage               ,
         |retainage_order_id      ,
         |presell_id              ,
         |presell_pay_type        ,
         |order_credit            ,
         |yn                      ,
         |manage_user_id          ,
         |manage_username         ,
         |buyer_manage_user_id    ,
         |buyer_manage_username   ,
         |purchase_date           ,
         |warehouse_code          ,
         |warehouse_name          ,
         |reason                  ,
         |audit_time              ,
         |audit_user_id           ,
         |audit_username          ,
         |remark                  ,
         |seller_org_code         ,
         |seller_org_parent_code  ,
         |buyer_org_code          ,
         |buyer_org_parent_code   ,
         |delivery_type           ,
         |print_price             ,
         |consignment             ,
         |store_complete          ,
         |balance_amount          ,
         |balance_flag            ,
         |issue_flag              ,
         |self_pick_flag          ,
         |expect_receive_time     ,
         |delivery_remark         ,
         |case when modify_time is not null
         |then to_date(modify_time) else to_date(create_time)
         |end as create_zipper_time,
         |'9999-12-31' as end_zipper_time,
         |date_format(create_time,'yyyyMMdd')
         |from
         |oders_tmp
         |""".stripMargin)

    /**
     * 订单明细拉链表
     */
    spark.sql(
      s"""
         |select
         |*
         |from
         |ods.ods_orders_detail
         |where dt =$dt
         |""".stripMargin).createOrReplaceTempView("ods_orders_detail_tmp")
    spark.sql(
      s"""
         |insert overwrite table dwd.fact_orders_detail
         |select
         |a.id,
         |a.order_id,
         |a.item_id,
         |a.cid,
         |a.brand_id,
         |a.sku_id,
         |a.sku_code,
         |a.item_name,
         |a.sku_pic_url,
         |a.sku_sale_attr_str,
         |a.item_original_price,
         |a.cost_price,
         |a.payment_total_money,
         |a.payment_price,
         |a.cut_price,
         |a.cut_price_total,
         |a.refund,
         |a.exchange,
         |a.num,
         |a.sheet_num,
         |a.reel,
         |a.discount_money,
         |a.freight_template_id,
         |a.delivery_type,
         |a.seller_id,
         |a.shop_id,
         |a.buyer_item_id,
         |a.buyer_sku_id,
         |a.buyer_sku_code,
         |a.evaluation,
         |a.create_time,
         |a.modify_time,
         |a.create_user,
         |a.modify_user,
         |a.is_gift,
         |a.inbound_num,
         |a.outbound_num,
         |a.weight_unit,
         |a.width_unit,
         |a.length_unit,
         |a.purchase_num,
         |a.divided_balance,
         |a.delivery_date,
         |a.urgent_type,
         |a.already_cut,
         |a.already_outbound,
         |a.work_order_no,
         |a.create_zipper_time,
         |case when b.order_id is not null and a.end_zipper_time = '9999-12-31'
         |then '$yesterDayDateTime' else a.end_zipper_time  end as end_zipper_time,
         |a.dt
         |from
         |(select * from dwd.fact_orders_detail where dt > $yesterDay ) a
         |left join
         |ods_orders_detail_tmp b
         |on a.id = b.id
         |union all
         |select
         |id,
         |order_id,
         |item_id,
         |cid,
         |brand_id,
         |sku_id,
         |sku_code,
         |item_name,
         |sku_pic_url,
         |sku_sale_attr_str,
         |item_original_price,
         |cost_price,
         |payment_total_money,
         |payment_price,
         |cut_price,
         |cut_price_total,
         |refund,
         |exchange,
         |num,
         |sheet_num,
         |reel,
         |discount_money,
         |freight_template_id,
         |delivery_type,
         |seller_id,
         |shop_id,
         |buyer_item_id,
         |buyer_sku_id,
         |buyer_sku_code,
         |evaluation,
         |create_time,
         |modify_time,
         |create_user,
         |modify_user,
         |is_gift,
         |inbound_num,
         |outbound_num,
         |weight_unit,
         |width_unit,
         |length_unit,
         |purchase_num,
         |divided_balance,
         |delivery_date,
         |urgent_type,
         |already_cut,
         |already_outbound,
         |work_order_no,
         |case
         |    when modify_time is not null
         |        then to_date(modify_time)
         |    else to_date(create_time)
         |    end      as create_zipper_time,
         |'9999-12-31' as end_zipper_time,
         |date_format(create_time, 'yyyyMMdd')
         |from ods_orders_detail_tmp
         |""".stripMargin)

    /**
     * 商品
     */
    spark.sql(
      s"""
         |select
         |*
         |from
         |ods.ods_item
         |where dt=$dt
         |""".stripMargin).createOrReplaceTempView("item_tmp")
    spark.sql(
      s"""
         |insert overwrite table dwd.fact_item
         |select
         |a.item_id                 ,
         |a.brand_id                ,
         |a.cid                     ,
         |a.seller_id               ,
         |a.shop_id                 ,
         |a.shop_cid                ,
         |a.shop_freight_template_id,
         |a.attributes              ,
         |a.attr_sale               ,
         |a.status                  ,
         |a.type                    ,
         |a.item_name               ,
         |a.shelve_time             ,
         |a.off_shelve_time         ,
         |a.task_shelve_time        ,
         |a.task_off_shelve_time    ,
         |a.origin                  ,
         |a.weight                  ,
         |a.volume                  ,
         |a.length                  ,
         |a.width                   ,
         |a.height                  ,
         |a.ad                      ,
         |a.keyword                 ,
         |a.remark                  ,
         |a.unit_code               ,
         |a.unit_name               ,
         |a.quotation_way           ,
         |a.create_time             ,
         |a.create_user             ,
         |a.update_time             ,
         |a.update_user             ,
         |a.yn                      ,
         |a.sign                    ,
         |a.status_change_reason    ,
         |a.platform                ,
         |a.give_away               ,
         |a.pay_type                ,
         |a.sale_channel            ,
         |a.points                  ,
         |a.upc                     ,
         |a.goods_code              ,
         |a.attr_template_id        ,
         |a.`describe`              ,
         |a.ad_url                  ,
         |a.source_item_id          ,
         |a.master_item_id          ,
         |a.rebate_flag             ,
         |a.shop_sales_terr_temp_id ,
         |a.create_zipper_time,
         |case when b.item_id is not null and a.end_zipper_time = '9999-12-31'
         |then '$yesterDayDateTime' else a.end_zipper_time  end as end_zipper_time,
         |a.dt
         |from
         |(select * from dwd.fact_item) a
         |left join
         |item_tmp b
         |on a.item_id = b.item_id
         |union all
         |select
         |item_id,
         |brand_id,
         |cid,
         |seller_id,
         |shop_id,
         |shop_cid,
         |shop_freight_template_id,
         |attributes,
         |attr_sale,
         |status,
         |type,
         |item_name,
         |shelve_time,
         |off_shelve_time,
         |task_shelve_time,
         |task_off_shelve_time,
         |origin,
         |weight,
         |volume,
         |length,
         |width,
         |height,
         |ad,
         |keyword,
         |remark,
         |unit_code,
         |unit_name,
         |quotation_way,
         |create_time,
         |create_user,
         |update_time,
         |update_user,
         |yn,
         |sign,
         |status_change_reason,
         |platform,
         |give_away,
         |pay_type,
         |sale_channel,
         |points,
         |upc,
         |goods_code,
         |attr_template_id,
         |`describe`,
         |ad_url,
         |source_item_id,
         |master_item_id,
         |rebate_flag,
         |shop_sales_terr_temp_id,
         |case
         |    when update_time is not null
         |        then to_date(update_time)
         |    else to_date(create_time)
         |    end      as create_zipper_time,
         |'9999-12-31' as end_zipper_time,
         |date_format(create_time, 'yyyyMMdd')
         |from item_tmp
         |""".stripMargin)

    /**
     * 订单收货拉链表
     */
    spark.sql(
      s"""
         |select
         |*
         |from
         |ods.ods_orders_receive
         |where dt =$dt
         |""".stripMargin).createOrReplaceTempView("ods_orders_receive_tmp")
    spark.sql(
      s"""
         |insert overwrite table dwd.fact_orders_receive
         |select
         |a.id,
         |a.order_id,
         |a.consignee_name,
         |a.consignee_mobile,
         |a.consignee_phone,
         |a.consignee_mail,
         |a.province_code,
         |a.city_code,
         |a.country_code,
         |a.town_code,
         |a.province_name,
         |a.city_name,
         |a.country_name,
         |a.town_name,
         |a.detail_address,
         |a.create_time,
         |a.modify_time,
         |a.create_user,
         |a.modify_user,
         |a.yn,
         |a.create_zipper_time,
         |case when b.order_id is not null and a.end_zipper_time = '9999-12-31'
         |then '$yesterDayDateTime' else a.end_zipper_time  end as end_zipper_time,
         |a.dt
         |from
         |(select * from dwd.fact_orders_receive where dt >  $yesterDay ) a
         |left join
         |ods_orders_receive_tmp b
         |on a.id = b.id
         |union all
         |select
         |id,
         |order_id,
         |consignee_name,
         |consignee_mobile,
         |consignee_phone,
         |consignee_mail,
         |province_code,
         |city_code,
         |country_code,
         |town_code,
         |province_name,
         |city_name,
         |country_name,
         |town_name,
         |detail_address,
         |create_time,
         |modify_time,
         |create_user,
         |modify_user,
         |yn,
         |case
         |when modify_time is not null
         |    then to_date(modify_time)
         |else to_date(create_time)
         |end  as create_zipper_time,
         |'9999-12-31' as end_zipper_time,
         |date_format(create_time, 'yyyyMMdd')
         |from
         |ods_orders_receive_tmp
         |""".stripMargin)

    /**
     * 订单退货明细拉链表
     */
    spark.sql(
      s"""
         |select
         |*
         |from
         |ods.ods_refund_detail
         |where dt =$dt
         |""".stripMargin).createOrReplaceTempView("ods_refund_details_tmp")
    spark.sql(
      s"""
         |insert overwrite table dwd.fact_refund_detail
         |select
         |a.id                ,
         |a.refund_id         ,
         |a.order_id          ,
         |a.order_detail_id   ,
         |a.item_id           ,
         |a.item_name         ,
         |a.sku_id            ,
         |a.sku_code          ,
         |a.sku_pic_url       ,
         |a.sku_sale_attr_str ,
         |a.num               ,
         |a.transaction_money ,
         |a.refund_money      ,
         |a.refund_num        ,
         |a.refund_price      ,
         |a.notes             ,
         |a.create_time       ,
         |a.modify_time       ,
         |a.create_user       ,
         |a.modify_user       ,
         |a.yn                ,
         |a.buyer_item_id     ,
         |a.buyer_sku_id      ,
         |a.buyer_sku_code    ,
         |a.inbound_num       ,
         |a.outbound_num      ,
         |a.divided_balance   ,
         |a.create_zipper_time   ,
         |case when b.order_id is not null and a.end_zipper_time = '9999-12-31'
         |then '$yesterDayDateTime' else a.end_zipper_time  end as end_zipper_time,
         |a.dt
         |from
         |(select * from dwd.fact_refund_detail where dt >  $yesterDay ) a
         |left join
         |ods_refund_details_tmp b
         |on a.id = b.id
         |union all
         |select
         |id                ,
         |refund_id         ,
         |order_id          ,
         |order_detail_id   ,
         |item_id           ,
         |item_name         ,
         |sku_id            ,
         |sku_code          ,
         |sku_pic_url       ,
         |sku_sale_attr_str ,
         |num               ,
         |transaction_money ,
         |refund_money      ,
         |refund_num        ,
         |refund_price      ,
         |notes             ,
         |create_time       ,
         |modify_time       ,
         |create_user       ,
         |modify_user       ,
         |yn                ,
         |buyer_item_id     ,
         |buyer_sku_id      ,
         |buyer_sku_code    ,
         |inbound_num       ,
         |outbound_num      ,
         |divided_balance   ,
         |case when modify_time is not null
         |then to_date(modify_time) else to_date(create_time)
         |end as create_zipper_time,
         |'9999-12-31' as end_zipper_time,
         |date_format(create_time,'yyyyMMdd')
         |from
         |ods_refund_details_tmp
         |""".stripMargin)

    /**
     * 订单退货拉链表
     */
    spark.sql(
      s"""
         |select
         |*
         |from
         |ods.ods_refund_apply
         |where dt =$dt
         |""".stripMargin).createOrReplaceTempView("ods_refund_apply_tmp")
    spark.sql(
      s"""
         |insert overwrite table dwd.fact_refund_apply
         |select
         |a.id,
         |a.refund_no,
         |a.type,
         |a.refund_type,
         |a.order_id,
         |a.order_status,
         |a.refund_status,
         |a.refund_reason,
         |a.question_description,
         |a.receiving,
         |a.audit_id,
         |a.audit_name,
         |a.audit_time,
         |a.audit_notes,
         |a.refund_total_money,
         |a.apply_refund_money,
         |a.province_code,
         |a.province_name,
         |a.city_code,
         |a.city_name,
         |a.country_code,
         |a.country_name,
         |a.town_code,
         |a.town_name,
         |a.detail_address,
         |a.refund_address,
         |a.refund_receiver,
         |a.refund_mobile,
         |a.refund_directions,
         |a.create_time,
         |a.modify_time,
         |a.create_user,
         |a.modify_user,
         |a.yn,
         |a.manage_user_id,
         |a.manage_username,
         |a.seller_org_code,
         |a.seller_org_parent_code,
         |a.buyer_manage_user_id,
         |a.buyer_manage_username,
         |a.buyer_org_code,
         |a.buyer_org_parent_code,
         |a.create_flag,
         |a.buyer_id,
         |a.buyer_name,
         |a.buyer_shop_id,
         |a.buyer_shop_name,
         |a.seller_id,
         |a.seller_name,
         |a.shop_id,
         |a.shop_name,
         |a.inbound_num,
         |a.outbound_num,
         |a.all_item_num,
         |a.consignment,
         |a.store_complete,
         |a.balance_amount,
         |a.is_create_bound_bill,
         |a.create_zipper_time,
         |case when b.order_id is not null and a.end_zipper_time = '9999-12-31'
         |then '$yesterDayDateTime' else a.end_zipper_time  end as end_zipper_time,
         |a.dt
         |from
         |(select * from dwd.fact_refund_apply where dt >  $yesterDay ) a
         |left join
         |ods_refund_apply_tmp b
         |on a.id = b.id
         |union all
         |select
         |id,
         |refund_no,
         |type,
         |refund_type,
         |order_id,
         |order_status,
         |refund_status,
         |refund_reason,
         |question_description,
         |receiving,
         |audit_id,
         |audit_name,
         |audit_time,
         |audit_notes,
         |refund_total_money,
         |apply_refund_money,
         |province_code,
         |province_name,
         |city_code,
         |city_name,
         |country_code,
         |country_name,
         |town_code,
         |town_name,
         |detail_address,
         |refund_address,
         |refund_receiver,
         |refund_mobile,
         |refund_directions,
         |create_time,
         |modify_time,
         |create_user,
         |modify_user,
         |yn,
         |manage_user_id,
         |manage_username,
         |seller_org_code,
         |seller_org_parent_code,
         |buyer_manage_user_id,
         |buyer_manage_username,
         |buyer_org_code,
         |buyer_org_parent_code,
         |create_flag,
         |buyer_id,
         |buyer_name,
         |buyer_shop_id,
         |buyer_shop_name,
         |seller_id,
         |seller_name,
         |shop_id,
         |shop_name,
         |inbound_num,
         |outbound_num,
         |all_item_num,
         |consignment,
         |store_complete,
         |balance_amount,
         |is_create_bound_bill,
         |case when modify_time is not null
         |then to_date(modify_time) else to_date(create_time)
         |end as create_zipper_time,
         |'9999-12-31' as end_zipper_time,
         |date_format(create_time,'yyyyMMdd')
         |from
         |ods_refund_apply_tmp
         |where
         |""".stripMargin)
    /**
     * 自提点 拉链表
     */
    spark.sql(
      s"""
         |select
         |*
         |from
         |ods.ods_orders_self_pick
         |where dt =$dt
         |""".stripMargin).createOrReplaceTempView("ods_orders_self_pick")
    spark.sql(
      s"""
         |insert overwrite table dwd.fact_orders_self_pick
         |select
         |a.id,
         |a.order_id,
         |a.verification,
         |a.pick_id,
         |a.pick_name,
         |a.province_code,
         |a.province_name,
         |a.city_code,
         |a.city_name,
         |a.country_code,
         |a.country_name,
         |a.town_code,
         |a.town_name,
         |a.detail_address,
         |a.contact_phone,
         |a.contact_name,
         |a.confirm_time,
         |a.confirm_user,
         |a.create_time,
         |a.create_user,
         |a.modify_time,
         |a.modify_user,
         |a.create_zipper_time,
         |case when b.id is not null and a.end_zipper_time = '9999-12-31'
         |then '$yesterDayDateTime' else a.end_zipper_time  end as end_zipper_time,
         |a.dt
         |from
         |(select * from dwd.fact_orders_self_pick where dt =  $yesterDay ) a
         |left join
         |ods_orders_self_pick b
         |on a.id = b.id
         |union all
         |select
         |id,
         |order_id,
         |verification,
         |pick_id,
         |pick_name,
         |province_code,
         |province_name,
         |city_code,
         |city_name,
         |country_code,
         |country_name,
         |town_code,
         |town_name,
         |detail_address,
         |contact_phone,
         |contact_name,
         |confirm_time,
         |confirm_user,
         |create_time,
         |create_user,
         |modify_time,
         |modify_user,
         |to_date(create_time) as create_zipper_time,
         |'9999-12-31' as end_zipper_time,
         |date_format(create_time, 'yyyyMMdd')
         |from ods_orders_self_pick
         |""".stripMargin)
    //出库单拉链表
    spark.sql(
      s"""
         |select
         |*
         |from
         |ods.ods_outbound_bill
         |where dt =$dt
         |""".stripMargin).createOrReplaceTempView("ods_outbound_bill_tmp")
    spark.sql(
      s"""
         |insert overwrite table dwd.fact_outbound_bill
         |select
         |a.id,
         |a.type,
         |a.order_id,
         |a.shop_id,
         |a.shop_name,
         |a.buyer_shop_id,
         |a.buyer_shop_name,
         |a.status,
         |a.sign_status,
         |a.sign_time,
         |a.sign_shop_id,
         |a.sign_user_id,
         |a.real_sign_user,
         |a.sign_remark,
         |a.order_amount,
         |a.total_amount,
         |a.create_time,
         |a.audit_time,
         |a.audit_user_id,
         |a.audit_user_name,
         |a.audit_remark,
         |a.outbound_time,
         |a.modify_time,
         |a.modify_user,
         |a.warehouse_code,
         |a.warehouse_name,
         |a.manage_user_id,
         |a.manage_username,
         |a.org_code,
         |a.org_parent_code,
         |a.outbound_remark,
         |a.operate_user_id,
         |a.operate_user_name,
         |a.operate_time,
         |a.self_pick_flag,
         |a.license_plate_num,
         |a.driver_id,
         |a.driver_name,
         |a.logistics_code,
         |a.logistics_no,
         |a.freight_label_num,
         |a.cut_order_id,
         |a.consignee_name,
         |a.consignee_mobile,
         |a.consignee_mail,
         |a.province_code,
         |a.city_code,
         |a.country_code,
         |a.town_code,
         |a.province_name,
         |a.city_name,
         |a.country_name,
         |a.town_name,
         |a.detail_address,
         |a.expect_receive_time,
         |a.reclaim_status,
         |a.sign_voucher_pic,
         |a.in_warehouse_code,
         |a.in_warehouse_name,
         |a.cut_price_total,
         |a.other_fee,
         |a.create_zipper_time,
         |case when b.id is not null and a.end_zipper_time = '9999-12-31'
         |then '$yesterDayDateTime' else a.end_zipper_time  end as end_zipper_time,
         |a.dt
         |from
         |(select * from dwd.fact_outbound_bill where dt >  $yesterDay ) a
         |left join
         |ods_outbound_bill_tmp b
         |on a.id = b.id
         |union all
         |select
         |id,
         |type,
         |order_id,
         |shop_id,
         |shop_name,
         |buyer_shop_id,
         |buyer_shop_name,
         |status,
         |sign_status,
         |sign_time,
         |sign_shop_id,
         |sign_user_id,
         |real_sign_user,
         |sign_remark,
         |order_amount,
         |total_amount,
         |create_time,
         |audit_time,
         |audit_user_id,
         |audit_user_name,
         |audit_remark,
         |outbound_time,
         |modify_time,
         |modify_user,
         |warehouse_code,
         |warehouse_name,
         |manage_user_id,
         |manage_username,
         |org_code,
         |org_parent_code,
         |outbound_remark,
         |operate_user_id,
         |operate_user_name,
         |operate_time,
         |self_pick_flag,
         |license_plate_num,
         |driver_id,
         |driver_name,
         |logistics_code,
         |logistics_no,
         |freight_label_num,
         |cut_order_id,
         |consignee_name,
         |consignee_mobile,
         |consignee_mail,
         |province_code,
         |city_code,
         |country_code,
         |town_code,
         |province_name,
         |city_name,
         |country_name,
         |town_name,
         |detail_address,
         |expect_receive_time,
         |reclaim_status,
         |sign_voucher_pic,
         |in_warehouse_code,
         |in_warehouse_name,
         |cut_price_total,
         |other_fee,
         |case
         |when modify_time is not null
         |    then to_date(modify_time)
         |else to_date(create_time)
         |end as create_zipper_time,
         |'9999-12-31' as end_zipper_time,
         |date_format(create_time, 'yyyyMMdd')
         |from ods_outbound_bill_tmp
         |""".stripMargin)


  }
}
