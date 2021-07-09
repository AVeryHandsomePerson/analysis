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
class WarehouseAnalysis(spark: SparkSession,var dt: String, timeFlag: String) extends WriteBase {

  val log = Logger.getLogger(App.getClass)
  var flag = "";
  {
    val startTime = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).minusWeeks(1).toString("yyyyMMdd")
    log.info("===========> 仓库分析模块-开始注册UDF函数:")
    UDFRegister.skuMapping(spark, dt)
    UDFRegister.brandThreeMapping(spark, dt)
    UDFRegister.WarehouseThreeMapping(spark, dt)
    if (timeFlag.equals("day")) {
      log.info("===========> 仓库分析模块-天:" + dt)
      // 入库
      spark.sql(
        s"""
           |select
           |*
           |from
           |ods.ods_inbound_bill
           |where dt = $dt
           |and (date_format(create_time,'yyyyMMdd') = $dt
           |or date_format(modify_time,'yyyyMMdd') = $dt )
           |and status !=3 and status !=5
           |""".stripMargin).createOrReplaceTempView("inbound_bill")
      spark.sql(
        s"""
           |select
           |*
           |from
           |ods.ods_inbound_bill_detail
           |where dt = $dt
           |and date_format(create_time,'yyyyMMdd') = $dt
           |""".stripMargin).createOrReplaceTempView("inbound_bill_detail")
      spark.sql(
        s"""
          |select
          |shop_id,
          |item_name,sku_code,
          |warehouse_code,
          |brand_id,
          |IFNULL(inbound_num - used_num, 0) as inbound_num,
          |IFNULL(inbound_num - used_num, 0) * price as total_money,
          |price
          |from
          |ods.ods_inbound_bill_record where dt = $dt
          |""".stripMargin).createOrReplaceTempView("inbound_bill_record")
      spark.sqlContext.uncacheTable("inbound_bill_record")
      // 出库
      spark.sql(
        s"""
           |select
           |*
           |from
           |ods.ods_outbound_bill
           |where dt = $dt
           |and status !=3 and status !=5
           |""".stripMargin).createOrReplaceTempView("outbound_bill")
      spark.sql(
        s"""
           |select
           |*
           |from
           |ods.ods_outbound_bill_detail
           |where dt = $dt
           |""".stripMargin).createOrReplaceTempView("outbound_bill_detail")

    }
    else if (timeFlag.equals("week")) {
      log.info("===========> 仓库员表-周:" + startTime + "and" + dt)
      // 入库
      spark.sql(
        s"""
           |select
           |*
           |from
           |ods.ods_inbound_bill
           |where dt = $dt
           |and (
           |date_format(create_time,'yyyyMMdd') >= $startTime
           |or date_format(modify_time,'yyyyMMdd') >= $startTime
           |) and
           |(
           |date_format(create_time,'yyyyMMdd') <=$dt
           |or date_format(modify_time,'yyyyMMdd') <=$dt
           |)
           |and status !=3 and status !=5
           |""".stripMargin).createOrReplaceTempView("inbound_bill")
      spark.sql(
        s"""
           |select
           |*
           |from
           |ods.ods_inbound_bill_detail
           |where dt = $dt
           |and date_format(create_time,'yyyyMMdd') >= $startTime and date_format(create_time,'yyyyMMdd') <=$dt
           |""".stripMargin).createOrReplaceTempView("inbound_bill_detail")
      spark.sql(
        s"""
           |select
           |shop_id,
           |sku_id,
           |warehouse_code,
           |brand_id,
           |IFNULL(inbound_num - used_num, 0) as inbound_num,
           |IFNULL(inbound_num - used_num, 0) * price as total_money,
           |price
           |from
           |ods.ods_inbound_bill_record where dt = $dt
           |""".stripMargin).createOrReplaceTempView("inbound_bill_record")
      // 出库
      spark.sql(
        s"""
           |select
           |*
           |from
           |ods.ods_outbound_bill
           |where dt = $dt
           |and (date_format(create_time,'yyyyMMdd') >= $startTime
           |or date_format(modify_time,'yyyyMMdd') >= $startTime )
           |and (date_format(create_time,'yyyyMMdd') <=$dt
           |or date_format(modify_time,'yyyyMMdd') <=$dt )
           |and status !=3 and status !=5
           |""".stripMargin).createOrReplaceTempView("outbound_bill")
      spark.sql(
        s"""
           |select
           |*
           |from
           |ods.ods_outbound_bill_detail
           |where dt = $dt
           |and date_format(create_time,'yyyyMMdd') >= $startTime and date_format(create_time,'yyyyMMdd') <=$dt
           |""".stripMargin).createOrReplaceTempView("outbound_bill_detail")
    }
    else if (timeFlag.equals("month")) {
      val startTime = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).toString("yyyyMM")
      dt = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).dayOfMonth().withMinimumValue().toString("yyyyMMdd")
      log.info("===========> 仓库员表-月:"+ dt)
      // 入库
      spark.sql(
        s"""
           |select
           |*
           |from
           |ods.ods_inbound_bill
           |where dt = $dt
           |and (date_format(create_time,'yyyyMMdd')  like '$startTime%'
           |or date_format(modify_time,'yyyyMMdd')  like '$startTime%' )
           |and status !=3 and status !=5
           |""".stripMargin).createOrReplaceTempView("inbound_bill")
      spark.sql(
        s"""
           |select
           |*
           |from
           |ods.ods_inbound_bill_detail
           |where dt = $dt
           |and date_format(create_time,'yyyyMMdd') like '$startTime%'
           |""".stripMargin).createOrReplaceTempView("inbound_bill_detail")
      spark.sql(
        s"""
           |select
           |shop_id,
           |item_name,sku_code,
           |warehouse_code,
           |brand_id,
           |IFNULL(inbound_num - used_num, 0) as inbound_num,
           |IFNULL(inbound_num - used_num, 0) * price as total_money,
           |price
           |from
           |ods.ods_inbound_bill_record where dt like '$startTime%'
           |""".stripMargin).createOrReplaceTempView("inbound_bill_record")
      spark.sqlContext.uncacheTable("inbound_bill_record")
      // 出库
      spark.sql(
        s"""
           |select
           |*
           |from
           |ods.ods_outbound_bill
           |where dt like '$startTime%'
           |and status !=3 and status !=5
           |""".stripMargin).createOrReplaceTempView("outbound_bill")
      spark.sql(
        s"""
           |select
           |*
           |from
           |ods.ods_outbound_bill_detail
           |where dt like '$startTime%'
           |""".stripMargin).createOrReplaceTempView("outbound_bill_detail")
    }
    flag = timeFlag
  }

  override def process(): Unit = {
    //-----------------------------------出入库统计
    //---------- 商品入库信息
    spark.sql(
      s"""
         |
         |select
         |t1.shop_id,
         |t1.warehouse_code,
         |t1.warehouse_name,
         |t2.item_id,
         |t2.sku_id,
         |t2.price,
         |t2.real_inbound_num,
         |$dt as dt
         |from
         |inbound_bill t1
         |left join
         |inbound_bill_detail t2
         |on t1.id = t2.inbound_bill_id
         |""".stripMargin).createOrReplaceTempView("inbound_merge_detail")
    //----------- 商品出库信息
    spark.sql(
      """
        |
        |select
        |t1.shop_id,
        |t1.warehouse_code,
        |t1.warehouse_name,
        |t2.sku_id,
        |t2.price,
        |t2.real_outbound_num
        |from
        |outbound_bill t1
        |left join
        |outbound_bill_detail t2
        |on t1.id = t2.outbound_bill_id
        |""".stripMargin).createOrReplaceTempView("outbound_merge_detail")
    //商品维度 以入库表为主表,算出既有入库又有出库的商品
    val skuInOut = spark.sql(
      s"""
         |with t1 as (
         |select
         |shop_id,
         |sku_id,
         |sku_mapping(sku_id) sku_name,
         |sum(real_inbound_num) as real_inbound_num
         |from
         |inbound_merge_detail
         |group by shop_id,sku_id
         |),
         |t2 as (
         |select
         |shop_id,
         |sku_id,
         |sku_mapping(sku_id) sku_name,
         |sum(real_outbound_num) as real_outbound_num
         |from
         |outbound_merge_detail
         |group by shop_id,sku_id
         |),
         |t3 as (select
         |t1.shop_id,
         |t1.sku_id,
         |t1.sku_name,
         |t1.real_inbound_num,
         |t2.real_outbound_num
         |from
         |t1 left join t2
         |on t1.shop_id = t2.shop_id
         |and t1.sku_id = t2.sku_id
         |),
         |t4 as (select -- 只有出库的
         |t2.shop_id,
         |t2.sku_id,
         |t2.sku_name,
         |t1.real_inbound_num,
         |t2.real_outbound_num
         |from
         |t2 left join t1
         |on t2.shop_id = t1.shop_id
         |and t2.sku_id = t1.sku_id
         |where t1.real_inbound_num is null
         |)
         |select
         |shop_id,
         |case when sku_id is null or sku_id = '' then 0 else sku_id end as business_id,
         |sku_name as business_name,
         |real_inbound_num,
         |real_outbound_num,
         |'1' as business_type, --商品
         |$dt as dt
         |from
         |t3
         |union all
         |select
         |shop_id,
         |case when sku_id is null or sku_id = '' then 0 else sku_id end as business_id,
         |sku_name as business_name,
         |real_inbound_num,
         |real_outbound_num,
         |'1' as business_type, --商品
         |$dt as dt
         |from t4
         |""".stripMargin)
    //仓库信息
    val warehouseInOut = spark.sql(
      s"""
         |with t1 as (
         |select
         |shop_id,
         |warehouse_code,
         |warehouse_name,
         |sum(real_inbound_num) as real_inbound_num
         |from
         |inbound_merge_detail
         |group by shop_id,warehouse_code,warehouse_name
         |),
         |t2 as (
         |select
         |shop_id,
         |warehouse_code,
         |warehouse_name,
         |sum(real_outbound_num) as real_outbound_num
         |from
         |outbound_merge_detail
         |group by shop_id,warehouse_code,warehouse_name
         |),
         |t3 as (select
         |t1.shop_id,
         |t1.warehouse_code,
         |t1.warehouse_name,
         |t1.real_inbound_num,
         |t2.real_outbound_num
         |from
         |t1 left join t2
         |on t1.shop_id = t2.shop_id
         |and t1.warehouse_code = t2.warehouse_code
         |),
         |t4 as (select -- 只有出库的
         |t2.shop_id,
         |t2.warehouse_code,
         |t2.warehouse_name,
         |t1.real_inbound_num,
         |t2.real_outbound_num
         |from
         |t2 left join t1
         |on t2.shop_id = t1.shop_id
         |and t2.warehouse_code = t1.warehouse_code
         |where t1.real_inbound_num is null
         |)
         |select
         |shop_id,
         |case when warehouse_code is null or warehouse_code = '' then 0 else warehouse_code end  as business_id,
         |warehouse_name as business_name,
         |real_inbound_num,
         |real_outbound_num,
         |'2' as business_type, --仓库
         |$dt as dt
         |from
         |t3
         |union all
         |select
         |shop_id,
         |case when warehouse_code is null or warehouse_code = '' then 0 else warehouse_code end as business_id,
         |warehouse_name as business_name,
         |real_inbound_num,
         |real_outbound_num,
         |'2' as business_type, --仓库
         |$dt as dt
         |from t4
         |""".stripMargin)

    val shopWarehouseInoutDF = skuInOut.union(warehouseInOut)
    writerMysql(shopWarehouseInoutDF, "shop_warehouse_inout", flag)
    //-----------------------------------库存成本
    //商品维度
    val skuDf = spark.sql(
      s"""
         |select
         |shop_id,
         |sku_code as business_id,
         |item_name as business_name,
         |SUM(inbound_num)  as inventory,
         |SUM(total_money)  as number_money,
         |'1' as business_type, --商品
         |$dt as dt
         |from
         |inbound_bill_record
         |where shop_id = 2000000027
         |group by
         |shop_id,
         |item_name,sku_code
         |""".stripMargin)
    //仓库维度
    val warehouseDf = spark.sql(
      s"""
         |select
         |shop_id,
         |case when warehouse_code is null or warehouse_code = '' then 0 else warehouse_code end  as business_id,
         |warehouse_mapping(warehouse_code) as business_name,
         |SUM(inbound_num) inventory,
         |SUM(total_money) number_money,
         |'2' as business_type, --商品
         |$dt as dt
         |from
         |inbound_bill_record
         |group by
         |shop_id,
         |warehouse_code
         |""".stripMargin)
    //品牌维度
    val brandDf = spark.sql(
      s"""
         |select
         |shop_id,
         |case when brand_id is null or brand_id = '' then 0 else brand_id end as business_id,
         |brand_three_mapping(brand_id) as business_name,
         |SUM(inbound_num) inventory,
         |SUM(total_money) number_money,
         |'3' as business_type, --商品
         |$dt as dt
         |from
         |inbound_bill_record
         |group by
         |shop_id,
         |brand_id
         |""".stripMargin)
    val shopWarehouseInfoDF = skuDf.union(warehouseDf).union(brandDf)
    writerMysql(shopWarehouseInfoDF, "shop_warehouse_info", flag)
  }
}
