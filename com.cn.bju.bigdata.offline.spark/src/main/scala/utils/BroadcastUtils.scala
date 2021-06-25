package utils

import common.{ReadStarvConfig, StarvConfig}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/**
 * @author ljh
 * @date 2021/3/25 17:04
 * @version 1.0
 */
object BroadcastUtils {
  def getUserMap(spark: SparkSession, dt: String): Broadcast[Map[Long, String]] = {
    import spark.implicits._
    val userMap = spark.sql(
      s"""
         |select
         |id,
         |name
         |from
         |ods.ods_user
         |where dt = $dt
         |""".stripMargin)
      .map(row => (row.getLong(0), row.getString(1)))
      .collect()
      .toMap
    spark.sparkContext.broadcast(userMap)
  }

  def getIndustryNameMap(spark: SparkSession, dt: String): Broadcast[Map[String, String]] = {
    import spark.implicits._
    val industryMap = spark.sql(
      s"""
         |select
         |industry,
         |industry_name
         |from
         |ods.ods_plat_industry_rel
         |where dt = $dt
         |""".stripMargin)
      .map(row => (row.getString(0), row.getString(1)))
      .collect()
      .toMap
    spark.sparkContext.broadcast(industryMap)
  }

  def getItemNameMap(spark: SparkSession, dt: String): Broadcast[Map[Long, String]] = {
    import spark.implicits._
    val skuMap = spark.sql(
      s"""
         |select
         |sku_id,
         |item_name
         |from
         |dwd.dwd_sku_name
         |where dt = $dt
         |""".stripMargin)
      .map(row => (row.getLong(0), row.getString(1)))
      .collect()
    val allSkuMap = spark.sql(
      s"""
         |select
         |id,
         |item_name
         |from
         |ods.ods_item_master
         |where dt = $dt
         |""".stripMargin)
      .map(row => (row.getLong(0), row.getString(1)))
      .collect().union(skuMap)
      .toMap
    spark.sparkContext.broadcast(allSkuMap)
  }

  def getShopNameMap(spark: SparkSession, dt: String): Broadcast[Map[Long, String]] = {
    import spark.implicits._
    val industryMap = spark.sql(
      s"""
         |select
         |shop_id,
         |shop_name
         |from
         |ods.ods_shop_info
         |where dt = $dt
         |""".stripMargin)
      .map(row => (row.getLong(0), row.getString(1)))
      .collect()
      .toMap
    spark.sparkContext.broadcast(industryMap)
  }

  def getShopTypeMap(spark: SparkSession, dt: String): Broadcast[Map[Long, String]] = {
    import spark.implicits._
    val industryMap = spark.sql(
      s"""
         |select
         |code,
         |name
         |from
         |ods.ods_base_dictionary
         |where dt = $dt
         |""".stripMargin)
      .map(row => (row.getLong(0), row.getString(1)))
      .collect()
      .toMap
    spark.sparkContext.broadcast(industryMap)
  }

  def getCidThreeMap(spark: SparkSession, dt: String): Broadcast[Map[String, String]] = {
    import spark.implicits._
    val industryMap = spark.sql(
      s"""
         |select
         |cat_3d_id,
         |cat_3d_name
         |from
         |dwd.dim_goods_cat
         |where dt = $dt
         |""".stripMargin)
      .map(row => (row.getString(0), row.getString(1)))
      .collect()
      .toMap
    spark.sparkContext.broadcast(industryMap)
  }

  def getBrandThreeMap(spark: SparkSession, dt: String): Broadcast[Map[Long, String]] = {
    import spark.implicits._
    val industryMap = spark.sql(
      s"""
         |select
         |brand_id,
         |brand_name
         |from
         |ods.ods_item_brand
         |where dt = $dt
         |""".stripMargin)
      .map(row => (row.getLong(0), row.getString(1)))
      .collect()
      .toMap
    spark.sparkContext.broadcast(industryMap)
  }

  def getWarehouseNameMap(spark: SparkSession, dt: String): Broadcast[Map[String, String]] = {
    import spark.implicits._
    val industryMap = spark.sql(
      s"""
         |select
         |code,name
         |from
         |ods.ods_warehouse
         |group by code,name
         |""".stripMargin)
      .map(row => (row.getString(0), row.getString(1)))
      .collect()
      .toMap
    spark.sparkContext.broadcast(industryMap)
  }

  def getMsqlSqlSkuPictureMap(spark: SparkSession): Broadcast[Map[Long, String]] = {
    val config = new ReadStarvConfig("read-dev")
    val dataFrame = spark.read.format("jdbc")
      .options(config.getMyJDBCConfig("item_sku_picture"))
      .load()
      .select("sku_id", "picture_url")
    val dataMap = dataFrame
      .rdd
      .map(row => (row.getLong(0), row.getString(1)))
      .collect()
      .toMap
    spark.sparkContext.broadcast(dataMap)
  }

  def getMsqlSqlSkuPriceMap(spark: SparkSession): Broadcast[Map[Long, java.math.BigDecimal]] = {
    val config = new ReadStarvConfig("read-dev")
    val dataFrame = spark.read.format("jdbc")
      .options(config.getMyJDBCConfig("item_sku_price"))
      .load()
      .select("sku_id", "sell_price")
    val dataMap = dataFrame
      .rdd
      .map(row => (row.getLong(0), row.getDecimal(1)))
      .collect()
      .toMap
    spark.sparkContext.broadcast(dataMap)
  }

}
