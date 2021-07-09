package udf

import ip.IPSeeker
import org.apache.spark.sql.SparkSession
import utils.{BroadcastUtils, TypeUtils}
import java.io.File

/**
  * @Auther: ljh
  * @Date: 2021/3/29 22:55
  * @Description:
  */
object UDFRegister{

  def skuMapping(spark:SparkSession,dt:String): Unit ={
    val getSkuName = BroadcastUtils.getItemNameMap(spark, dt)
    spark.udf.register("sku_mapping", func = (skuId: Long) => {
      getSkuName.value.getOrElse(skuId,"")
    })
  }
  //映射 industry 名称
  def industryMapping(spark:SparkSession,dt:String): Unit ={
    val getSkuName = BroadcastUtils.getItemNameMap(spark, dt)
    spark.udf.register("industry_mapping", func = (skuId: Long) => {
      getSkuName.value.getOrElse(skuId,"")
    })
  }

  def shopMapping(spark:SparkSession,dt:String): Unit ={
    val getSkuName = BroadcastUtils.getShopNameMap(spark, dt)
    spark.udf.register("shop_mapping", func = (skuId: Long) => {
      getSkuName.value.getOrElse(skuId,"")
    })
  }

  def shopTypeMapping(spark:SparkSession,dt:String): Unit ={
    val getSkuName = BroadcastUtils.getShopTypeMap(spark, dt)
    spark.udf.register("shop_type_mapping", func = (skuId: Long) => {
      getSkuName.value.getOrElse(skuId,"")
    })
  }

  def ordersStatusMapping(spark:SparkSession): Unit ={
    val map = TypeUtils.map
    spark.udf.register("orders_status_mapping", func = (skuId: Long) => {
      map.getOrElse(skuId.toString,"")
    })
  }

  def clientMapping(spark:SparkSession,dt:String): Unit ={
    val userMap = BroadcastUtils.getUserMap(spark, dt)
    spark.udf.register("user_mapping", func = (userId: Long) => {
      userMap.value.getOrElse(userId,"")
    })
  }

  def cidThreeMapping(spark:SparkSession,dt:String): Unit ={
    val cidMap = BroadcastUtils.getCidThreeMap(spark, dt)
    spark.udf.register("cid_three_mapping", func = (userId: String) => {
      cidMap.value.getOrElse(userId,"")
    })
  }

  def brandThreeMapping(spark:SparkSession,dt:String): Unit ={
    val brandMap = BroadcastUtils.getBrandThreeMap(spark, dt)
    spark.udf.register("brand_three_mapping", func = (userId: Long) => {
      brandMap.value.getOrElse(userId,"")
    })
  }
  def WarehouseThreeMapping(spark:SparkSession,dt:String): Unit ={
    val brandMap = BroadcastUtils.getWarehouseNameMap(spark, dt)
    spark.udf.register("warehouse_mapping", func = (code: String) => {
      brandMap.value.getOrElse(code,"")
    })
  }

  def SkuPictureMapping(spark:SparkSession): Unit ={
    val brandMap = BroadcastUtils.getMsqlSqlSkuPictureMap(spark)
    spark.udf.register("sku_picture_mapping", func = (code: Long) => {
      val str = brandMap.value.getOrElse(code, "")
      str
    })
  }

  def SkuPriceMap(spark:SparkSession): Unit ={
    val brandMap = BroadcastUtils.getMsqlSqlSkuPriceMap(spark)
    spark.udf.register("sku_price_mapping", func = (code: Long) => {
      val str = brandMap.value.get(code)
      str
    })
  }


  def FileIpMapping(spark:SparkSession): Unit ={
    spark.udf.register("ip_mapping", func = (ip: String) => {
      val file = new File("/export/servers/bju/bin/ipMapping.dat")
      val ipSeeker: IPSeeker = new IPSeeker(file)
      val country = ipSeeker.getCountry("192.168.0.139")
      var province = ""
      var city = ""
      //如河南省郑州市
      var areaArray: Array[String] = country.split("省")
      if (areaArray.size > 1) {
        //表示非直辖市
        province = areaArray(0) + "省"
        city = areaArray(1)
      } else {
        //表示直辖市
        //如北京市海淀区
        areaArray = country.split("市")
        if (areaArray.length > 1) {
          province = areaArray(0) + "市"
          city = areaArray(1)
        } else {
          province = areaArray(0)
          city = ""
        }
      }
      province+","+city
    })
  }
}
