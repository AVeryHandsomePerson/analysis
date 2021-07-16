import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

/**
 * @author ljh
 * @version 1.0
 */
object demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("spark_ck")
      .master("local[*]")
      .getOrCreate()
//    val ck_params_Map = Map[String,String](
//      "driver"->"ru.yandex.clickhouse.ClickHouseDriver",
//      "url"-> "jdbc:clickhouse://10.30.0.240:8123/off_line_dws",
//      "dbtable"-> "myOperator",
//      "password"-> "6lYaUiFi"
//    )
//
//
//    spark.read.format("jdbc").options(ck_params_Map).load().show(false)
//    spark.stop()


    //生成df
    val external_info = spark.createDataFrame(Seq(
      (2000000259,1000000441,2000000281,"火影忍者手办GK须佐能乎宇智波鼬佐","null","null","TC",1,0,0.0000,0.00,0,93.00,122.00,20210712)
    )).toDF("shop_id","buyer_id","sku_id","item_name","pick_id","pick_name","order_type","sale_order_number","sale_pick_order_number","payment_num","payment_succeed_money","payment_succeed_number","income_money","payment_money","dt")
    //写入参数
    val write_maps = Map[String,String](
      "batchsize"->"2000",
      "isolationLevel"->"NONE",
      "numPartitions"->"1"
    )
    val url = "jdbc:clickhouse://10.30.0.240:8123/off_line_dws"
    val dbtable = "dws_shop_deal_info"
    val pro = new Properties()
    pro.put("driver","ru.yandex.clickhouse.ClickHouseDriver")
    pro.put("password","6lYaUiFi")

    //jdbc方式写入
    external_info.write.mode(SaveMode.Append)
      .options(write_maps)
      .jdbc(url,dbtable,pro)
    spark.stop()




  }
}
