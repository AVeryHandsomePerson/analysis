package app

import dwd.{DwdETL, DwsETL}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import shop.{ClickLogAnalysis, ClientAnalysis, CommonAnalysis, DealAnlaysis, GoodsAnalysis, OneGoodsAnalysis, WarehouseAnalysis}

/**
 * @author ljh
 * @version 1.0
 */
object OffLineApp {

  val log = Logger.getLogger(App.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("OffLineAppAnlysis")
      .config("hive.exec.dynamici.partition", true)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    val dt = args(0)
    val flag = args(1);

    flag match {
      case "all" =>
        val dwsETL = new DwsETL(spark, dt);
        dwsETL.process()
      case "dwd" =>
        val dwdETL = new DwdETL(spark, dt);
        dwdETL.process()
      case "dws" =>
        val dwsETL = new DwsETL(spark, dt);
        dwsETL.process()
      case _ =>
    }
    log.info("===========> 执行结束:" + dt)

  }

}
