package app

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import shop.{ClickLogAnalysis, ClientAnalysis, CommonAnalysis, DealAnlaysis, GoodsAnalysis, OneGoodsAnalysis, WarehouseAnalysis}

/**
 * @author ljh
 * @version 1.0
 */
object App {

  val log = Logger.getLogger(App.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Anlysis")
      .config("hive.exec.dynamici.partition", true)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    val dt = args(0)
    val flag = args(1);
    val timeFlag = args(2);

    flag match {
      case "all" =>
        log.info("===========> 开始执行 公共模块")
        val commonAnalysis = new CommonAnalysis(spark, dt, timeFlag)
        commonAnalysis.process()
        log.info("===========> 开始执行 商品分析模块")
        val goodsAnalysis = new GoodsAnalysis(spark, dt, timeFlag)
        goodsAnalysis.process()
        log.info("===========> 开始执行 客户分析模块")
        val clientAnalysis: ClientAnalysis = new ClientAnalysis(spark, dt, timeFlag)
        clientAnalysis.process()
        log.info("===========> 开始执行 交易分析模块")
        val dealAnlaysis = new DealAnlaysis(spark, dt, timeFlag)
        dealAnlaysis.process()
        log.info("===========> 开始执行 单品分析模块")
        val oneGoodsAnalysis: OneGoodsAnalysis = new OneGoodsAnalysis(spark, dt, timeFlag)
        oneGoodsAnalysis.process()
        log.info("===========> 开始执行 仓库分析模块")
        val warehouseAnalysis: WarehouseAnalysis = new WarehouseAnalysis(spark, dt, timeFlag)
        warehouseAnalysis.process()
        log.info("===========> 开始执行 流量分析模块")
        val clickLogAnalysis: ClickLogAnalysis = new ClickLogAnalysis(spark, dt, timeFlag)
        clickLogAnalysis.process()

      case "goods" =>
        log.info("===========> 开始执行 商品分析模块")
        val goodsAnalysis = new GoodsAnalysis(spark, dt, timeFlag)
        goodsAnalysis.process()
      case "client" =>
        log.info("===========> 开始执行 客户分析模块")
        val clientAnalysis: ClientAnalysis = new ClientAnalysis(spark, dt, timeFlag)
        clientAnalysis.process()
      case "deal" =>
        log.info("===========> 开始执行 交易分析模块")
        val dealAnlaysis = new DealAnlaysis(spark, dt, timeFlag)
        dealAnlaysis.process()
      case "ware" =>
        log.info("===========> 开始执行 仓库分析模块")
        val warehouseAnalysis: WarehouseAnalysis = new WarehouseAnalysis(spark, dt, timeFlag)
        warehouseAnalysis.process()
      case "one_goods" =>
        log.info("===========> 开始执行 单品分析模块")
        val oneGoodsAnalysis: OneGoodsAnalysis = new OneGoodsAnalysis(spark, dt, timeFlag)
        oneGoodsAnalysis.process()
      case "click_log" =>
        log.info("===========> 开始执行 流量分析模块")
        val clickLogAnalysis: ClickLogAnalysis = new ClickLogAnalysis(spark, dt, timeFlag)
        clickLogAnalysis.process()
      case "comm" =>
        log.info("===========> 开始执行 公共模块")
        val commonAnalysis = new CommonAnalysis(spark, dt, timeFlag)
        commonAnalysis.process()
      case _ =>
    }
    log.info("===========> 执行结束:" + dt)

  }

}
