package com.cn.bju.realtime.etl.async

import com.cn.bju.common.bean.CanalRowData
import com.cn.bju.realtime.etl.bean.{DimGoodsDBEntity, OrderGoodsWideEntity}
import com.cn.bju.realtime.etl.util.RedisUtil
import org.apache.commons.lang3.time.DateUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.table.shaded.org.joda.time.DateTime
import redis.clients.jedis.Jedis

import scala.concurrent.{ExecutionContext, Future}

/**
 *
 * @author ljh
 * @version 1.0
 *          异步查询埋点数据对商品名称进行映射
 *          根据商品ID 需要跟商品表关联，所以需要redis，从而打开关闭数据源，使用RichAsyncFunction
 *          使用异步IO的目的是为了提高吞吐量
 */
class AsyncOrderRedisRequest extends RichAsyncFunction[CanalRowData, OrderGoodsWideEntity] {
  //定义redis的对象
  var jedis: Jedis = _

  //初始化数据源，打开连接
  override def open(parameters: Configuration): Unit = {
    //获取redis的连接
    jedis = RedisUtil.getJedis()
    //指定维度数据所在的数据库的索引
    jedis.select(1)
  }

  //释放资源，关闭连接
  override def close(): Unit = {
    if (jedis.isConnected) {
      jedis.close()
    }
  }

  /**
   * 连接redis超时的操作，默认会抛出异常，一旦重写了该方法，则执行方法的逻辑
   *
   * @param input
   * @param resultFuture
   */
  override def timeout(input: CanalRowData, resultFuture: ResultFuture[OrderGoodsWideEntity]): Unit = {
    println("订单明细实时拉宽操作的时候，与维度数据进行关联操作超时了")
  }

  /**
   * 定义异步回调的上下文对象
   */
  implicit lazy val executor = ExecutionContext.fromExecutor(Executors.directExecutor())

  //异步操作，对数据流中的每一条数据进行处理，但是这个是一个异步的操作
  override def asyncInvoke(rowData: CanalRowData, resultFuture: ResultFuture[OrderGoodsWideEntity]): Unit = {
    //发起异步请求，获取结束的Fature
    Future {
      if (!jedis.isConnected) {
        jedis = RedisUtil.getJedis()
        jedis.select(1)
      }
      //1：根据商品id获取商品的详细信息（itcast_shop:dim_goods）
      val goodsJson: String = jedis.hget("goodscenter:dim_item", rowData.getColumns.get("item_id"))
      //将商品的json字符串解析成商品的样例类
      val dimGoods: DimGoodsDBEntity = DimGoodsDBEntity(goodsJson)
      // 时间解析
      val startTime = new DateTime(DateUtils.parseDate(rowData.getColumns.get("create_time"), "yyyy-MM-dd HH:mm:ss"))
      val orderGoodsWideEntity = OrderGoodsWideEntity(
        rowData.getColumns.get("id").toLong,
        rowData.getColumns.get("order_id").toLong,
        rowData.getColumns.get("item_id").toLong,
        rowData.getColumns.get("item_name"),
        rowData.getColumns.get("cid").toLong,
        rowData.getColumns.get("brand_id").toLong,
        rowData.getColumns.get("sku_id").toLong,
        rowData.getColumns.get("num").toDouble,
        rowData.getColumns.get("cost_price").toDouble,
        rowData.getColumns.get("item_original_price").toDouble,
        rowData.getColumns.get("payment_total_money").toDouble,
        rowData.getColumns.get("payment_price").toDouble,
        rowData.getColumns.get("cut_price"),
        rowData.getColumns.get("cut_price_total"),
        rowData.getColumns.get("refund"),
        rowData.getColumns.get("purchase_num"),
        rowData.getColumns.get("divided_balance"),
        rowData.getColumns.get("warehouse_code"),
        rowData.getColumns.get("dis_commission_rate"),
        rowData.getColumns.get("dis_commission_amount"),
        dimGoods.getShopId,
        startTime.toString("HH"),
        startTime.toString("yyyy-MM-dd")
      )
      //异步请求回调
      resultFuture.complete(Array(orderGoodsWideEntity))
    }
  }
}
