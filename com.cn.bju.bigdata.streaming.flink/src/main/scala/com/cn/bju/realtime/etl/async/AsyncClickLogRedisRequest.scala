package com.cn.bju.realtime.etl.async

import com.alibaba.fastjson.JSON
import com.cn.bju.common.bean.CanalRowData
import com.cn.bju.realtime.etl.bean.{ClickLogEntity, DimGoodsDBEntity}
import com.cn.bju.realtime.etl.util.RedisUtil
import ip.IPSeeker
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.DateUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.table.shaded.org.joda.time.DateTime
import redis.clients.jedis.Jedis

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 *
 * @author ljh
 * @version 1.0
 * 异步查询埋点数据对商品名称进行映射
 * 根据商品ID 需要跟商品表关联，所以需要redis，从而打开关闭数据源，使用RichAsyncFunction
 * 使用异步IO的目的是为了提高吞吐量
 */
class AsyncClickLogRedisRequest extends RichAsyncFunction[String, ClickLogEntity]{
  //定义redis的对象
  var jedis:Jedis = _
  //初始化数据源，打开连接
  override def open(parameters: Configuration): Unit = {
    //获取redis的连接
    jedis = RedisUtil.getJedis()
    //指定维度数据所在的数据库的索引
    jedis.select(1)
  }

  //释放资源，关闭连接
  override def close(): Unit = {
    if(jedis.isConnected){
      jedis.close()
    }
  }
  /**
   * 连接redis超时的操作，默认会抛出异常，一旦重写了该方法，则执行方法的逻辑
   * @param input
   * @param resultFuture
   */
  override def timeout(input: String, resultFuture: ResultFuture[ClickLogEntity]): Unit = {
    println("订单明细实时拉宽操作的时候，与维度数据进行关联操作超时了")
  }
  /**
   * 定义异步回调的上下文对象
   */
  implicit lazy val executor = ExecutionContext.fromExecutor(Executors.directExecutor())
  //异步操作，对数据流中的每一条数据进行处理，但是这个是一个异步的操作
  override def asyncInvoke(rowData: String, resultFuture: ResultFuture[ClickLogEntity]): Unit = {
    //发起异步请求，获取结束的Fature
    Future {
      if(!jedis.isConnected){
        jedis = RedisUtil.getJedis()
        jedis.select(1)
      }
      val jsonData = JSON.parseObject(rowData)
      val inTime = new DateTime(DateUtils.parseDate(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(jsonData.getLong("timeIn"))), "yyyy-MM-dd HH:mm:ss"))
      var loginToken = ""
      if(Try{jsonData.getString("loginToken")}.isSuccess && StringUtils.isNotEmpty(jsonData.getString("loginToken"))){
        loginToken = jsonData.getString("loginToken")
      }
      var skuId = ""
      var itemId = ""
      var itemName = ""
      if(!jsonData.getString("event").contains("_home")){
        skuId = jsonData.getString("skuId")
        itemId = jsonData.getString("itemId")
        //1：根据商品id获取商品的详细信息（goodscenter:dim_item）
        val goodsJson: String = jedis.hget("goodscenter:dim_item", itemId)
       //将商品的json字符串解析成商品的样例类
       val dimGoods: DimGoodsDBEntity = DimGoodsDBEntity(goodsJson)
        itemName = dimGoods.getItemName
      }
      //构建埋点维度数据，返回
      val clickGoodsWideEntity = ClickLogEntity(
        inTime.toString("yyyy-MM-dd HH:mm:ss"),
        jsonData.getString("domain"),
        jsonData.getString("url"),
        jsonData.getString("title"),
        jsonData.getString("referrer"),
        jsonData.getLong("sh"),
        jsonData.getLong("sw"),
        jsonData.getLong("cd"),
        jsonData.getString("lang"),
        jsonData.getString("shopId"),
        jsonData.getString("ip"),
        loginToken,
        skuId,
        itemId,
        itemName,
        jsonData.getString("event"),
        jsonData.getString("userId"),
        "",
        "",
        inTime.toString("HH"),
        inTime.toString("yyyy-MM-dd")
      )
      //异步请求回调
      resultFuture.complete(Array(clickGoodsWideEntity))
    }
   }
}
