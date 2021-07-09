package com.cn.bju.realtime.etl.process

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.cn.bju.common.bean.CanalRowData
import com.cn.bju.realtime.etl.`trait`.MysqlBaseETL
import com.cn.bju.realtime.etl.bean.{OrderDBEntity, OrderUser}
import com.cn.bju.realtime.etl.util.RedisUtil
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.DateUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.shaded.org.joda.time.DateTime
import redis.clients.jedis.Jedis

/**
 * @author ljh
 * @version 1.0
 */
class OrderDataETL(env: StreamExecutionEnvironment) extends MysqlBaseETL(env) {
  /**
   * 根据业务抽取出来process方法，因为所有的ETL都有操作方法
   */
  override def process(): Unit = {
    //1：从kafka中消费出来订单数据，过滤出来订单表的数据
    val orderDataStream: DataStream[CanalRowData] = getKafkaDataStream()
      .filter(x => (x.getTableName == "orders"))
    // 过滤需要的数据
    val orderDBFilterDataStream = orderDataStream.filter(rowData =>
      StringUtils.isNotEmpty(rowData.getColumns.get("create_time"))
        &&
        !rowData.getColumns.get("parent_order_id").equals("0")
    )
    //2：自定义map 判断是否支付，如果支付将用户ID存到redis中做缓存   将RowData转换成OrderDBEntity对象
    val orderDBDataStream: DataStream[OrderDBEntity] = orderDBFilterDataStream.map(new RichMapFunction[CanalRowData, OrderDBEntity] {
      //定义redis的对象
      var jedis: Jedis = _

      override def map(rowData: CanalRowData): OrderDBEntity = {
        if (!jedis.isConnected) {
          jedis = RedisUtil.getJedis()
          jedis.select(1)
        }
        val startTime = new DateTime(DateUtils.parseDate(rowData.getColumns.get("create_time"), "yyyy-MM-dd HH:mm:ss"))
        val userRedisKey = rowData.getColumns.get("shop_id") + "_" + rowData.getColumns.get("buyer_id")
        val orderUser = OrderUser(userRedisKey, startTime.toString("yyyy-MM-dd HH:mm:ss"))
        val userJson = JSON.toJSONString(orderUser, SerializerFeature.DisableCircularReferenceDetect)
        var long = 0L
        // 判断是否是支付
        if (rowData.getColumns.get("paid").toLong == 2) {
          /**
           * 判断是否存在用户
           * 如果存在，判断储存时间跟当前时间相差多久 如果是一年内则为老用户，如果是一年后则为新用户
           */
          // 设置成功 返回1  覆盖返回0
          val userBoolean = jedis.hexists("tradecenter:dim_orders_user", userRedisKey)
          if (userBoolean) {
            val str = jedis.hget("tradecenter:dim_orders_user", userRedisKey)
            val userLastJson = JSON.parseObject(str, classOf[OrderUser])
            val lastDataTime = new DateTime(DateUtils.parseDate(userLastJson.getCreateTime, "yyyy-MM-dd HH:mm:ss"))
            val newDataTime = new DateTime(DateUtils.parseDate(orderUser.getCreateTime, "yyyy-MM-dd HH:mm:ss"))
            if (((newDataTime.getMillis / 1000 - lastDataTime.getMillis / 1000) / 3600) / 24 > 365) {
              long = jedis.hset("tradecenter:dim_orders_user", userRedisKey, userJson)
            } else {
              jedis.hset("tradecenter:dim_orders_user", userRedisKey, userJson)
            }
          } else {
            long = jedis.hset("tradecenter:dim_orders_user", userRedisKey, userJson)
          }
        }
        OrderDBEntity(rowData, rowData.getEventType, long)
      }

      override def open(parameters: Configuration): Unit = {
        //获取redis的连接
        jedis = RedisUtil.getJedis()
        //指定维度数据所在的数据库的索引
        jedis.select(1)
      }

      override def close(): Unit = {
        if (jedis.isConnected) {
          jedis.close()
        }
      }
    })
    //3：将OrderDBEntity对象转换成Json字符串
    val orderDBJsonDataStream: DataStream[String] = orderDBDataStream.map(orderDBEntity => {
      //将样例类转换成json字符串
      JSON.toJSONString(orderDBEntity, SerializerFeature.DisableCircularReferenceDetect)
    })
    //打印测试
    orderDBJsonDataStream.print("订单数据>>>")
    //4：将转换后的json字符串写入到kafka集群
    orderDBJsonDataStream.addSink(kafkaProducer("dwd_order"))
  }
}
