package com.cn.bju.realtime.etl.dataloader

import java.sql.{Connection, DriverManager, Statement}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.cn.bju.realtime.etl.bean.DimGoodsDBEntity
import com.cn.bju.realtime.etl.util.{GlobalConfigUtil, RedisUtil}
import redis.clients.jedis.Jedis

/**
 * 维度表数据的全量装载实现类
 * 1）商品维度表
 */
object DimentsionDataLoader {
  def main(args: Array[String]): Unit = {
    //1：注册mysql的驱动
    Class.forName("com.mysql.jdbc.Driver")

    //2：创建连接
    val connection: Connection = DriverManager.getConnection(s"jdbc:mysql://${GlobalConfigUtil.`mysql.server.ip`}:${GlobalConfigUtil.`mysql.server.port`}/${GlobalConfigUtil.`mysql.server.database`}",
      GlobalConfigUtil.`mysql.server.username`,
      GlobalConfigUtil.`mysql.server.password`
    )
    //3：创建redis的连接
    val jedis: Jedis = RedisUtil.getJedis()
    jedis.select(1)
    //4：加载维度表的数据到redis中
    //1）商品维度表
    LoadDimGoods(connection, jedis)

    System.exit(0)
  }

  /**
   * 1）商品维度表
   * @param connection
   * @param jedis
   */
  def LoadDimGoods(connection: Connection, jedis: Jedis)={
    //定义sql查询语句
    val sql =
      """
        |SELECT
        |	 item_id,
        |  brand_id,
        |  cid,
        |  seller_id,
        |  shop_id,
        |  item_name
        |FROM
        | item
        |""".stripMargin
    //创建statement
    val statement: Statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)

    //遍历商品表的数据
    while(resultSet.next()){
      val itemId = resultSet.getLong("item_id")
      val brandId = resultSet.getLong("brand_id")
      val cid = resultSet.getLong("cid")
      val sellerId = resultSet.getLong("seller_id")
      val shopId = resultSet.getLong("shop_id")
      val itemName = resultSet.getString("item_name")
      //需要将获取到的商品维度表数据写入到redis中
      //redis是一个k/v数据库，需要需要将以上五个字段封装成json结构保存到redis中
      val goodsDBEntity: DimGoodsDBEntity = DimGoodsDBEntity(itemId, brandId, cid, sellerId, shopId, itemName)
      //将样例类转换成json字符串写入到redis中
      /**
       * ambiguous reference to overloaded definition,
       * both method toJSONString in object JSON of type (x$1: Any, x$2: com.alibaba.fastjson.serializer.SerializerFeature*)String
       * and  method toJSONString in object JSON of type (x$1: Any)String
       * match argument types (com.cn.bju.realtime.etl.bean.DimGoodsDBEntity) and expected result type Any
       * println(JSON.toJSONString(goodsDBEntity))
       */
      val json: String = JSON.toJSONString(goodsDBEntity, SerializerFeature.DisableCircularReferenceDetect)
//      println(json)
      jedis.hset("goodscenter:dim_item", itemId.toString, json)
    }
    resultSet.close()
    statement.close()
  }
}
