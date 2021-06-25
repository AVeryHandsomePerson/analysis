package com.cn.bju.realtime.etl.bean

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.beans.BeanProperty

/**
 * @author ljh
 * @version 1.0
 * 商品维度样例类
 *
 * @param item_id 商品id
 * @param brand_id 品牌ID
 * @param cid 类目ID
 * @param seller_id 商家ID
 * @param shop_id 店铺ID
 * @param item_name 商品名称
 * @BeanProperty：会生成seter方法和geter方法
 */
case class DimGoodsDBEntity(@BeanProperty itemId:Long = 0,		// 商品id
                            @BeanProperty brandId:Long = 0,	// 商品名称
                            @BeanProperty cid:Long = 0,			// 店铺id
                            @BeanProperty sellerId:Long = 0,   // 商品分类id
                            @BeanProperty shopId:Long = 0,   // 商品分类id
                            @BeanProperty itemName:String = "") // 商品价格

/**
 * 商品的伴生对象
 */
object DimGoodsDBEntity{
  def apply(json:String): DimGoodsDBEntity = {
    //正常的话，订单明细表的商品id会存在与商品表中，假如商品id不存在商品表，这里解析的时候就会抛出异常
    if(json != null){
      val jsonObject: JSONObject = JSON.parseObject(json)
      new DimGoodsDBEntity(
        jsonObject.getLong("itemId"),
        jsonObject.getLong("brandId"),
        jsonObject.getLong("cid"),
        jsonObject.getLong("sellerId"),
        jsonObject.getLong("shopId"),
        jsonObject.getString("itemName"))
    }else{
      new DimGoodsDBEntity
    }
  }
}
