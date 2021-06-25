package com.cn.bju.realtime.etl.bean

import scala.beans.BeanProperty

// 订单明细拉宽数据
case class OrderGoodsWideEntity(@BeanProperty id:Long, //订单明细id
                                @BeanProperty orderId:Long, //订单id
                                @BeanProperty itemId:Long, //商品id
                                @BeanProperty itemName:String, //商品名称 item_name
                                @BeanProperty cid:Long, //类目id
                                @BeanProperty brandId:Long, //品牌id
                                @BeanProperty skuId:Long, //sku_id
                                @BeanProperty num:Double, //商品数量 num
                                @BeanProperty costPrice:Double, //成本价 cost_price
                                @BeanProperty itemOriginal_price:Double, //商品价格 商品的原始价格
                                @BeanProperty paymentTotalMoney:Double, //商品价格 支付的总价格
                                @BeanProperty paymentPrice:Double, //支付的单价（不含运费）
                                @BeanProperty cutPrice:String, //分切单价
                                @BeanProperty cutPriceTotal:String, //分切总价
                                @BeanProperty refund:String, //退款标记
                                @BeanProperty purchaseNum:String, //采购量
                                @BeanProperty dividedBalance:String, //分摊的余额金额
                                @BeanProperty warehouseCode:String, //仓库编码
                                @BeanProperty disCommissionRate:String, //分销佣金费率
                                @BeanProperty disCommissionAmount:String, //分销佣金金额
                                @BeanProperty shopId:Long, //拓宽后的字段-》商品表：店铺id
                                @BeanProperty hour: String, //小时
                                @BeanProperty day: String //天
                                )