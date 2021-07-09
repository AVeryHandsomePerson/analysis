package shop

import app.App
import common.StarvConfig
import org.apache.commons.lang3.time.DateUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}
import org.joda.time.DateTime
import udf.UDFRegister

/**
 * @author ljh
 * @version 1.0
 */
class OneGoodsAnalysis(spark: SparkSession,var dt: String, timeFlag: String) extends WriteBase {

  val log = Logger.getLogger(App.getClass)
  var flag = "";
  {
    val startTime = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).minusWeeks(1).toString("yyyyMMdd")
    log.info("===========> 单品分析模块-开始注册UDF函数:")
    UDFRegister.skuMapping(spark, dt)
    UDFRegister.SkuPictureMapping(spark)
    UDFRegister.cidThreeMapping(spark, dt)
    UDFRegister.FileIpMapping(spark)
    //    UDFRegister.FileIpMapping(spark)
    if (timeFlag.equals("day")) {
      log.info("===========> 单品分析模块-天:" + dt)
      //零售
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_orders_detail
           |where dt=$dt and po_type is null
           |""".stripMargin).createOrReplaceTempView("orders_retail")
      // 埋点
      spark.sql(
        s"""
          |select
          |*
          |from
          |dwd.dwd_click_log
          |where dt = $dt
          |""".stripMargin).createOrReplaceTempView("dwd_click_log")
      //     spark.read.json(s"hdfs://bogon:8020/click_log/${dt}/").createOrReplaceTempView("click_log")
    }
    else if (timeFlag.equals("week")) {
      log.info("===========> 单品分析模块-周:" + startTime + "and" + dt)
      //零售
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_orders_detail
           |where dt>= $startTime and dt<=$dt and po_type is null
           |""".stripMargin).createOrReplaceTempView("orders_retail")
      // 埋点
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_click_log
           |where dt>= $startTime and dt<=$dt
           |""".stripMargin).createOrReplaceTempView("dwd_click_log")
    }
    else if (timeFlag.equals("month")) {
      val startTime = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).toString("yyyyMM")
      dt = new DateTime(DateUtils.parseDate(dt, "yyyyMMdd")).dayOfMonth().withMinimumValue().toString("yyyyMMdd")
      log.info("===========> 单品分析模块-月:"+ dt)
      //零售
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_dim_orders_detail
           |where dt like '$startTime%'  and po_type is null
           |""".stripMargin).createOrReplaceTempView("orders_retail")
      // 埋点
      spark.sql(
        s"""
           |select
           |*
           |from
           |dwd.dwd_click_log
           |where dt like '$startTime%'
           |""".stripMargin).createOrReplaceTempView("dwd_click_log")
    }
    flag = timeFlag
  }

  override def process(): Unit = {
    // 商品表 信息
    spark.sql(
      s"""
         |select
         |shop_id,
         |shelve_time,
         |item_id,
         |item_name,
         |'' as picture_url,
         |cid_three_mapping(cid)  as cid_name,
         |row_number() over(partition by shop_id order by shelve_time desc) as profit_top
         |from
         |dwd.fact_item
         |where end_zipper_time = '9999-12-31'
      """.stripMargin).createOrReplaceTempView("item")
    spark.sql(
      s"""
         |select
         |shop_id,
         |create_time as shelve_time,
         |id as item_id,
         |item_name,
         |pic_url as picture_url,
         |cid_three_mapping(cid)  as cid_name,
         |row_number() over(partition by shop_id order by create_time desc) as profit_top
         |from
         |ods.ods_item_master
         |where dt=$dt
         |""".stripMargin).createOrReplaceTempView("item_master")
    spark.sql(
      """
        |select
        |*
        |from
        |item
        |union all
        |select
        |*
        |from
        |item_master
        |""".stripMargin).createOrReplaceTempView("item_tmp")
    // --- 后续放置中间表模块
    //对 点击数据进行 扩维解析
    spark.sqlContext.uncacheTable("dwd_click_log")
    //最新上架的前10商品数
    val shopNewputawayGoodsDF = spark.sql(
      s"""
         |select
         |shop_id,
         |shelve_time,
         |item_id,
         |item_name,
         |$dt as dt
         |from
         |item_tmp
         |where profit_top <= 10
         |order by shelve_time desc
      """.stripMargin)
    writerMysql(shopNewputawayGoodsDF, "shop_one_goods_newputaway", flag)
    //商品销量排行
    val shopGoodsSaleTopDF = spark.sql(
      s"""
         |select
         |shop_id,
         |sku_id,
         |sku_mapping(sku_id) as sku_name,
         |sum(num) as sale_number,
         |$dt as dt
         |from
         |orders_retail
         |group by shop_id,sku_id
      """.stripMargin)
    writerMysql(shopGoodsSaleTopDF, "shop_one_goods_top", flag)
    //解析hdfs_page -- 需埋点
    /**
     * 每个商品
     * 访客数（UV） 浏览量 PV
     * 详情页跳出率( 商品分析-商品明细)
     * 1.从商品详情页 直接离开店铺的用户
     * 2.取店铺下每个用户访问的最后一条浏览记录，判断是否是详情页
     * 3.如果是 详情页，记录访问的 商品ID 如果不是则不做累计
     */
    // 先计算 登录用户，最后一个访问记录是商品的数量
    spark.sql(
      """
        |with t1 as (
        |select
        |shopId,skuId,page_type,
        |row_number() over(partition by shopId,loginToken,skuId order by timeIn desc ) as max_time
        |from
        |dwd_click_log
        |where loginToken != '' and loginToken is not null
        |),
        |t2 as (
        |select
        |shopId,skuId,page_type,
        |case when page_type == 'goods' then 1 else 0 end as last_access_page
        |from
        |t1
        |where max_time = 1
        |)
        |select
        |shopId,skuId,sum(last_access_page) as last_access_page
        |from
        |t2
        |where page_type = 'goods'
        |group by shopId,skuId
        |""".stripMargin).createOrReplaceTempView("user")
    //获取每个商品的不同用户的停留时长
    spark.sql(
      """
        |with t1 as (
        |select
        |shopId,skuId,
        |(max(timeIn) - min(timeIn))/1000 as time
        |from
        |dwd_click_log
        |where loginToken != '' and loginToken is not null and page_type == 'goods'
        |group by shopId,skuId,loginToken
        |)
        |select
        |shopId,skuId,
        |sum(time) as all_time
        |from
        |t1
        |group by shopId,skuId
        |""".stripMargin).createOrReplaceTempView("user_time")
    //计算 游客 ，最后一个访问记录是商品的数量
    spark.sql(
      """
        |with t1 as (
        |select
        |shopId,skuId,page_type,
        |row_number() over(partition by shopId,ip,skuId order by timeIn desc ) as max_time
        |from
        |dwd_click_log
        |where loginToken == '' or loginToken is null
        |),
        |t2 as (
        |select
        |shopId,skuId,page_type,
        |case when page_type == 'goods' then 1 else 0 end as last_access_page
        |from
        |t1
        |where max_time = 1
        |)
        |select
        |shopId,skuId,sum(last_access_page) as last_access_page
        |from
        |t2
        |where page_type = 'goods'
        |group by shopId,skuId
        |""".stripMargin).createOrReplaceTempView("tourist")
    //获取每个商品的不同用户的停留时长
    spark.sql(
      """
        |with t1 as (
        |select
        |shopId,skuId,
        |(max(timeIn) - min(timeIn))/1000 as time
        |from
        |dwd_click_log
        |where (loginToken == '' or loginToken is null) and page_type == 'goods'
        |group by shopId,skuId,ip
        |)
        |select
        |shopId,skuId,
        |sum(time) as all_time
        |from
        |t1
        |group by shopId,skuId
        |""".stripMargin).createOrReplaceTempView("tourist_time")
    spark.sql(
      """
        |select  --- 全是访客的数据 和 访客+游客的数据 不包含全是游客的数据
        |a.shopId,
        |a.skuId,
        |case when b.last_access_page is null
        |then a.last_access_page else a.last_access_page + b.last_access_page end as last_access_number
        |from
        |user a
        |left join
        |tourist b
        |on a.shopId  = b.shopId and a.skuId = b.skuId
        |
        |union all
        |
        |select --- 只包含 全是游客的数据
        |b.shopId,
        |b.skuId,
        |b.last_access_page as last_access_number
        |from
        |user a
        |right join
        |tourist b
        |on a.shopId  = b.shopId and a.skuId = b.skuId
        |where a.shopId is null
        |""".stripMargin).createOrReplaceTempView("last_access")
    spark.sql(
      """
        |select  --- 全是访客的数据 和 访客+游客的数据 不包含全是游客的数据
        |a.shopId,
        |a.skuId,
        |case when b.all_time is null
        |then a.all_time else a.all_time + b.all_time end as all_time
        |from
        |user_time a
        |left join
        |tourist_time b
        |on a.shopId  = b.shopId and a.skuId = b.skuId
        |
        |union all
        |
        |select --- 只包含 全是游客的数据
        |b.shopId,
        |b.skuId,
        |b.all_time as all_time
        |from
        |user_time a
        |right join
        |tourist_time b
        |on a.shopId  = b.shopId and a.skuId = b.skuId
        |where a.shopId is null
        |""".stripMargin).createOrReplaceTempView("last_access_time")
    spark.sql(
      """
        |with t1 as (
        |select
        |shopId,
        |skuId,
        |count(1) as pv,
        |count(distinct
        |case when loginToken == '' or loginToken is null
        |then ip else loginToken end
        |) as uv
        |from
        |dwd_click_log
        |group by shopId,skuId
        |)
        |select
        |a.shopId,
        |a.skuId,
        |a.pv,
        |a.uv,
        |cast(if(c.all_time is null ,0,c.all_time) /  a.uv as decimal(10,2)) as avg_time,
        |if(b.last_access_number is null ,0,b.last_access_number) as last_access_number,
        |cast(if(b.last_access_number is null ,0,b.last_access_number) / a.uv as decimal(10,2)) as page_leave_ratio
        |from
        |t1 a
        |left join
        |last_access b
        |on a.shopId  = b.shopId and a.skuId = b.skuId
        |left join
        |last_access_time c
        |on a.shopId  = c.shopId and a.skuId = c.skuId
        |""".stripMargin).createOrReplaceTempView("page_info_all")
    // 先计算 登录用户，最后一个访问记录是商品的数量
    spark.sql(
      """
        |with t1 as (
        |select
        |shopId,page_source,skuId,page_type,
        |row_number() over(partition by shopId,loginToken,page_source,skuId order by timeIn desc ) as max_time
        |from
        |dwd_click_log
        |where loginToken != '' and loginToken is not null
        |),
        |t2 as (
        |select
        |shopId,page_source,skuId,page_type,
        |case when page_type == 'goods' then 1 else 0 end as last_access_page
        |from
        |t1
        |where max_time = 1
        |)
        |select
        |shopId,page_source,skuId,sum(last_access_page) as last_access_page
        |from
        |t2
        |where page_type = 'goods'
        |group by shopId,page_source,skuId
        |""".stripMargin).createOrReplaceTempView("user_type")
    //获取每个商品的不同用户的停留时长
    spark.sql(
      """
        |with t1 as (
        |select
        |shopId,page_source,skuId,
        |(max(timeIn) - min(timeIn))/1000 as time
        |from
        |dwd_click_log
        |where loginToken != '' and loginToken is not null and page_type == 'goods'
        |group by shopId,skuId,page_source,loginToken
        |)
        |select
        |shopId,skuId,page_source,
        |sum(time) as all_time
        |from
        |t1
        |group by shopId,skuId,page_source
        |""".stripMargin).createOrReplaceTempView("user_type_time")
    //计算 游客 ，最后一个访问记录是商品的数量
    spark.sql(
      """
        |with t1 as (
        |select
        |shopId,skuId,page_source,page_type,
        |row_number() over(partition by shopId,ip,page_source,skuId order by timeIn desc ) as max_time
        |from
        |dwd_click_log
        |where loginToken == '' or loginToken is null
        |),
        |t2 as (
        |select
        |shopId,skuId,page_source,page_type,
        |case when page_type == 'goods' then 1 else 0 end as last_access_page
        |from
        |t1
        |where max_time = 1
        |)
        |select
        |shopId,page_source,skuId,sum(last_access_page) as last_access_page
        |from
        |t2
        |where page_type = 'goods'
        |group by shopId,page_source,skuId
        |""".stripMargin).createOrReplaceTempView("tourist_type")
    //获取每个商品的不同用户的停留时长
    spark.sql(
      """
        |with t1 as (
        |select
        |shopId,skuId,page_source,
        |(max(timeIn) - min(timeIn))/1000 as time
        |from
        |dwd_click_log
        |where (loginToken == '' or loginToken is null) and page_type == 'goods'
        |group by shopId,skuId,page_source,ip
        |)
        |select
        |shopId,skuId,page_source,
        |sum(time) as all_time
        |from
        |t1
        |group by shopId,page_source,skuId
        |""".stripMargin).createOrReplaceTempView("tourist_type_time")
    spark.sql(
      """
        |select  --- 全是访客的数据 和 访客+游客的数据 不包含全是游客的数据
        |a.shopId,
        |a.skuId,
        |a.page_source,
        |case when b.last_access_page is null
        |then a.last_access_page else a.last_access_page + b.last_access_page end as last_access_number
        |from
        |user_type a
        |left join
        |tourist_type b
        |on a.shopId  = b.shopId and a.skuId = b.skuId  and a.page_source = b.page_source
        |
        |union all
        |
        |select --- 只包含 全是游客的数据
        |b.shopId,
        |b.skuId,
        |b.page_source,
        |b.last_access_page as last_access_number
        |from
        |user_type a
        |right join
        |tourist_type b
        |on a.shopId  = b.shopId and a.skuId = b.skuId  and a.page_source = b.page_source
        |where a.shopId is null
        |""".stripMargin).createOrReplaceTempView("last_access_type")
    spark.sql(
      """
        |select  --- 全是访客的数据 和 访客+游客的数据 不包含全是游客的数据
        |a.shopId,
        |a.skuId,
        |a.page_source,
        |case when b.all_time is null
        |then a.all_time else a.all_time + b.all_time end as all_time
        |from
        |user_type_time a
        |left join
        |tourist_type_time b
        |on a.shopId  = b.shopId and a.skuId = b.skuId  and a.page_source = b.page_source
        |
        |union all
        |
        |select --- 只包含 全是游客的数据
        |b.shopId,
        |b.skuId,
        |b.page_source,
        |b.all_time as all_time
        |from
        |user_type_time a
        |right join
        |tourist_type_time b
        |on a.shopId  = b.shopId and a.skuId = b.skuId  and a.page_source = b.page_source
        |where a.shopId is null
        |""".stripMargin).createOrReplaceTempView("last_access_type_time")
    spark.sql(
      """
        |with t1 as (
        |select
        |shopId,
        |skuId,
        |page_source,
        |count(1) as pv,
        |count(distinct
        |case when loginToken == '' or loginToken is null
        |then ip else loginToken end
        |) as uv
        |from
        |dwd_click_log
        |group by shopId,skuId,page_source
        |)
        |select
        |a.shopId,
        |a.skuId,
        |a.page_source,
        |a.pv,
        |a.uv,
        |cast(if(c.all_time is null ,0,c.all_time) /  a.uv as decimal(10,2)) as avg_time,
        |cast(b.last_access_number / a.uv as decimal(10,2)) as page_leave_ratio
        |from
        |t1 a
        |left join
        |last_access_type b
        |on a.shopId  = b.shopId and a.skuId = b.skuId and a.page_source = b.page_source
        |left join
        |last_access_type_time c
        |on a.shopId  = c.shopId and a.skuId = c.skuId and a.page_source = c.page_source
        |""".stripMargin).createOrReplaceTempView("page_info_type")
    /**
     * 1、加购商品件数( 商品分析-商品明细)：
     * 被客户加入购物车的商品的件数，没有去掉加购后减少的件数。
     */
    /**
     * 加购人数( 商品分析-商品明细)：
     * 即加购的客户的数量。当筛选时，暂时不具备合计的加购客户数。
     */
    // 订单明细 中间表 和 商品表关联 获取商品的 上架时间
    spark.sql(
      """
        |select
        |a.shop_id,
        |a.item_id,
        |a.sku_id,
        |a.paid,
        |a.buyer_id,
        |a.num,
        |a.payment_total_money,
        |a.order_source,
        |a.item_original_price, -- 销售价
        |a.cost_price, -- 成本价
        |b.shelve_time,
        |case when b.picture_url is null or b.picture_url == ''
        |then sku_picture_mapping(a.sku_id) else b.picture_url end as picture_url,
        |b.cid_name
        |from
        |orders_retail a
        |left join
        |item_tmp b
        |on
        |a.shop_id = b.shop_id and a.item_id = b.item_id
        |""".stripMargin).createOrReplaceTempView("order_item")
    /**
     * 支付件数
     * 支付人数
     * 每个商品得支付金额
     * --------------------
     * 访问-支付转化率：
     * 支付客户数/访客数。
     * 下单客户数( 商品分析-商品明细)：
     * 下单商品的客户数，下单即算，包含下单未支付订单，不剔除取消订单
     */
    spark.sql(
      s"""
         |select
         |a.shop_id,
         |a.sku_id ,
         |a.sku_name,
         |a.paid_number,
         |a.sale_user_number,
         |a.sale_succeed_money,
         |a.all_sale_user_count,
         |if(pv is null,0,pv) as pv,
         |if(uv is null,0,uv) as uv,
         |if(avg_time is null,0,avg_time) as avg_time,
         |cast (a.sale_succeed_money/if(uv is null,0,uv)  as decimal(10,2)) as uv_value,
         |if(page_leave_ratio is null,0,page_leave_ratio) as page_leave_ratio,
         |cast (a.sale_user_number/if(uv is null,0,uv)  as decimal(10,2)) as orders_ratio,
         |'all' as source_type
         |from
         |(
         |select
         |shop_id,
         |sku_id,
         |sku_mapping(sku_id) sku_name,
         |sum(case when paid = 2 then num end) as paid_number, --支付件数
         |count(distinct case when paid = 2 then buyer_id end) as sale_user_number, -- 成交的下单用户数
         |round(sum(case when paid = 2 then payment_total_money end),2) as sale_succeed_money, --支付金额
         |count(distinct buyer_id) as all_sale_user_count -- 总下单用户数
         |from
         |order_item
         |group by
         |shop_id,
         |sku_id
         |) a
         |left join
         |page_info_all b on
         |a.shop_id = b.shopId and
         |a.sku_id = b.skuId
         |""".stripMargin).union(
      spark.sql(
        s"""
           |select
           |a.shop_id,
           |a.sku_id ,
           |a.sku_name,
           |a.paid_number,
           |a.sale_user_number,
           |a.sale_succeed_money,
           |a.all_sale_user_count,
           |if(pv is null,0,pv) as pv,
           |if(uv is null,0,uv) as uv,
           |if(avg_time is null,0,avg_time) as avg_time,
           |cast (a.sale_succeed_money/if(uv is null,0,uv)  as decimal(10,2)) as uv_value,
           |if(page_leave_ratio is null,0,page_leave_ratio) as page_leave_ratio,
           |cast (a.sale_user_number/if(uv is null,0,uv)  as decimal(10,2)) as orders_ratio,
           |a.order_source as source_type
           |from(
           |select
           |shop_id,
           |sku_id,
           |order_source,
           |sku_mapping(sku_id) sku_name,
           |sum(case when paid = 2 then num end) as paid_number, --支付件数
           |count(distinct case when paid = 2 then buyer_id end) as sale_user_number, -- 成交的下单用户数
           |round(sum(case when paid = 2 then payment_total_money end),2) as sale_succeed_money, --支付金额
           |count(distinct buyer_id) as all_sale_user_count -- 总下单用户数
           |from
           |order_item
           |group by
           |shop_id,
           |sku_id,
           |order_source
           |) a
           |left join
           |page_info_type b
           |on
           |a.shop_id = b.shopId and
           |a.sku_id = b.skuId and
           |a.order_source = b.page_source
           |""".stripMargin)
    ).createOrReplaceTempView("shop_one_goods")
    val shopGoodsPayInfoDF = spark.sql(
      s"""
         |
         |select
         |a.shop_id,
         |a.sku_id,
         |a.shelve_time,
         |a.picture_url,
         |a.item_original_price, -- 销售价
         |a.cost_price, -- 成本价
         |a.cid_name, -- 类目
         |b.sku_name,
         |b.paid_number,
         |b.sale_user_number,
         |b.sale_succeed_money,
         |b.all_sale_user_count,
         |b.pv,
         |b.uv,
         |b.uv_value,
         |b.page_leave_ratio,
         |b.orders_ratio,
         |b.source_type,
         |$dt as dt
         |from
         |(
         |select
         |distinct
         |shop_id,
         |sku_id,
         |shelve_time,
         |picture_url,
         |item_original_price, -- 销售价
         |cost_price, -- 成本价
         |cid_name -- 类目
         |from
         |order_item
         |) a
         |left join
         |shop_one_goods b
         |on
         |a.shop_id = b.shop_id and a.sku_id=b.sku_id
         |""".stripMargin)
    writerMysql(shopGoodsPayInfoDF, "shop_one_goods_info", flag)

  }
}

//    spark.sql(
//      s"""
//         |select
//         |shop_id,
//         |sku_id,
//         |0 as pv,
//         |0 as uv,
//         |'all' as source_type,
//         |sku_mapping(sku_id) sku_name,
//         |sum(case when paid = 2 then num end) as paid_number, --支付件数
//         |count(distinct case when paid = 2 then buyer_id end) as sale_user_number, -- 成交的下单用户数
//         |round(sum(case when paid = 2 then payment_total_money end),2) as sale_succeed_money, --支付金额
//         |count(distinct buyer_id) as all_sale_user_count, -- 总下单用户数
//         |0.00 as sku_rate,
//         |$dt as dt
//         |from
//         |order_item
//         |group by
//         |shop_id,
//         |sku_id
//         |""".stripMargin)
//      .union(spark.sql(
//        s"""
//           |select
//           |shop_id,
//           |sku_id,
//           |0 as pv,
//           |0 as uv,
//           |order_source as source_type,
//           |sku_mapping(sku_id) sku_name,
//           |sum(case when paid = 2 then num end) as paid_number, --支付件数
//           |count(distinct case when paid = 2 then buyer_id end) as sale_user_number, -- 成交的下单用户数
//           |round(sum(case when paid = 2 then payment_total_money end),2) as sale_succeed_money, --支付金额
//           |count(distinct buyer_id) as all_sale_user_count, -- 总下单用户数
//           |0.00 as sku_rate,
//           |$dt as dt
//           |from
//           |order_item
//           |group by
//           |shop_id,
//           |sku_id,
//           |order_source
//           |""".stripMargin)).createOrReplaceTempView("shop_one_goods")