drop table off_line_dws.dws_shop_deal_info on cluster bip_ck_cluster;
CREATE TABLE off_line_dws.dws_shop_deal_info on cluster bip_ck_cluster
(
    shop_id                String,
    buyer_id               String,
    buyer_name             String,
    sku_id                 String,
    item_name              String,
    pick_id                String,
    pick_name              String,
    province_name          String,
    city_name              String,
    country_name           String,
    sku_pic_url            String,
    paid                   Int64,
    refund                 Int64,
    po_type                String,
    seller_id              String,
    seller_name            String,
    cid                    String,
    cat_3d_name            String,
    order_type             String,
    sale_order_number      Int64 comment '下单笔数',
    sale_pick_order_number Int64 comment '自提点订单数',
    num                    Decimal(10, 2) comment '下单件数',
    payment_num            Decimal(10, 2) comment '支付件数',
    payment_succeed_money  Decimal(10, 2) comment '支付成功金额',
    payment_succeed_number Int64 comment '支付成功数据(成交单量)',
    income_money           Decimal(10, 2) comment '收入金额',
    payment_money          Decimal(10, 2) comment '下单金额',
    dt                     String
    )
    ENGINE = MergeTree()
    PARTITION BY (dt)
    ORDER BY (shop_id)
    SETTINGS index_granularity = 8192;



drop table off_line_dws.dws_shop_sku_info on cluster bip_ck_cluster
CREATE TABLE off_line_dws.dws_shop_sku_info on cluster bip_ck_cluster
(
    shop_id                String,
    sku_id                 String,
    item_name              String,
    sku_pic_url            String,
    order_type             String,
    cost_price             Decimal(10, 2),
    payment_price          Decimal(10, 2),
    sale_order_number      Int64 comment '下单笔数',
    payment_num            Decimal(10, 2) comment '支付件数',
    payment_succeed_money  Decimal(10, 2) comment '支付成功金额',
    payment_succeed_number Int64 comment '支付成功数据(成交单量)',
    income_money           Decimal(10, 2) comment '收入金额',
    payment_money          Decimal(10, 2) comment '下单金额',
    dt                     String
    )
    ENGINE = MergeTree()
    PARTITION BY (dt)
    ORDER BY (shop_id)
    SETTINGS index_granularity = 8192;

CREATE TABLE off_line_dws.dws_shop_deal_refund_info on cluster bip_ck_cluster
(
    shop_id              String,
    refund_reason        String,
    sku_id               String,
    sku_pic_url          String,
    item_name            String,
    order_type           String,
    refund_reason_number Int64 comment '店铺下每个商品的总退款单数',
    refund_number        Int64 comment '成功退款数量',
    refund_money         Decimal(10, 2) comment '成功退款金额',
    all_money            Decimal(10, 2) comment '申请退款金额',
    avg_time             Decimal(10, 2),
    dt                   String
    )
    ENGINE = MergeTree()
    PARTITION BY (dt)
    ORDER BY (shop_id)
    SETTINGS index_granularity = 8192;

drop table off_line_dws.dws_shop_clicklog_info on cluster bip_ck_cluster;
CREATE TABLE off_line_dws.dws_shop_clicklog_info on cluster bip_ck_cluster
(
    shop_id          String,
    loginToken       String,
    ip               String,
    user_id          String,
    sku_id           String,
    order_type       String,
    pv               Int64,
    time             Decimal(10, 2),
    one_visit_page   Int64, --只访问一次页面
    last_access_page Int64, --最后一次访问商品页面数量
    dt               String
    )
    ENGINE = MergeTree()
    PARTITION BY (dt)
    ORDER BY (shop_id)
    SETTINGS index_granularity = 8192;


-- shop_id|order_type|po_type|paid|  buyer_id|first_time|last_time|final_time|      dt
CREATE TABLE off_line_dws.dws_shop_client_info on cluster bip_ck_cluster
(
    shop_id    String,
    order_type String,
    po_type    String,
    paid       String,
    buyer_id   String,
    first_time String,
    last_time  String, --只访问一次页面
    final_time String, --只访问一次页面
    dt         String
)
    ENGINE = MergeTree()
    PARTITION BY (dt)
    ORDER BY (shop_id)
    SETTINGS index_granularity = 8192;


drop table off_line_dws.dws_shop_item_info on cluster bip_ck_cluster
CREATE TABLE off_line_dws.dws_shop_item_info on cluster bip_ck_cluster
(
    shop_id     String,
    item_number Int64,
    dt          String
)
    ENGINE = MergeTree()
    PARTITION BY (dt)
    ORDER BY (shop_id)
    SETTINGS index_granularity = 8192;

drop table off_line_dws.dws_shop_inbound_bill_record on cluster bip_ck_cluster;
CREATE TABLE off_line_dws.dws_shop_inbound_bill_record on cluster bip_ck_cluster
(
    shop_id        String,
    item_name      String,
    sku_code       String,
    warehouse_code String,
    warehouse_name String,
    brand_id       String,
    brand_name     String,
    total_money    Float64,
    inbound_num    Float64,
    price          Float64,
    dt             String
)
    ENGINE = MergeTree()
    PARTITION BY (dt)
    ORDER BY (shop_id)
    SETTINGS index_granularity = 8192;



CREATE TABLE off_line_dws.shop_client_vip_info on cluster bip_ck_cluster
(
    shop_id                String,
    all_vip_user_number    Int64,
    new_vip_user_number    Int64,
    vip_user_access_number Int64,
    vip_user_up            Int64,
    attention_number       Int64,
    new_attention_number   Int64,
    all_client_user_number Int64,
    new_client_user_number Int64,
    dt                     String
)
    ENGINE = MergeTree()
    PARTITION BY (dt)
    ORDER BY (shop_id)
    SETTINGS index_granularity = 8192;

drop table off_line_dws.dws_shop_warehouse_inout on cluster bip_ck_cluster;
CREATE TABLE off_line_dws.dws_shop_warehouse_inout on cluster bip_ck_cluster
(
    shop_id            String,
    sku_id             String,
    item_name          String,
    warehouse_name     String,
    in_warehouse_code  String,
    in_item_id         String,
    in_item_name       String,
    in_price           Decimal(10, 4),
    inbound_num        Decimal(10, 4),
    out_warehouse_code String,
    out_item_id        String,
    out_item_name      String,
    out_price          Decimal(10, 4),
    outbound_num       Decimal(10, 4),
    types              String,
    dt                 String
    )
    ENGINE = MergeTree()
    PARTITION BY (dt)
    ORDER BY (shop_id)
    SETTINGS index_granularity = 8192;


CREATE TABLE off_line_dws.dws_shop_group_user_info on cluster bip_ck_cluster
(
    id                 String,
    user_id            String,
    shop_id            String,
    attend_group_count Int64,
    group_total_amount Decimal(20, 2),
    create_time        String,
    last_buy_time      String,
    dt                 String
    )
    ENGINE = MergeTree()
    PARTITION BY (dt)
    ORDER BY (shop_id)
    SETTINGS index_granularity = 8192;

---------------------------------- 移动H5


-- 移动端H5-离线-全部数据-核心数据

-- b.totalMoney   as "总订单金额",
--        b.orderNumber  as "总订单数",
--        b.userNumber   as "总下单人数",
--        a.income_money as "总收入",
--        c.uv           as "浏览人数",
--        c.pv           as "浏览次数",
--        b.newUser      as "新下单用户",
--        d.refund_money as "退款金额"
-- 移动端H5-离线-全部数据-概况-核心
select `总订单金额`,
       `总订单数`,
       `总下单人数`,
       `总收入`,
       `浏览人数`,
       `浏览次数`,
       c.`新下单用户` as `新下单用户`,
       d.`退款金额`  as `退款金额`
from (
         select shop_id,
                sum(payment_money)       as "总订单金额",
                sum(sale_order_number)   as "总订单数",
                count(distinct buyer_id) as "总下单人数",
                sum(income_money)        as "总收入"
         from dws_shop_deal_info
         where shop_id = '2000000027'
           and order_type = 'ALL'
           and dt between '2021-07-21' and '2021-07-21'
         group by shop_id
     ) a
    all
         full join (
    select shop_id,
           count(distinct if(loginToken is null or loginToken = '', ip, loginToken)) as "浏览人数",
           sum(pv)                                                                      "浏览次数"
    from dws_shop_clicklog_info
    where shop_id = '2000000027'
      and dt between '2021-07-21' and '2021-07-21'
    group by shop_id
    ) b
on a.shop_id = b.shop_id
    all
    full join (
    select shop_id,
    count(distinct case when paid = '2' and last_time = '' then buyer_id end) as "新下单用户"
    from dws_shop_client_info
    where shop_id = '2000000027'
    and dt between '2021-07-21' and '2021-07-21'
    group by shop_id
    ) c
    on a.shop_id = c.shop_id
    all
    full join (
    select shop_id,
    sum(refund_money) as "退款金额"
    from dws_shop_deal_refund_info
    where shop_id = '2000000027'
    and order_type = 'ALL'
    and dt between '2021-07-21' and '2021-07-21'
    group by shop_id
    ) d
    on a.shop_id = d.shop_id;
-- 移动端H5-离线-全部数据-概况-趋势
select case
           when a.dt = '' and b.dt = '' and c.dt = ''
               then d.dt
           when d.dt = '' and b.dt = '' and c.dt = ''
               then a.dt
           when a.dt = '' and b.dt = '' and d.dt = ''
               then c.dt
           when a.dt = '' and c.dt = '' and c.dt = ''
               then b.dt
           else a.dt
           end
                 as "时间",
       a.dt,
       b.dt,
       c.dt,
       d.dt,
       `总订单金额`,
       `总订单数`,
       `总下单人数`,
       `总收入`,
       `浏览人数`,
       `浏览次数`,
       c.`新下单用户` as `新下单用户`,
       d.`退款金额`  as `退款金额`
from (
         select dt,
                shop_id,
                sum(payment_money)       as "总订单金额",
                sum(sale_order_number)   as "总订单数",
                count(distinct buyer_id) as "总下单人数",
                sum(income_money)        as "总收入"
         from dws_shop_deal_info
         where shop_id = '2000000027'
           and order_type = 'ALL'
           and dt between '2021-07-01' and '2021-07-31'
         group by shop_id, dt
     ) a
    all
         full join (
    select dt,
           shop_id,
           count(distinct if(loginToken is null or loginToken = '', ip, loginToken)) as "浏览人数",
           sum(pv)                                                                      "浏览次数"
    from dws_shop_clicklog_info
    where shop_id = '2000000027'
      and dt between '2021-07-01' and '2021-07-31'
    group by shop_id, dt
    ) b
on a.shop_id = b.shop_id and a.dt = b.dt
    all
    full join (
    select dt,
    shop_id,
    count(distinct case when paid = '2' and last_time = '' then buyer_id end) as "新下单用户"
    from dws_shop_client_info
    where shop_id = '2000000027'
    and dt between '2021-07-01' and '2021-07-31'
    group by dt, shop_id
    ) c
    on a.shop_id = c.shop_id and a.dt = c.dt
    all
    full join (
    select dt,
    shop_id,
    sum(refund_money) as "退款金额"
    from dws_shop_deal_refund_info
    where shop_id = '2000000027'
    and order_type = 'ALL'
    and dt between '2021-07-01' and '2021-07-31'
    group by dt, shop_id
    ) d
    on a.shop_id = d.shop_id and a.dt = d.dt;


-- 移动端H5-离线-全部数据-交易-趋势
select dt,
       sum(`总订单金额`)   "总订单金额",
       sum(`总收入`)     `总收入`,
       sum(`总订单数`)    `总订单数`,
       sum(`客单价`)     `客单价`,
       sum(`退款金额`)    `退款金额`,
       sum(`自提点订单金额`) `自提点订单金额`,
       sum(`自提点订单收入`) `自提点订单收入`,
       sum(`自提点订单数量`) `自提点订单数量`,
       sum(`自提点数量`)   `自提点数量`
from (
         select dt,
                sum(payment_money)                                                            as "总订单金额",
                sum(income_money)                                                             as "总收入",
                sum(sale_order_number)                                                        as "总订单数",
                CAST(sum(payment_succeed_money) / count(distinct buyer_id) as Decimal(10, 2)) as "客单价",
                0                                                                                "退款金额",
                0                                                                                "自提点订单金额",
                0                                                                                "自提点订单收入",
                0                                                                                "自提点订单数量",
                ifNull(count(distinct case when pick_id = '' then null else pick_id end), 0)  as "自提点数量"
         from dws_shop_deal_info
         where shop_id = '2000000079'
           and order_type = 'ALL'
           and dt between 20210713 and 20210713
         group by dt
         union all
         select dt,
                0                 as "总订单金额",
                0                 as "总收入",
                0                 as "总订单数",
                0                 as "客单价",
                sum(refund_money) as "退款金额",
                0                    "自提点订单金额",
                0                    "自提点订单收入",
                0                    "自提点订单数量",
                0                 as "自提点数量"
         from dws_shop_deal_refund_info
         where shop_id = '2000000079'
           and order_type = 'ALL'
           and dt between 20210713 and 20210713
         group by dt
         union all
         select dt,
                0                      as "总订单金额",
                0                      as "总收入",
                0                      as "总订单数",
                0                      as "客单价",
                0                      as "退款金额",
                sum(payment_money)     as "自提点订单金额",
                sum(income_money)      as "自提点订单收入",
                sum(sale_order_number) as "自提点订单数量",
                0                      as "自提点数量"
         from dws_shop_deal_info
         where shop_id = '2000000079'
           and order_type = 'ALL'
           and pick_id != ''
        and dt between 20210713 and 20210713
         group by dt, pick_id, pick_name)
group by dt;
-- 移动端H5-离线-全部数据-交易-交易数据
select sum(`总订单金额`)   "总订单金额",
       sum(`总收入`)     `总收入`,
       sum(`总订单数`)    `总订单数`,
       sum(`客单价`)     `客单价`,
       sum(`退款金额`)    `退款金额`,
       sum(`自提点订单金额`) `自提点订单金额`,
       sum(`自提点订单收入`) `自提点订单收入`,
       sum(`自提点订单数量`) `自提点订单数量`,
       sum(`自提点数量`)   `自提点数量`
from (
         select sum(payment_money)                                                                         as "总订单金额",
                sum(income_money)                                                                          as "总收入",
                sum(sale_order_number)                                                                     as "总订单数",
                CAST(intDivOrZero(sum(payment_succeed_money), count(distinct buyer_id)) as Decimal(10, 2)) as "客单价",
                0                                                                                             "退款金额",
                0                                                                                             "自提点订单金额",
                0                                                                                             "自提点订单收入",
                0                                                                                             "自提点订单数量",
                ifNull(count(distinct case when pick_id = '' then null else pick_id end), 0)               as "自提点数量"
         from dws_shop_deal_info
         where shop_id = '2000000079'
           and order_type = 'ALL'
           and dt between '2021-07-13' and '2021-07-13'
         union all
         select 0                 as "总订单金额",
                0                 as "总收入",
                0                 as "总订单数",
                0                 as "客单价",
                sum(refund_money) as "退款金额",
                0                    "自提点订单金额",
                0                    "自提点订单收入",
                0                    "自提点订单数量",
                0                 as "自提点数量"
         from dws_shop_deal_refund_info
         where shop_id = '2000000079'
           and order_type = 'ALL'
           and dt between '2021-07-13' and '2021-07-13'
         union all
         select 0                      as "总订单金额",
                0                      as "总收入",
                0                      as "总订单数",
                0                      as "客单价",
                0                      as "退款金额",
                sum(payment_money)     as "自提点订单金额",
                sum(income_money)      as "自提点订单收入",
                sum(sale_order_number) as "自提点订单数量",
                0                      as "自提点数量"
         from dws_shop_deal_info
         where shop_id = '2000000079'
           and order_type = 'ALL'
           and pick_id != ''
        and dt between '2021-07-13' and '2021-07-13'
         group by pick_id, pick_name);


-- 移动端H5-离线-全部数据-交易-退款原因分析
select sum(refund_money) as                                                                    "退款金额",
       case
           when sum(refund_money) = 0 then 0
           when sum(all_money) = 0 then 0
           else round(CAST(sum(refund_money) as Int64) / CAST(sum(all_money) as Int64), 2) end "占比"
from dws_shop_deal_refund_info
where shop_id = '2000000079'
  and order_type = 'ALL'
  and dt between '2021-07-13' and '2021-07-13';
-- 移动端H5-离线-全部数据-商品-商品数据
select item_name   as "商品名称",
       sku_pic_url as "图片路径",
       sum(`销量`)   as `销量`,
       sum(`支付金额`) as `支付金额`,
       sum(`退款金额`) as `退款金额`
from (
         select item_name,
                sku_pic_url,
                sum(payment_num)           "销量",
                sum(payment_succeed_money) "支付金额",
                0                          "退款金额"
         from dws_shop_deal_info
         where shop_id = '2000000111'
           and order_type = 'ALL'
           and dt between '2021-07-13' and '2021-07-13'
         group by item_name, sku_pic_url
         union all
         select item_name,
                sku_pic_url,
                0                 "销量",
                0                 "支付金额",
                sum(refund_money) "退款金额"
         from dws_shop_deal_refund_info
         where shop_id = '2000000111'
           and order_type = 'ALL'
           and dt between '2021-07-13' and '2021-07-13'
         group by item_name, sku_pic_url
     )
group by item_name, sku_pic_url;
-- 移动端H5-离线-全部数据-概况-收入分析
select a.typs      as             "类型",
       a.tb_income as             "收入",
       a.tb_income / b.all_income "占比"
from (
         select shop_id,
                '渠道收入'            as typs,
                sum(income_money) as tb_income
         from dws_shop_deal_info
         where shop_id = '2000000027'
           and order_type = 'TB'
           and dt between '2021-07-01' and '2021-07-30'
         group by shop_id
     ) a
         left join (
    select shop_id,
           '全部收入'            as typs,
           sum(income_money) as all_income
    from dws_shop_deal_info
    where shop_id = '2000000027'
      and order_type = 'ALL'
      and dt between '2021-07-01' and '2021-07-30'
    group by shop_id
) b
                   on a.shop_id = b.shop_id
union all
select a.typs      as             "类型",
       a.tc_income as             "收入",
       a.tc_income / b.all_income "占比"
from (
         select shop_id,
                '零售收入'            as typs,
                sum(income_money) as tc_income
         from dws_shop_deal_info
         where shop_id = '2000000027'
           and order_type = 'TC'
           and dt between '2021-07-01' and '2021-07-30'
         group by shop_id
     ) a
         left join (
    select shop_id,
           '全部收入'            as typs,
           sum(income_money) as all_income
    from dws_shop_deal_info
    where shop_id = '2000000027'
      and order_type = 'ALL'
      and dt between '2021-07-01' and '2021-07-30'
    group by shop_id
) b
                   on a.shop_id = b.shop_id;
--移动端H5-离线-全部数据-概况-趋势  --- 去除
select dt,
       sum(`总订单金额`)   "总订单金额",
       sum(`总收入`)     `总收入`,
       sum(`总订单数`)    `总订单数`,
       sum(`客单价`)     `客单价`,
       sum(`退款金额`)    `退款金额`,
       sum(`自提点订单金额`) `自提点订单金额`,
       sum(`自提点订单收入`) `自提点订单收入`,
       sum(`自提点订单数量`) `自提点订单数量`,
       sum(`自提点数量`)   `自提点数量`
from (
         select dt,
                sum(payment_money)                                                            as "总订单金额",
                sum(income_money)                                                             as "总收入",
                sum(sale_order_number)                                                        as "总订单数",
                CAST(sum(payment_succeed_money) / count(distinct buyer_id) as Decimal(10, 2)) as "客单价",
                0                                                                                "退款金额",
                0                                                                                "自提点订单金额",
                0                                                                                "自提点订单收入",
                0                                                                                "自提点订单数量",
                ifNull(count(distinct case when pick_id = '' then null else pick_id end), 0)  as "自提点数量"
         from dws_shop_deal_info
         where shop_id = '2000000079'
           and order_type = 'ALL'
           and dt between 20210713 and 20210713
         group by dt
         union all
         select dt,
                0                 as "总订单金额",
                0                 as "总收入",
                0                 as "总订单数",
                0                 as "客单价",
                sum(refund_money) as "退款金额",
                0                    "自提点订单金额",
                0                    "自提点订单收入",
                0                    "自提点订单数量",
                0                 as "自提点数量"
         from dws_shop_deal_refund_info
         where shop_id = '2000000079'
           and order_type = 'ALL'
           and dt between 20210713 and 20210713
         group by dt
         union all
         select dt,
                0                      as "总订单金额",
                0                      as "总收入",
                0                      as "总订单数",
                0                      as "客单价",
                0                      as "退款金额",
                sum(payment_money)     as "自提点订单金额",
                sum(income_money)      as "自提点订单收入",
                sum(sale_order_number) as "自提点订单数量",
                0                      as "自提点数量"
         from dws_shop_deal_info
         where shop_id = '2000000079'
           and order_type = 'ALL'
           and pick_id != ''
        and dt between 20210713 and 20210713
         group by dt, pick_id, pick_name)
group by dt;


-- 移动端H5-离线-全部数据-概况-新老用户分析
select '下单老用户'                                                                    as "用户类型",
       count(distinct case when paid = '2' and last_time != '' then buyer_id end) as present_user_dis_number
from dws_shop_client_info
where shop_id = '2000000027'
  and dt between '2021-07-01' and '2021-07-31'
  and buyer_id != ''
union all
select '下单新用户'                                                                   as "用户类型",
       count(distinct case when paid = '2' and last_time = '' then buyer_id end) as present_user_dis_number
from dws_shop_client_info
where shop_id = '2000000027'
  and dt between '2021-07-01' and '2021-07-31'
  and buyer_id != '';


-- 移动端H5-离线-全部数据-用户-用户数据
select `总订单金额`,
       `浏览人数`,
       `浏览次数`,
       `累计订阅人数`,
       `新增订阅人数`,
       `非会员人数`,
       `会员人数`,
       `新增会员人数`,
       `会员升级人数`
from (
         select shop_id,
                count(distinct if(loginToken is null or loginToken = '', ip, loginToken)) as "浏览人数",
                sum(pv)                                                                      "浏览次数"
         from dws_shop_clicklog_info
         where shop_id = '2000000027'
           and dt between '2021-07-14' and '2021-07-14'
         group by shop_id) a
    all
         full join
     (select shop_id,
             sum(attention_number)       as "累计订阅人数",
             sum(new_attention_number)   as "新增订阅人数",
             sum(all_client_user_number) as "非会员人数",
             sum(all_vip_user_number)    as "会员人数",
             sum(new_vip_user_number)    as "新增会员人数",
             sum(vip_user_up)            as "会员升级人数"
      from shop_client_vip_info
      where shop_id = '2000000027'
        and dt between '2021-07-14' and '2021-07-14'
      group by shop_id) b
on a.shop_id = b.shop_id
    all
    full join (
    select shop_id, sum(payment_money) as "总订单金额"
    from dws_shop_deal_info
    where shop_id = '2000000027'
    and order_type = 'ALL'
    and dt between '2021-07-14' and '2021-07-14'
    group by shop_id
    ) c
    on a.shop_id = c.shop_id;

--     渠道数据
select `渠道订单金额`,
       `渠道收入`,
       `渠道订单数`,
       `渠道下单人数`,
       `退款金额`,
       `渠道退款数量`
from (
         select shop_id,
                sum(payment_money)       as "渠道订单金额",
                sum(sale_order_number)   as "渠道订单数",
                count(distinct buyer_id) as "渠道下单人数",
                sum(income_money)        as "渠道收入"
         from dws_shop_deal_info
         where shop_id = '2000000027'
           and order_type = 'TB'
           and dt between '2021-07-15' and '2021-07-15'
         group by shop_id
     ) a
    all
         full join
     (
         select shop_id,
                sum(refund_money)  as "退款金额",
                sum(refund_number) as "渠道退款数量"
         from dws_shop_deal_refund_info
         where shop_id = '2000000079'
           and order_type = 'TB'
           and dt between '2021-07-15' and '2021-07-15'
         group by shop_id
         ) b
on a.shop_id = b.shop_id;
--     渠道数据-趋势
select dt,
       `渠道订单金额`,
       `渠道收入`,
       `渠道订单数`,
       `渠道下单人数`,
       `退款金额`,
       `渠道退款数量`
from (
         select shop_id,
                sum(payment_money)       as "渠道订单金额",
                sum(sale_order_number)   as "渠道订单数",
                count(distinct buyer_id) as "渠道下单人数",
                sum(income_money)        as "渠道收入"
         from dws_shop_deal_info
         where shop_id = '2000000027'
           and order_type = 'TB'
           and dt between '2021-07-15' and '2021-07-15'
         group by dt, shop_id
     ) a
    all
         full join
     (
         select dt,
                shop_id,
                sum(refund_money)  as "退款金额",
                sum(refund_number) as "渠道退款数量"
         from dws_shop_deal_refund_info
         where shop_id = '2000000079'
           and order_type = 'TB'
           and dt between '2021-07-15' and '2021-07-15'
         group by dt, shop_id
         ) b
on a.shop_id = b.shop_id;

-- 零售数据
select payment_money     as `零售订单金额`,
       income_money      as "零售收入",
       sale_order_number as "零售订单数",
       dis_buyer_id      as "零售下单人数",
       refund_money      as "退款金额",
       refund_number     as "退款数量",
       pv                as "零售浏览量",
       uv                as "零售访客数",
       user_number / uv  as "访问支付转换率"
from (
         select shop_id,
                sum(payment_money)                                                  as payment_money,
                sum(sale_order_number)                                              as sale_order_number,
                count(distinct buyer_id)                                            as dis_buyer_id,
                sum(income_money)                                                   as income_money,
                count(distinct case when paid = 2 and refund = 0 then buyer_id end) as user_number
         from dws_shop_deal_info
         where shop_id = '2000000027'
           and order_type = 'TC'
           and dt between '2021-07-25' and '2021-07-25'
         group by shop_id
     ) a
    all
         full join
     (
         select shop_id,
                sum(refund_money)  as refund_money,
                sum(refund_number) as refund_number
         from dws_shop_deal_refund_info
         where shop_id = '2000000027'
           and order_type = 'TC'
           and dt between '2021-07-25' and '2021-07-25'
         group by shop_id
         ) b
on a.shop_id = b.shop_id
    all
    full join (
    select shop_id,
    count(distinct if(loginToken is null or loginToken = '', ip, loginToken)) as uv,
    sum(pv)                                                                      pv
    from dws_shop_clicklog_info
    where shop_id = '2000000027'
    and order_type = 'H5'
    and dt between '2021-07-25' and '2021-07-25'
    group by shop_id
    ) c
    on a.shop_id = c.shop_id;
-- 零售数据-趋势
select dt,
       payment_money     as `零售订单金额`,
       income_money      as "零售收入",
       sale_order_number as "零售订单数",
       dis_buyer_id      as "零售下单人数",
       refund_money      as "退款金额",
       refund_number     as "退款数量",
       pv                as "零售浏览量",
       uv                as "零售访客数",
       user_number / uv  as "访问支付转换率"
from (
         select dt,
                shop_id,
                sum(payment_money)                                                  as payment_money,
                sum(sale_order_number)                                              as sale_order_number,
                count(distinct buyer_id)                                            as dis_buyer_id,
                sum(income_money)                                                   as income_money,
                count(distinct case when paid = 2 and refund = 0 then buyer_id end) as user_number
         from dws_shop_deal_info
         where shop_id = '2000000027'
           and order_type = 'TC'
           and dt between '2021-07-25' and '2021-07-25'
         group by dt, shop_id
     ) a
    all
         full join
     (
         select dt,
                shop_id,
                sum(refund_money)  as refund_money,
                sum(refund_number) as refund_number
         from dws_shop_deal_refund_info
         where shop_id = '2000000027'
           and order_type = 'TC'
           and dt between '2021-07-25' and '2021-07-25'
         group by dt, shop_id
         ) b
on a.shop_id = b.shop_id
    all
    full join (
    select dt,
    shop_id,
    count(distinct if(loginToken is null or loginToken = '', ip, loginToken)) as uv,
    sum(pv)                                                                      pv
    from dws_shop_clicklog_info
    where shop_id = '2000000027'
    and order_type = 'H5'
    and dt between '2021-07-25' and '2021-07-25'
    group by dt, shop_id
    ) c
    on a.shop_id = c.shop_id;
-------------------------------  pc
-- 客户分析-成交客户分析-ALL
select count(distinct case when a.buyer_id != '' then a.buyer_id end)                                 as "总订单用户数",
       sum(payment_succeed_money)                                                                     as "成交金额",
       count(distinct case when a.paid = 2 and a.refund = 0 and a.buyer_id != '' then a.buyer_id end) as "成交用户数",
       count(distinct case
                          when b.last_time != '' and a.paid = 2 and a.refund = 0 and a.buyer_id is not null and
                      a.buyer_id != '' then a.buyer_id end)                                  as "成交老用户数",
       count(distinct case
                          when b.last_time = '' and a.paid = 2 and a.refund = 0 and a.buyer_id is not null and
                               a.buyer_id != '' then a.buyer_id end)                                  as "成交新用户数",
       round(count(distinct case when a.paid = 2 and a.refund = 0 then a.buyer_id end) /
             count(distinct case when a.buyer_id != '' then a.buyer_id end), 2)                       as "成交客户占比",
       round(count(distinct case
                                when b.last_time = '' and a.paid = 2 and a.refund = 0 and a.buyer_id is not null and
                                     a.buyer_id != '' then a.buyer_id end) /
             count(distinct case when a.buyer_id != '' then a.buyer_id end), 2)                       as "新成交客户占比",
       round(count(distinct case
                                when b.last_time != '' and a.paid = 2 and a.refund = 0 and a.buyer_id is not null and
                            a.buyer_id != '' then a.buyer_id end) /
             count(distinct case when a.buyer_id != '' then a.buyer_id end), 2)                       as "老成交客户占比",
       CAST(sum(payment_succeed_money) /
            count(distinct case when a.buyer_id != '' then a.buyer_id end) as Decimal(10, 2))         as "客单价"
from (
         select shop_id,
                buyer_id,
                paid,
                refund,
                sum(payment_succeed_money) as payment_succeed_money
         from dws_shop_deal_info
         where shop_id = '2000000027'
           and order_type = 'ALL'
           and dt between '2021-07-22' and '2021-07-22'
           and buyer_id != ''
         group by shop_id, buyer_id, paid, refund) a
         left join (
    select shop_id, buyer_id, last_time
    from dws_shop_client_info
    where shop_id = '2000000027'
      and dt between '2021-07-22' and '2021-07-22'
    group by shop_id, buyer_id, last_time
) b
                   on a.shop_id = b.shop_id and a.buyer_id = b.buyer_id
group by a.shop_id;
--客户分析-交易构成-ALL
select count(distinct case when b.last_time != '' and a.paid = 2 and a.refund = 0 then a.buyer_id end)   as "老成交用户数",
       count(distinct case when b.last_time = '' and a.paid = 2 and a.refund = 0 then a.buyer_id end)    as "新成交用户数",
       sum(case when b.last_time != '' and a.paid = 2 and a.refund = 0 then a.payment_succeed_money end) as "老成交用户金额",
       sum(case when b.last_time = '' and a.paid = 2 and a.refund = 0 then a.payment_succeed_money end)  as "新成交用户金额"
from (
         select shop_id,
                buyer_id,
                paid,
                refund,
                sum(payment_succeed_money) as payment_succeed_money
         from dws_shop_deal_info
         where shop_id = '2000000027'
           and order_type = 'ALL'
           and dt between '2021-07-22' and '2021-07-22'
           and buyer_id != ''
         group by shop_id, buyer_id, paid, refund) a
         left join (
    select shop_id, buyer_id, last_time
    from dws_shop_client_info
    where shop_id = '2000000027'
      and dt between '2021-07-22' and '2021-07-22'
    group by shop_id, buyer_id, last_time
) b
                   on a.shop_id = b.shop_id and a.buyer_id = b.buyer_id
group by a.shop_id;
-- 客户分析-采购TOP-ALL
select buyer_name         as "用户名",
       sum(payment_money) as "采购金额",
       sum(income_money)  as "采购利润"
from dws_shop_deal_info
where shop_id = '2000000027'
  and order_type = 'ALL'
  and dt between '2021-07-21' and '2021-07-21'
  and po_type = 'PO'
group by buyer_id, buyer_name
    limit 10;

-- 客户分析-采购TOP-TC
select buyer_name         as "用户名",
       sum(payment_money) as "采购金额",
       sum(income_money)  as "采购利润"
from dws_shop_deal_info
where shop_id = '2000000027'
  and order_type = 'TC'
  and dt between '2021-07-15' and '2021-07-15'
  and po_type = 'PO'
group by buyer_id, buyer_name;
--客户分析-交易构成-TC
select count(distinct case when b.last_time != '' and a.paid = 2 and a.refund = 0 then a.buyer_id end)   as "老成交用户数",
       count(distinct case when b.last_time = '' and a.paid = 2 and a.refund = 0 then a.buyer_id end)    as "新成交用户数",
       sum(case when b.last_time != '' and a.paid = 2 and a.refund = 0 then a.payment_succeed_money end) as "老成交用户金额",
       sum(case when b.last_time = '' and a.paid = 2 and a.refund = 0 then a.payment_succeed_money end)  as "新成交用户金额"
from (
         select shop_id,
                buyer_id,
                paid,
                refund,
                sum(payment_succeed_money) as payment_succeed_money
         from dws_shop_deal_info
         where shop_id = '2000000027'
           and order_type = 'TC'
           and dt between '2021-07-15' and '2021-07-15'
         group by shop_id, buyer_id, paid, refund) a
         left join (
    select shop_id, buyer_id, last_time
    from dws_shop_client_info
    where shop_id = '2000000027'
      and dt between '2021-07-15' and '2021-07-15'
    group by shop_id, buyer_id, last_time
) b
                   on a.shop_id = b.shop_id and a.buyer_id = b.buyer_id
group by a.shop_id;
-- 客户分析-成交客户分析-TC
select count(distinct a.buyer_id)                                                             as "总订单用户数",
       sum(payment_succeed_money)                                                             as "成交金额",
       count(distinct case when a.paid = 2 and a.refund = 0 then a.buyer_id end)              as "成交用户数",
       count(distinct
             case when b.last_time != '' and a.paid = 2 and a.refund = 0 then a.buyer_id end) as "成交老用户数",
       count(distinct
             case when b.last_time = '' and a.paid = 2 and a.refund = 0 then a.buyer_id end)  as "成交新用户数",
       count(distinct case when a.paid = 2 and a.refund = 0 then a.buyer_id end) /
       count(distinct a.buyer_id)                                                             as "成交客户占比",
       count(distinct case when b.last_time = '' and a.paid = 2 and a.refund = 0 then a.buyer_id end) /
       count(distinct a.buyer_id)                                                             as "新成交客户占比",
       count(distinct case when b.last_time != '' and a.paid = 2 and a.refund = 0 then a.buyer_id end) /
       count(distinct a.buyer_id)                                                             as "老成交客户占比",
       CAST(sum(payment_succeed_money) / count(distinct buyer_id) as Decimal(10, 2))          as "客单价"
from (
         select shop_id,
                buyer_id,
                paid,
                refund,
                sum(payment_succeed_money) as payment_succeed_money
         from dws_shop_deal_info
         where shop_id = '2000000027'
           and order_type = 'TC'
           and dt between '2021-07-15' and '2021-07-15'
         group by shop_id, buyer_id, paid, refund) a
         left join (
    select shop_id, buyer_id, last_time
    from dws_shop_client_info
    where shop_id = '2000000027'
      and dt between '2021-07-15' and '2021-07-15'
    group by shop_id, buyer_id, last_time
) b
                   on a.shop_id = b.shop_id and a.buyer_id = b.buyer_id
group by a.shop_id;
-- 客户分析-采购TOP-TB
select buyer_name         as "用户名",
       sum(payment_money) as "采购金额",
       sum(income_money)  as "采购利润"
from dws_shop_deal_info
where shop_id = '2000000027'
  and order_type = 'TB'
  and dt between '2021-07-15' and '2021-07-15'
group by buyer_id, buyer_name;
--客户分析-交易构成-TB
select count(distinct case when b.last_time != '' and a.paid = 2 and a.refund = 0 then a.buyer_id end)   as "老成交用户数",
       count(distinct case when b.last_time = '' and a.paid = 2 and a.refund = 0 then a.buyer_id end)    as "新成交用户数",
       sum(case when b.last_time != '' and a.paid = 2 and a.refund = 0 then a.payment_succeed_money end) as "老成交用户金额",
       sum(case when b.last_time = '' and a.paid = 2 and a.refund = 0 then a.payment_succeed_money end)  as "新成交用户金额"
from (
         select shop_id,
                buyer_id,
                paid,
                refund,
                sum(payment_succeed_money) as payment_succeed_money
         from dws_shop_deal_info
         where shop_id = '2000000027'
           and order_type = 'TB'
           and dt between '2021-07-15' and '2021-07-15'
         group by shop_id, buyer_id, paid, refund) a
         left join (
    select shop_id, buyer_id, last_time
    from dws_shop_client_info
    where shop_id = '2000000027'
      and dt between '2021-07-15' and '2021-07-15'
    group by shop_id, buyer_id, last_time
) b
                   on a.shop_id = b.shop_id and a.buyer_id = b.buyer_id
group by a.shop_id;
-- 客户分析-成交客户分析-TB
select count(distinct case when a.buyer_id != '' then a.buyer_id end)                                           as "总订单用户数",
       sum(payment_succeed_money)                                                                               as "成交金额",
       count(distinct
             case when a.paid = 2 and a.refund = 0 and a.buyer_id != '' then a.buyer_id end)                    as "成交用户数",
       count(distinct case
                          when b.last_time != '' and a.paid = 2 and a.refund = 0 and a.buyer_id is not null and
                      a.buyer_id != ''
                              then a.buyer_id end)                                                              as "成交老用户数",
       count(distinct case
                          when b.last_time = '' and a.paid = 2 and a.refund = 0 and a.buyer_id is not null and
                               a.buyer_id != ''
                              then a.buyer_id end)                                                              as "成交新用户数",
       count(distinct case when a.paid = 2 and a.refund = 0 then a.buyer_id end) /
       count(distinct case when a.buyer_id != '' then a.buyer_id end)                                           as "成交客户占比",
       intDivOrZero(count(distinct case
                                       when b.last_time = '' and a.paid = 2 and a.refund = 0 and
                                            a.buyer_id is not null and
                                            a.buyer_id != '' then a.buyer_id end),
                    count(distinct case when a.buyer_id != '' then a.buyer_id end))                             as "新成交客户占比",
       intDivOrZero(count(distinct case
                                       when b.last_time != '' and a.paid = 2 and a.refund = 0 and
                                   a.buyer_id is not null and
                                   a.buyer_id != '' then a.buyer_id end),
                    count(distinct case when a.buyer_id != '' then a.buyer_id end))                             as "老成交客户占比",
       intDivOrZero(sum(payment_succeed_money), count(distinct case when a.buyer_id != '' then a.buyer_id end)) as "客单价"
from (
         select shop_id,
                buyer_id,
                paid,
                refund,
                sum(payment_succeed_money) as payment_succeed_money
         from dws_shop_deal_info
         where shop_id = '2000000027'
           and order_type = 'TB'
           and dt between '2021-07-22' and '2021-07-22'
           and buyer_id != ''
         group by shop_id, buyer_id, paid, refund) a
         left join (
    select shop_id, buyer_id, last_time
    from dws_shop_client_info
    where shop_id = '2000000027'
      and dt between '2021-07-22' and '2021-07-22'
    group by shop_id, buyer_id, last_time
) b
                   on a.shop_id = b.shop_id and a.buyer_id = b.buyer_id
group by a.shop_id;
-- 地域分布-TC
select count(distinct case when paid = 2 and buyerId != 0 then buyerId end) as userNumber,
       sum(case when paid = 1 then 0 else 1 end)                            as orderNumber,
       sum(case when paid = 1 then 0 else totalMoney - discountMoney end)   as money
from default.dwd_order_all final
where shopId = 2000000027
          and day = cast(toDate(now()) as String);

select a.pv                                                       as "浏览量",
       a.uv                                                       as "访客数",
       case
           when b.hour is null or b.hour = '' then a.hour
           when a.hour is null or a.hour = '' then b.hour
           when a.hour is not null and b.hour is not null then a.hour
           end                                                    as hour,
       b.orderNumber                                              as "订单量",
       b.money                                                    as "支付金额",
       b.userNumber                                               as "订单用户",
       if(isNaN(toFloat64(b.money) / toFloat64(b.userNumber)), 0,
          round(toFloat64(b.money) / toFloat64(b.userNumber), 3)) as "客单价",
       b.userNumber / a.uv                                        as "访客-支付转换率"
from (select count()                                                                                         pv,
    count(distinct case when loginToken == '' or loginToken is null then ip else loginToken end) as uv,
    concat(hour, ':00')                                                                          as hour
    from default.dwd_page_log_all
    where shopId = '2000000027'
    and day = cast(toDate(now()) as String)
    and hour >= '00'
    group by hour
    order by hour desc) a
    all
    full join
    ( select count(distinct multiIf(paid = 1, null, buyerId))      as userNumber,
    sum(multiIf(paid = 1, 0, 1))                          as orderNumber,
    sum(multiIf(paid = 1, 0, totalMoney - discountMoney)) as money,
    concat(hour, ':00')                                   as hour
    from default.dwd_order_all final
    where shopId = cast('2000000027' as Int64)
    and day = cast(toDate(now()) as String)
    and hour >= '00'
    group by hour
    ) b
on a.hour = b.hour
order by hour;


-- 实时概况
select a.pv          as "浏览量",
       a.uv          as "访客数",
       case
           when b.hour is null or b.hour = '' then a.hour
           when a.hour is null or a.hour = '' then b.hour
           when a.hour is not null and b.hour is not null then a.hour
           end       as hour,
       b.orderNumber as "订单量",
       b.money       as "交易金额",
       b.userNumber  as "订单用户"
from (select count()                                                                                         pv,
    count(distinct case when loginToken == '' or loginToken is null then ip else loginToken end) as uv,
    concat(hour, ':00')                                                                          as hour
    from default.dwd_page_log_all
    where shopId = '2000000027'
    and day = cast(toDate(now()) as String)
    and hour >= '00'
    group by hour) a
    full join
    (
    select count(distinct case when paid = 2 and buyerId != 0 then buyerId end) as userNumber,
    sum(case when paid = 1 then 0 else 1 end)                            as orderNumber,
    sum(case when paid = 1 then 0 else totalMoney - discountMoney end)   as money,
    concat(hour, ':00')                                                  AS hour
    from default.dwd_order_all final
    where shopId = 2000000027
    and day = cast(toDate(now()) as String)
    group by hour
    ) b
on a.hour = b.hour
order by hour;
--实时概况-pv,uv,金额
select b.pv          as "浏览量",
       b.uv          as "访客数",
       a.orderNumber as "订单量",
       a.money       as "交易金额",
       a.userNumber  as "订单用户"
from (
         select shopId,
                count(distinct case when paid = 2 and buyerId != 0 then buyerId end) as userNumber,
                sum(case when paid = 1 then 0 else 1 end)                            as orderNumber,
                sum(case when paid = 1 then 0 else totalMoney - discountMoney end)   as money
         from default.dwd_order_all final
         where shopId = 2000000027
                   and day = cast(toDate(now()) as String)
         group by shopId
     ) a
    all
         full join (
    select count()                                                                                         pv,
           count(distinct case when loginToken == '' or loginToken is null then ip else loginToken end) as uv,
           shopId
    from default.dwd_page_log_all
    where shopId = '2000000027'
      and day = cast(toDate(now()) as String)
    group by shopId
    ) b
on a.shopId = cast(b.shopId as Int64);
--实时概况-昨日信息
select a.pv          as "浏览量",
       a.uv          as "访客数",
       case
           when b.hour is null or b.hour = '' then a.hour
           when a.hour is null or a.hour = '' then b.hour
           when a.hour is not null and b.hour is not null then a.hour
           end       as hour,
       b.orderNumber as "订单量",
       b.money       as "交易金额",
       b.userNumber  as "订单用户"
from (select count()                                                                                         pv,
    count(distinct case when loginToken == '' or loginToken is null then ip else loginToken end) as uv,
    concat(hour, ':00')                                                                          as hour
    from default.dwd_page_log_all
    where shopId = '2000000027'
    and day = cast(timestamp_sub(DAY, 1, toDate(now())) as String)
    and hour >= '00'
    group by hour) a
    full join
    (
    select count(distinct case when paid = 2 and buyerId != 0 then buyerId end) as userNumber,
    sum(case when paid = 1 then 0 else 1 end)                            as orderNumber,
    sum(case when paid = 1 then 0 else totalMoney - discountMoney end)   as money,
    concat(hour, ':00')                                                  AS hour
    from default.dwd_order_all final
    where shopId = 2000000027
    and day = cast(timestamp_sub(DAY, 1, toDate(now())) as String)
    and hour >= '00'
    group by hour
    ) b
on a.hour = b.hour
order by hour;
-- 实时概况-核心指标-图表
select case
           when b.day == '' then a.day
           else b.day end                                     as day,
       pv                                                     as "浏览量",
       uv                                                     as "访客数",
       userNumber                                             as "订单用户",
       orderNumber                                            as "订单量",
       money                                                  as "支付金额",
       if(isNaN(toFloat64(money) / toFloat64(userNumber)), 0,
          round(toFloat64(money) / toFloat64(userNumber), 3)) as "客单价",
       case
           when userNumber = 0 then 0
           when uv = 0 then 0
           else round(CAST(ifNull(userNumber, 0) as Int64) / CAST(ifNull(uv, 0) as Int64), 2) end
                                                              as "访客-支付转换率"
from (
         select count()                                                                                         pv,
                count(distinct case when loginToken == '' or loginToken is null then ip else loginToken end) as uv,
                day
         from default.dwd_page_log_all
         where shopId = '2000000027'
           and day between '2021-07-19' and '2021-07-25'
         group by day
         ) a
         all
         full join (
    select count(distinct case when paid = 2 and buyerId != 0 then buyerId end) as userNumber,
           sum(case when paid = 1 then 0 else 1 end)                            as orderNumber,
           sum(case when paid = 1 then 0 else totalMoney - discountMoney end)   as money,
           day
    from default.dwd_order_all final
    where shopId = 2000000027
      and day between '2021-07-19' and '2021-07-15'
    group by day
    ) b
                   on a.day = b.day
order by a.day;
--实时概况-流量看板-all
select intDivOrZero(all_time, uv)          as "平均停留时长",
       intDivOrZero(pv, uv)                as "人均浏览量",
       intDivOrZero(order_user_number, uv) as "跳失率",
       dt
from (
         select dt,
                count(distinct if(loginToken is null or loginToken = '', ip, loginToken)) as uv,
                sum(time)                                                                 as all_time,
                sum(pv)                                                                   as pv
         from dws_shop_clicklog_info
         where shop_id = '2000000027'
           and order_type = 'H5'
           and dt between '2021-07-19' and '2021-07-19'
         group by dt
     ) a
         left join(
    select dt,
           count(distinct buyer_id) as order_user_number
    from dws_shop_deal_info
    where shop_id = '2000000027'
      and order_type = 'ALL'
      and dt between '2021-07-19' and '2021-07-19'
    group by dt ) b
                  on a.dt = b.dt;
-- 实时概览-B端客户
select dt,
       sum(all_client_user_number) as "累计订阅人数",
       sum(new_client_user_number) as "新增订阅人数",
       0                           as "访问客户数"
from shop_client_vip_info
where shop_id = '2000000027'
  and dt between '2021-07-19' and '2021-07-19'
group by dt;
-- 实时概览-会员
select sum(all_vip_user_number) as "累计客户数",
       sum(new_vip_user_number) as "静增客户数",
       0                        as "访问客户数"
from shop_client_vip_info
where shop_id = '2000000027'
  and dt between '2021-07-19' and '2021-07-19'
group by dt;
-- 单品分析-商品信息-all-(包含图片)
select sku_id,
       `商品信息`,
       `成本价`,
       `单价`,
       `浏览量`,
       `访客数`,
       `商品名称`,
       `支付件数`,
       `成交的下单用户数`,
       `支付金额`,
       `总下单用户数`
from (select sku_id,
             sku_pic_url                as "商品信息",
             cost_price                 as "成本价",
             payment_price              as "单价",
             item_name                  as "商品名称",
             sum(payment_num)           as "支付件数",
             sum(payment_succeed_money) as "支付金额",
             sum(sale_order_number)     as "总下单用户数"
      from dws_shop_sku_info
      where shop_id = '2000000027'
        and dt between '2021-07-20' and '2021-07-20'
        and order_type = 'ALL'
      group by sku_id, sku_pic_url, cost_price, payment_price, item_name) a
         left join
     (select sku_id,
             count(distinct case when paid = 2 and refund = 0 then buyer_id end) as "成交的下单用户数"
      from dws_shop_deal_info
      where shop_id = '2000000027'
        and dt between '2021-07-19' and '2021-07-19'
        and order_type = 'ALL'
      group by sku_id) b
     on a.sku_id = b.sku_id
         left join (
    select sku_id,
           count(distinct if(loginToken is null or loginToken = '', ip, loginToken)) as "访客数",
           sum(pv)                                                                      "浏览量"
    from dws_shop_clicklog_info
    where shop_id = '2000000027'
      and dt between '2021-07-19' and '2021-07-19'
    group by sku_id
) c on a.sku_id = c.sku_id;
-- 单品分析-销售分析-商品详情 销售分析
select dt,
       `支付金额`,
       `访客数`,
       intDivOrZero(`成交的下单用户数`, `访客数`) as "访客转化率"
from (select dt,
             sku_id,
             sum(payment_succeed_money) as "支付金额"
      from dws_shop_sku_info
      where shop_id = '2000000027'
        and dt between '2021-07-19' and '2021-07-19'
        and order_type = 'ALL'
        and sku_id = '2000000032'
      group by dt, sku_id, sku_pic_url, cost_price, item_name) a
         left join
     (select dt,
             sku_id,
             count(distinct case when paid = 2 and refund = 0 then buyer_id end) as "成交的下单用户数"
      from dws_shop_deal_info
      where shop_id = '2000000027'
        and dt between '2021-07-19' and '2021-07-19'
        and order_type = 'ALL'
        and sku_id = '2000000032'
      group by dt, sku_id) b
     on a.sku_id = b.sku_id and a.dt = b.dt
         left join (
    select dt,
           sku_id,
           count(distinct if(loginToken is null or loginToken = '', ip, loginToken)) as "访客数"
    from dws_shop_clicklog_info
    where shop_id = '2000000027'
      and dt between '2021-07-19' and '2021-07-19'
      and sku_id = '2000000032'
    group by dt, sku_id
) c on a.sku_id = c.sku_id and a.dt = c.dt;
-- 流量分析
select dt,
       count(distinct if(loginToken is null or loginToken = '', ip, loginToken))                             as "访客数",
       intDivOrZero(max(last_access_page), count(distinct
                                                 if(loginToken is null or loginToken = '', ip, loginToken))) as "跳失率",
       intDivOrZero(sum(time), count(distinct
                                     if(loginToken is null or loginToken = '', ip, loginToken)))             as "平均停留时长"
from dws_shop_clicklog_info
where shop_id = '2000000027'
  and dt between '2021-07-19' and '2021-07-19'
  and sku_id = '2000000033'
  and order_type = 'ALL'
group by dt;
-- 单品分析-详情-销售趋势分析
select sku_id,
       `商品信息`,
       `成本价`,
       `单价`,
       `浏览量`,
       `访客数`,
       `商品名称`,
       `支付件数`,
       `成交的下单用户数`,
       `支付金额`,
       intDivOrZero(`成交的下单用户数`, `访客数`) as "访客转化率",
       `总下单用户数`
from (select sku_id,
             sku_pic_url                as "商品信息",
             cost_price                 as "成本价",
             payment_price              as "单价",
             item_name                  as "商品名称",
             sum(payment_num)           as "支付件数",
             sum(payment_succeed_money) as "支付金额",
             sum(sale_order_number)     as "总下单用户数"
      from dws_shop_sku_info
      where shop_id = '2000000027'
        and dt between '2021-07-19' and '2021-07-19'
        and sku_id = ''
        and order_type = 'ALL'
      group by sku_id, sku_pic_url, cost_price, payment_price, item_name) a
         left join
     (select sku_id,
             count(distinct case when paid = 2 and refund = 0 then buyer_id end) as "成交的下单用户数"
      from dws_shop_deal_info
      where shop_id = '2000000027'
        and dt between '2021-07-19' and '2021-07-19'
        and sku_id = ''
        and order_type = 'ALL'
      group by sku_id) b
     on a.sku_id = b.sku_id
         left join (
    select sku_id,
           count(distinct if(loginToken is null or loginToken = '', ip, loginToken)) as "访客数",
           sum(pv)                                                                      "浏览量"
    from dws_shop_clicklog_info
    where shop_id = '2000000027'
      and dt between '2021-07-19' and '2021-07-19'
      and sku_id = ''
    group by sku_id
) c on a.sku_id = c.sku_id;
-- 单品分析-商品渠道构成
select sku_id,
       order_type                  as `渠道类型`,
       `浏览量`,
       `访客数`,
       `商品曝光`,
       0                           as "加购人数",
       0                           as "加购件数",
       `支付件数`,
       `支付人数`,
       `支付金额`,
       intDivOrZero(`支付人数`, `访客数`) as "支付转换率"
from (select sku_id,
             cost_price                 as "成本价",
             payment_price              as "单价",
             sum(payment_num)           as "支付件数",
             sum(payment_succeed_money) as "支付金额",
             sum(sale_order_number)     as "总下单用户数"
      from dws_shop_sku_info
      where shop_id = '2000000027'
        and dt between '2021-07-19' and '2021-07-19'
        and sku_id = '2000000032'
        and order_type = 'ALL'
      group by sku_id, order_type, cost_price, payment_price) a
         left join
     (select sku_id,
             count(distinct case when paid = 2 and refund = 0 then buyer_id end) as "支付人数"
      from dws_shop_deal_info
      where shop_id = '2000000027'
        and dt between '2021-07-19' and '2021-07-19'
        and sku_id = '2000000032'
        and order_type = 'ALL'
      group by sku_id) b
     on a.sku_id = b.sku_id
         left join (
    select sku_id,
           count(distinct if(loginToken is null or loginToken = '', ip, loginToken)) as "访客数",
           count(distinct if(loginToken is null or loginToken = '', ip, loginToken)) as "商品曝光",
           sum(pv)                                                                      "浏览量",
           order_type
    from dws_shop_clicklog_info
    where shop_id = '2000000027'
      and dt between '2021-07-19' and '2021-07-19'
      and sku_id = '2000000032'
    group by sku_id, order_type
) c on a.sku_id = c.sku_id;
-- -----------------------交易分析-整体看板-all
select dt,
       count(distinct buyer_id)                                            as "下单人数",
       sum(payment_money)                                                  as "下单金额",
       count(distinct case when paid = 2 and refund = 0 then buyer_id end) as "成交客户数",
       count(case when paid = 2 and refund = 0 then 1 end)                 as "成交客单量",
       sum(1)                                                              as "下单笔数",
       sum(payment_succeed_money)                                          as "成交金额",
       intDivOrZero(sum(payment_succeed_money), count(distinct buyer_id))  as "客单价"
from dws_shop_deal_info
where shop_id = '2000000079'
  and order_type = 'ALL'
  and dt between 20210713 and 20210713
group by dt;
-- 交易分析-退款商品-all
select item_name                                             as "商品名称",
       refund_reason_number                                  as "总退款笔数",
       refund_money                                          as "成功退款金额",
       payment_money                                         as "订单金额",
       refund_number                                         as "成功退款数量",
       intDivOrZero(refund_number, orders_succeed_number)    as "退款率",
       intDivOrZero(refund_sku_reason_number, refund_number) as "退款原因比",
       dt
from (
         select dt,
                shop_id,
                sku_id,
                item_name,
                sum(refund_reason_number)        refund_reason_number,
                sum(refund_number)            as refund_number,
                sum(refund_money)             as refund_money,
                count(distinct refund_reason) as refund_sku_reason_number
         from dws_shop_deal_refund_info
         where shop_id = '2000000027'
           and order_type = 'ALL'
           and dt between '2021-07-19' and '2021-07-19'
         group by dt, shop_id, sku_id, item_name
     ) a
         left join (

    select dt,
           shop_id,
           sku_id,
           count(distinct buyer_id)                            as "下单人数",
           sum(payment_money)                                  as payment_money,
           count(case when paid = 2 and refund = 0 then 1 end) as orders_succeed_number
    from dws_shop_deal_info
    where shop_id = '2000000027'
      and order_type = 'ALL'
      and dt between '2021-07-19' and '2021-07-19'
    group by dt, shop_id, sku_id
) b
                   on a.dt = b.dt and a.shop_id = b.shop_id and a.sku_id = b.sku_id;
-- 交易分析-退款原因-all
select refund_reason                       as "退货原因",
       refund_reason_number                as "总退款笔数",
       refund_money                        as "成功退款金额",
       refund_number                       as "成功退款笔数",
       divide(refund_money, b.all_monery)  as "退款金额比",
       divide(refund_number, b.all_number) as "退款笔数比",
       dt
from (
         select shop_id,
                dt,
                refund_reason,
                sum(refund_reason_number) as refund_reason_number,
                sum(refund_money)         as refund_money,
                sum(refund_number)        as refund_number
         from dws_shop_deal_refund_info
         where shop_id = '2000000027'
           and order_type = 'ALL'
           and dt between '2021-07-19' and '2021-07-19'
         group by shop_id, dt, refund_reason
     ) a
         left join (
    select shop_id,
           dt,
           count(1)          as all_number,
           sum(refund_money) as all_monery
    from dws_shop_deal_refund_info
    where shop_id = '2000000027'
      and order_type = 'ALL'
      and dt between '2021-07-19' and '2021-07-19'
    group by shop_id, dt) b
                   on a.shop_id = b.shop_id and a.dt = b.dt;
--交易分析-退款看板
select payment_succeed_number                       as "支付成功数",
       avg_time                                     as "退款平均处理时间(分)",
       refund_money                                 as "成功退款金额",
       refund_number                                as "成功退款数量",
       divide(refund_number, orders_succeed_number) as "退款率",
       dt
from (
         select dt,
                shop_id,
                sum(refund_reason_number)        refund_reason_number,
                sum(refund_number)            as refund_number,
                sum(refund_money)             as refund_money,
                sum(avg_time) / 60            as avg_time,
                count(distinct refund_reason) as refund_sku_reason_number
         from dws_shop_deal_refund_info
         where shop_id = '2000000027'
           and order_type = 'ALL'
           and dt between '2021-07-19' and '2021-07-19'
         group by dt, shop_id
     ) a
         left join (

    select dt,
           shop_id,
           sum(payment_succeed_number)                         as payment_succeed_number,
           count(case when paid = 2 and refund = 0 then 1 end) as orders_succeed_number
    from dws_shop_deal_info
    where shop_id = '2000000027'
      and order_type = 'ALL'
      and dt between '2021-07-19' and '2021-07-19'
    group by dt, shop_id
) b
                   on a.dt = b.dt and a.shop_id = b.shop_id;

-- 交易分析-交易概况-访客
-- select uv                    as 访客数
-- from shop_clicklog_info
-- where shop_id=#{shop_id} and dt between #{start_time} and #{end_time} and page_source= 'all'
select dt,
       count(distinct if(loginToken is null or loginToken = '', ip, loginToken)) as "访客数"
from dws_shop_clicklog_info
where shop_id = '2000000027'
  and dt between '2021-07-19' and '2021-07-19'
group by dt;
--     交易分析-交易概况-下单
select dt,
       count(distinct buyer_id) as "下单人数",
       count(1)                 as "下单笔数",
       sum(payment_money)       as "下单金额"
from dws_shop_deal_info
where shop_id = '2000000027'
  and order_type = 'ALL'
  and dt between '2021-07-19' and '2021-07-19'
group by dt, shop_id;
--     交易分析-交易概况-支付
select dt,
       shop_id,
       count(distinct case when paid = 2 and refund = 0 then buyer_id end) as "支付人数",
       sum(payment_succeed_number)                                         as "支付订单数",
       sum(payment_succeed_money)                                          as "支付金额",
       divide(sum(payment_succeed_money), count(distinct buyer_id))        as "客单价"
from dws_shop_deal_info
where shop_id = '2000000027'
  and order_type = 'ALL'
  and dt between '2021-07-19' and '2021-07-19'
group by dt;
--  交易分析-地域分布
select dt,
       province_name                                       as "省份",
       payment_buyer_number                                as "支付人数",
       payment_succeed_money                               as "支付金额",
       divide(payment_succeed_money, total_province_money) as "支付比例"
from (select dt,
             province_name,
             count(distinct case when paid = 2 and refund = 0 then buyer_id end) as payment_buyer_number,
             sum(payment_succeed_money)                                          as payment_succeed_money
      from dws_shop_deal_info
      where shop_id = '2000000027'
        and order_type = 'ALL'
        and dt between '2021-07-19' and '2021-07-19'
      group by dt, province_name) a
         left join
     (select dt,
             sum(payment_succeed_money) as total_province_money
      from dws_shop_deal_info
      where shop_id = '2000000027'
        and order_type = 'ALL'
        and dt between '2021-07-19' and '2021-07-19'
      group by dt) b
     on a.dt = b.dt;
-- --------------------------------商品概况
--     商品分析-商品整体概览-商品转换
select a.item_number        as "在架商品数",
       c.access_item_number as "被访问商品数",
       b.sale_number        as "动销商品数"
from (
         select dt,
                max(item_number) as item_number
         from dws_shop_item_info
         where shop_id = '2000000027'
           and dt between '2021-07-19' and '2021-07-19'
         group by dt
     ) a
         left join (
    select dt,
           count(distinct case when paid = 2 then sku_id end) as sale_number
    from dws_shop_deal_info
    where shop_id = '2000000027'
      and order_type = 'ALL'
      and dt between '2021-07-19' and '2021-07-19'
    group by dt
) b
                   on a.dt = b.dt
         left join (
    select dt,
           count(distinct sku_id) as access_item_number
    from dws_shop_clicklog_info
    where shop_id = '2000000027'
      and dt between '2021-07-19' and '2021-07-19'
    group by dt
) c
                   on a.dt = c.dt;
--商品分析-商品整体概览-商品流量
select 0                      as "商品曝光数",
       count(sku_id)          as "商品浏览量",
       count(distinct sku_id) as "商品访客数"
from dws_shop_clicklog_info
where shop_id = '2000000027'
  and dt between '2021-07-19' and '2021-07-19'
group by dt;
--商品分析-商品整体概览-商品概览
select 0                           as "加购件数",
       sum(payment_num)            as "下单件数",
       sum(payment_succeed_number) as "支付件数"
from dws_shop_deal_info
where shop_id = '2000000027'
  and dt between '2021-07-19' and '2021-07-19'
  and order_type = 'ALL';
-- 商品分析-商品趋势分析
select b.item_number          as "在架商品数",
       sale_number            as "动销商品品种数",
       buyer_number           as "下单客户数",
       sale_order_number      as "下单笔数",
       payment_num            as "下单商品件数",
       payment_succeed_number as "成交商品件数",
       payment_succeed_money  as "成交金额",
       goods_transform_ratio  as "商品转换率",
       a.dt
from (
         select dt,
                count(distinct case when paid = 2 then sku_id end) as sale_number,
                count(distinct buyer_id)                           as buyer_number,
                sum(sale_order_number)                             as sale_order_number,
                sum(payment_num)                                   as payment_num,
                sum(payment_succeed_number)                        as payment_succeed_number,
                sum(payment_succeed_money)                         as payment_succeed_money,
                0.0                                                as goods_transform_ratio
         from dws_shop_deal_info
         where shop_id = '2000000027'
           and dt between '2021-07-19' and '2021-07-19'
           and order_type = 'ALL'
         group by dt
     ) a
         left join
     (
         select dt,
                max(item_number) as item_number
         from dws_shop_item_info
         where shop_id = '2000000027'
           and dt between '2021-07-19' and '2021-07-19'
         group by dt
     ) b
     on a.dt = b.dt;
-- 商品分析-支付金额TOP 10
select item_name                                                as "商品名称",
       paid_buyer                                               as "支付人数",
       a.payment_succeed_money                                  as "统计支付金额",
       divide(a.payment_succeed_money, b.payment_succeed_money) as "支付金额占总支付金额比例"
from (
         select dt,
                item_name,
                count(distinct case when paid = 2 then buyer_id end) as paid_buyer,
                sum(payment_succeed_money)                           as payment_succeed_money
         from dws_shop_deal_info
         where shop_id = '2000000027'
           and dt between '2021-07-19' and '2021-07-19'
           and order_type = 'ALL'
         group by dt, sku_id, item_name
     ) a
         left join (
    select dt,
           sum(payment_succeed_money) as payment_succeed_money
    from dws_shop_deal_info
    where shop_id = '2000000027'
      and dt between '2021-07-19' and '2021-07-19'
      and order_type = 'ALL'
    group by dt
) b
                   on a.dt = b.dt
order by a.payment_succeed_money desc
    limit 10;
-- 商品分析-支付利润TOP 10
select item_name                              as "商品名称",
       paid_buyer                             as "支付人数",
       a.income_money                         as "统计支付金额",
       divide(a.income_money, b.income_money) as "支付金额占总支付金额比例"
from (
         select dt,
                item_name,
                count(distinct case when paid = 2 then buyer_id end) as paid_buyer,
                sum(income_money)                                    as income_money
         from dws_shop_deal_info
         where shop_id = '2000000027'
           and dt between '2021-07-19' and '2021-07-19'
           and order_type = 'ALL'
         group by dt, sku_id, item_name
     ) a
         left join (
    select dt,
           sum(income_money) as income_money
    from dws_shop_deal_info
    where shop_id = '2000000027'
      and dt between '2021-07-19' and '2021-07-19'
      and order_type = 'ALL'
    group by dt
) b
                   on a.dt = b.dt
order by a.income_money desc
    limit 10;
-- 商品分析-采购商品统计-商品
select dt,
       item_name          as "商品名称",
       sum(payment_num)   as "采购数量",
       sum(payment_money) as "采购金额"
from dws_shop_deal_info
where shop_id = '2000000282'
  and dt between '2021-07-19' and '2021-07-19'
  and order_type = 'ALL'
  and po_type = 'PO'
group by dt, sku_id, item_name;
--     商品分析-采购商品统计-供应商
select dt,
       seller_name        as "供应商名称",
       sum(payment_num)   as "采购数量",
       sum(payment_money) as "采购金额"
from dws_shop_deal_info
where shop_id = '2000000282'
  and dt between '2021-07-20' and '2021-07-20'
  and order_type = 'ALL'
  and po_type = 'PO'
group by dt, seller_name;
-- 商品分析-采购商品统计-类目
select dt,
       cat_3d_name        as "类目名称",
       sum(num)           as "采购数量",
       sum(payment_money) as "采购金额"
from dws_shop_deal_info
where shop_id = '2000000282'
  and dt between '2021-07-20' and '2021-07-20'
  and order_type = 'ALL'
  and po_type = 'PO'
group by dt, cat_3d_name;
--     仓库分析-出入库统计-商品折线图
-- select business_name as 商品名称, real_inbound_num as 入库数量, real_outbound_num as 出库数量
-- from shop_warehouse_inout
-- where shop_id=#{shop_id} and dt between #{start_time} and #{end_time} and business_type = '1'
select item_name         as "商品名称",
       sum(inbound_num)  as "入库数量",
       sum(outbound_num) as "出库数量"
from dws_shop_warehouse_inout
where shop_id = '2000000027'
  and dt between '2021-07-20' and '2021-07-20'
  and types = 'item'
group by item_name;
--     仓库分析-出入库统计-仓库折线图
-- select business_name as 商品名称, real_inbound_num as 入库数量, real_outbound_num as 出库数量
-- from shop_warehouse_inout
-- where shop_id=#{shop_id} and dt between #{start_time} and #{end_time} and business_type = '2'
select warehouse_name    as "仓库名称",
       sum(inbound_num)  as "入库数量",
       sum(outbound_num) as "出库数量"
from dws_shop_warehouse_inout
where shop_id = '2000000027'
  and dt between '2021-07-20' and '2021-07-20'
  and types = 'warehouse'
group by warehouse_name;
--     仓库分析-出入库统计-商品详细数据
-- select business_name as 商品名称, real_inbound_num as 入库数量, real_outbound_num as 出库数量, dt as 时间
-- from shop_warehouse_inout
-- where shop_id=#{shop_id} and dt between #{start_time} and #{end_time} and  business_type = '1'
select item_name         as "商品名称",
       sum(inbound_num)  as "入库数量",
       sum(outbound_num) as "出库数量",
       dt
from dws_shop_warehouse_inout
where shop_id = '2000000027'
  and dt between '2021-07-20' and '2021-07-20'
  and types = 'item'
group by dt, item_name;

-- 仓库分析-出入库统计-仓库详细数据
-- select business_name as 商品名称, real_inbound_num as 入库数量, real_outbound_num as 出库数量, dt as 时间
-- from shop_warehouse_inout
-- where shop_id=#{shop_id} and dt between #{start_time} and #{end_time} and business_type = '2'
select warehouse_name    as "仓库名称",
       sum(inbound_num)  as "入库数量",
       sum(outbound_num) as "出库数量",
       dt
from dws_shop_warehouse_inout
where shop_id = '2000000027'
  and dt between '2021-07-20' and '2021-07-20'
  and types = 'warehouse'
group by dt, warehouse_name;
-- 仓库分析-库存成本统计-商品折线图
select item_name        as "商品名称",
       sum(inbound_num) as "库存",
       sum(total_money) as "金额"
from dws_shop_inbound_bill_record
where shop_id = '2000000027'
  and dt between '2021-07-21' and '2021-07-21'
group by item_name;
--     仓库分析-库存成本统计-仓库折线图
select warehouse_name   as "仓库名称",
       sum(inbound_num) as "库存",
       sum(total_money) as "金额"
from dws_shop_inbound_bill_record
where shop_id = '2000000027'
  and dt between '2021-07-21' and '2021-07-21'
group by warehouse_code, warehouse_name;
--     仓库分析-库存成本统计-品牌折线图
select brand_name       as "品牌名称",
       sum(inbound_num) as "库存",
       sum(total_money) as "金额"
from dws_shop_inbound_bill_record
where shop_id = '2000000027'
  and dt between '2021-07-21' and '2021-07-21'
group by brand_id, brand_name;
--     仓库分析-库存成本统计-商品
select sku_code         as "sku编码",
       item_name        as "商品名称",
       sum(inbound_num) as "库存",
       sum(total_money) as "金额",
       dt
from dws_shop_inbound_bill_record
where shop_id = '2000000027'
  and dt between '2021-07-21' and '2021-07-21'
group by dt, sku_code, item_name;
--     sku_code desc limit %s,%s
--     仓库分析-库存成本统计-仓库
select warehouse_code   as "仓库编码",
       warehouse_name   as "仓库名称",
       sum(inbound_num) as "库存",
       sum(total_money) as "金额",
       dt
from dws_shop_inbound_bill_record
where shop_id = '2000000027'
  and dt between '2021-07-21' and '2021-07-21'
group by dt, warehouse_code, warehouse_name;
-- warehouse_code desc limit %s,%s
--     仓库分析-库存成本统计-品牌
select brand_id         as "品牌编码",
       brand_name       as "品牌名称",
       sum(inbound_num) as "库存",
       sum(total_money) as "金额",
       dt
from dws_shop_inbound_bill_record
where shop_id = '2000000027'
  and dt between '2021-07-21' and '2021-07-21'
group by dt, brand_id, brand_name;
--     brand_id desc limit %s,%s
--     流量分析-浏览访问-all
select pv                               as "浏览量",
       uv                               as "访客数",
       new_user                         as "新访客数",
       divide(total_time, uv)           as "平均停留时长",
       divide(pv, uv)                   as "人均浏览量",
       divide(last_access_page, uv)     as "跳失率",
       payment_buyer_number             as "支付人数",
       divide(payment_buyer_number, uv) as "访问-支付转化率"
from (
         select dt,
                sum(pv)                                                                                      as pv,
                count(distinct case when loginToken == '' or loginToken is null then ip else loginToken end) as uv,
                count(distinct user_id)                                                                      as new_user,
                max(last_access_page)                                                                        as last_access_page,
                sum(time)                                                                                    as total_time
         from dws_shop_clicklog_info
         where shop_id = '2000000027'
           and dt between '2021-07-21' and '2021-07-21'
         group by dt
     ) a
         left join
     (
         select dt,
                count(distinct case when paid = 2 and refund = 0 then buyer_id end) as payment_buyer_number,
                count(distinct buyer_id)                                            as order_user_number
         from dws_shop_deal_info
         where shop_id = '2000000027'
           and order_type = 'ALL'
           and dt between '2021-07-21' and '2021-07-21'
         group by dt
     ) b
     on a.dt = b.dt;

--     流量分析-流量趋势-all
select dt,
       pv                                         as "浏览量",
       uv                                         as "访客数",
       new_user                                   as "新访客数",
       round(divide(total_time, uv), 2)           as "平均停留时长",
       round(divide(pv, uv), 2)                   as "人均浏览量",
       round(divide(last_access_page, uv), 2)     as "跳失率",
       payment_buyer_number                       as "支付人数",
       round(divide(payment_buyer_number, uv), 2) as "访问-支付转化率"
from (
         select dt,
                sum(pv)                                                                                      as pv,
                count(distinct case when loginToken == '' or loginToken is null then ip else loginToken end) as uv,
                count(distinct user_id)                                                                      as new_user,
                max(last_access_page)                                                                        as last_access_page,
                sum(time)                                                                                    as total_time
         from dws_shop_clicklog_info
         where shop_id = '2000000027'
           and dt between '2021-07-19' and '2021-07-19'
         group by dt
     ) a
         left join
     (
         select dt,
                count(distinct case when paid = 2 and refund = 0 then buyer_id end) as payment_buyer_number,
                count(distinct buyer_id)                                            as order_user_number
         from dws_shop_deal_info
         where shop_id = '2000000027'
           and order_type = 'ALL'
           and dt between '2021-07-19' and '2021-07-19'
         group by dt
     ) b
     on a.dt = b.dt;



select a.itemName                           as "商品名称",
       a.uv                                 as "商品访客数",
       round(b.order_user_number / a.uv, 2) as "访问-支付转换率"
from (
         select itemId, itemName, uv
         from (
                  select itemId,
                         itemName,
                         count(distinct
                               case when loginToken == '' or loginToken is null then ip else loginToken end) as uv
                  from default.dwd_page_log_all
                  where shopId = '2000000027'
                            and day = cast(toDate(now()) as String)
                    and event = 'page_H5_goods'
                  group by itemId, itemName
              ) a
         order by uv desc
     ) a
         left join
     (
         select itemId, count(distinct buyerId) as order_user_number
         from (
                  select a.itemId, b.buyerId
                  from (
                           select *
                           from dwd_order_detail_all
                           where shopId = cast('2000000027' as Int64)
                                     and day = cast(toDate(now()) as String)
                       ) a
                           left join
                       (select orderId, buyerId
                        from dwd_order_all
                        where shopId = cast('2000000027' as Int64)
                                  and day = cast(toDate(now()) as String)
                        group by orderId, buyerId ) b
                       on a.orderId = b.orderId
              )
         group by itemId
     ) b
     on cast(a.itemId as Int64) = b.itemId
    limit 5;



select a.buyer_id,
       a.paid,
       a.refund,
       b.last_time,
       a.paid,
--        ,
--       count(distinct case when a.paid = 2 and a.refund = 0 and a.buyer_id != '' then a.buyer_id end)     as "成交用户数"
--        count(distinct case
--                           when b.last_time != '' and a.paid = 2 and a.refund = 0 and a.buyer_id is not null and
--                                a.buyer_id != '' then a.buyer_id end)                 as "成交老用户数"
--         count(distinct case
--                           when b.last_time = '' and a.paid = 2 and a.refund = 0 and a.buyer_id is not null and
--                                a.buyer_id != '' then a.buyer_id end)                 as "成交新用户数"
       case
--
           when b.last_time = '' and a.paid = 2 and a.refund = 0 then a.buyer_id end as "da"
from (
         select shop_id,
                buyer_id,
                paid,
                refund,
                sum(payment_succeed_money) as payment_succeed_money
         from dws_shop_deal_info
         where shop_id = '2000000027'
           and order_type = 'ALL'
           and dt between '2021-07-22' and '2021-07-22'
         group by shop_id, buyer_id, paid, refund) a
         left join (
    select shop_id, buyer_id, last_time
    from dws_shop_client_info
    where shop_id = '2000000027'
      and dt between '2021-07-22' and '2021-07-22'
    group by shop_id, buyer_id, last_time
) b
                   on a.shop_id = b.shop_id and a.buyer_id = b.buyer_id;