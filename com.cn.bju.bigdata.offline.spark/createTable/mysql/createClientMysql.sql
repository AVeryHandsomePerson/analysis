create table shop_client_analysis
(
    id                      int       primary key AUTO_INCREMENT ,
    shop_id                 bigint      null,
    order_type              varchar(50) null comment '平台类型',
    money                   double      null comment '客单价',
    sale_succeed_money      double      null comment '成交金额',
    new_user_succeed_money  double      null comment '新用户成交金额',
    aged_user_succeed_money double      null comment '老用户成交金额',
    user_dis_number         bigint      null comment '全部成交客户占比',
    present_user_dis_number int         null comment '当天划分平台成交的用户数',
    aged_user_dis_number    int         null comment '当天划分平台成交的老用户数',
    new_user_dis_number     int         null comment '成交的新用户数',
    type_user_ratio         double      null comment '成交客户占比',
    new_user_ratio          double      null comment '新成交客户占比',
    aged_user_ratio         double      null comment '老成交客户占比',
    dt                      date        not null
) comment '客户销售分析' charset = utf8;

create table shop_client_sale_top
(
    shop_id             bigint ,
    order_type          varchar(5)  comment '订单类型',
    user_name           varchar(100)  comment '用户名',
    sale_succeed_money  integer  comment '采购金额',
    sale_succeed_profit integer  comment '采购利润',
    dt                  date not null
) comment '客户采购排行榜' ENGINE=InnoDB DEFAULT CHARSET=utf8;

create table shop_deal_refund_info
(
    shop_id               bigint comment '商铺ID',
    shop_name            varchar(255) comment '商铺名称',
    order_type            varchar(5) comment '平台类型',
    orders_succeed_number bigint comment '支付成功数',
    avg_time              Double comment '退款平均处理时间',
    refund_money          Double comment '成功退款金额',
    refund_number         Double comment '成功退款笔数',
    refund_ratio          Double comment '退款率',
    dt                    date not null
) comment '退款指标' ENGINE=InnoDB DEFAULT CHARSET=utf8;


create table shop_deal_refund_reason
(
    shop_id              bigint comment '商铺ID',
    shop_name            varchar(255) comment '商铺名称',
    refund_reason        varchar(255) comment '退款原因',
    order_type           varchar(5) comment '平台类型',
    refund_reason_number bigint comment '总退款笔数',
    refund_money         Double comment '成功退款金额',
    refund_number        bigint comment '成功退款笔数',
    refund_number_ratio  Double comment '退款金额比',
    refund_money_ratio   Double comment '退款笔数比',
    dt                   date not null
) comment '退款理由指标' ENGINE=InnoDB DEFAULT CHARSET=utf8;

shop_id,
shop_name,
refund_reason,
order_type,
refund_reason_number,
refund_money,
refund_number,
refund_number_ratio,
refund_money_ratio,
$dt as dt




create table shop_deal_refund_sku
(
    shop_id              bigint comment '商铺ID',
    shop_name            varchar(255) comment '商铺名称',
    sku_id               bigint comment '商品ID',
    order_type           varchar(5) comment '平台类型',
    sku_name             varchar(255) comment '商品名称',
    refund_reason_number bigint comment '总退款笔数',
    refund_money         Double comment '成功退款金额',
    orders_succeed_money bigint comment '订单金额',
    refund_number        bigint comment '成功退款数量',
    refund_ratio         Double comment '退款率',
    refund_reason_ratio  Double comment '退款原因比',
    dt                   date not null
) comment '退款商品指标' ENGINE=InnoDB DEFAULT CHARSET=utf8;