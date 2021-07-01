-- auto-generated definition
create table shop_deal_refund_reason
(
    shop_id              bigint       null comment '商铺ID',
    shop_name            varchar(255) null comment '商铺名称',
    refund_reason        varchar(255) null comment '退款原因',
    order_type           varchar(5)   null comment '平台类型',
    refund_reason_number bigint       null comment '总退款笔数',
    refund_money         double       null comment '成功退款金额',
    refund_number        bigint       null comment '成功退款笔数',
    refund_number_ratio  double       null comment '退款金额比',
    refund_money_ratio   double       null comment '退款笔数比',
    dt                   date         not null
)
    comment '退款理由指标' charset = utf8;

-- auto-generated definition
create table shop_deal_refund_sku
(
    shop_id              bigint       null comment '商铺ID',
    shop_name            varchar(255) null comment '商铺名称',
    sku_id               bigint       null comment '商品ID',
    order_type           varchar(5)   null comment '平台类型',
    sku_name             varchar(255) null comment '商品名称',
    refund_reason_number bigint       null comment '总退款笔数',
    refund_money         double       null comment '成功退款金额',
    orders_succeed_money bigint       null comment '订单金额',
    refund_number        bigint       null comment '成功退款数量',
    refund_ratio         double       null comment '退款率',
    refund_reason_ratio  double       null comment '退款原因比',
    dt                   date         not null
)
    comment '退款商品指标' charset = utf8;

-- auto-generated definition
create table shop_deal_info
(
    shop_id               bigint         null,
    order_type            text           null,
    user_number           bigint         not null,
    sale_money            double         null,
    sale_user_number      bigint         not null,
    orders_succeed_number bigint         not null,
    sale_order_number     bigint         not null,
    sale_succeed_money    double         null,
    money                 decimal(12, 2) null,
    paid_num              double         null comment '支付件数',
    dt                    date           not null
);

-- auto-generated definition
create table shop_deal_refund_info
(
    shop_id               bigint       null comment '商铺ID',
    shop_name             varchar(255) null comment '商铺名称',
    order_type            varchar(5)   null comment '平台类型',
    orders_succeed_number bigint       null comment '支付成功数',
    avg_time              double       null comment '退款平均处理时间',
    refund_money          double       null comment '成功退款金额',
    refund_number         double       null comment '成功退款笔数',
    refund_ratio          double       null comment '退款率',
    dt                    date         not null
)
    comment '退款指标' charset = utf8;

