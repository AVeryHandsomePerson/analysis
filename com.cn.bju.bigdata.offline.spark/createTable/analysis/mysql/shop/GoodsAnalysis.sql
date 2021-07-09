-- auto-generated definition
create table shop_goods_money_top
(
    shop_id            bigint       null,
    item_name          varchar(255) null,
    sale_user_count    bigint       null comment '支付人数',
    sale_succeed_money double       null comment '统计支付金额',
    sale_ratio         double       null comment '支付金额占总支付金额比例',
    dt                 date         not null
)
    comment '商品金额TOP 10' charset = utf8;
-- auto-generated definition
create table shop_goods_profit_top
(
    shop_id             bigint       null,
    item_name           varchar(255) null,
    sale_user_count     bigint       null comment '支付人数',
    sale_succeed_profit double       null comment '统计利润',
    sale_profit_ratio   double       null comment '统计利润比',
    dt                  date         not null
)
    comment '商品利润TOP 10' charset = utf8;
-- auto-generated definition
create table shop_goods_purchase_info
(
    id       int auto_increment comment 'id'
        primary key,
    shop_id  bigint       null,
    pu_type  int          null comment '类型区分',
    name     varchar(255) null,
    pu_num   varchar(255) null comment '采购数量',
    pu_money double       null comment '采购金额',
    dt       date         not null
)
    comment '商品采购数据统计' charset = utf8;
-- auto-generated definition
create table shop_goods_sale
(
    shop_id               bigint       null,
    item_number           double       null comment '在架商品数',
    shop_name             varchar(255) null,
    sale_number           bigint       null comment '动销商品品种数',
    orders_user_number    bigint       null comment '下单客户数',
    orders_number         bigint       null comment '下单笔数',
    sale_goods_number     bigint       null comment '下单商品件数',
    sale_succeed_number   bigint       null comment '成交商品件数',
    sale_succeed_money    double       null comment '成交金额',
    goods_transform_ratio double       null comment '商品转换率',
    dt                    date         not null
)
    comment '店铺-商品分析-商品销售' charset = utf8;
create index shop_goods_sale_dt_index
    on shop_goods_sale (dt);

create table shop_goods_money_top_month
(
    shop_id            bigint       null,
    item_name          varchar(255) null,
    sale_user_count    bigint       null comment '支付人数',
    sale_succeed_money double       null comment '统计支付金额',
    sale_ratio         double       null comment '支付金额占总支付金额比例',
    dt                 date         not null
)
    comment '商品金额TOP 10' charset = utf8;
-- auto-generated definition
create table shop_goods_profit_top_month
(
    shop_id             bigint       null,
    item_name           varchar(255) null,
    sale_user_count     bigint       null comment '支付人数',
    sale_succeed_profit double       null comment '统计利润',
    sale_profit_ratio   double       null comment '统计利润比',
    dt                  date         not null
)
    comment '商品利润TOP 10' charset = utf8;
-- auto-generated definition
create table shop_goods_purchase_info_month
(
    id       int auto_increment comment 'id'
        primary key,
    shop_id  bigint       null,
    pu_type  int          null comment '类型区分',
    name     varchar(255) null,
    pu_num   varchar(255) null comment '采购数量',
    pu_money double       null comment '采购金额',
    dt       date         not null
)
    comment '商品采购数据统计' charset = utf8;
-- auto-generated definition
create table shop_goods_sale_month
(
    shop_id               bigint       null,
    item_number           double       null comment '在架商品数',
    shop_name             varchar(255) null,
    sale_number           bigint       null comment '动销商品品种数',
    orders_user_number    bigint       null comment '下单客户数',
    orders_number         bigint       null comment '下单笔数',
    sale_goods_number     bigint       null comment '下单商品件数',
    sale_succeed_number   bigint       null comment '成交商品件数',
    sale_succeed_money    double       null comment '成交金额',
    goods_transform_ratio double       null comment '商品转换率',
    dt                    date         not null
)
    comment '店铺-商品分析-商品销售' charset = utf8;
create index shop_goods_sale_dt_index
    on shop_goods_sale_month (dt);