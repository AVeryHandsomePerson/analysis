create table goods_profit_top
(
    shop_id             bigint null,
    item_name           text null,
    sale_user_count     bigint not null,
    sale_succeed_profit double null,
    sale_profit_ratio   double null,
    dt                  date   not null
) comment '商品利润TOP 10' ENGINE=InnoDB DEFAULT CHARSET=utf8;

create table goods_money_top
(
    shop_id            bigint null,
    item_name          text null,
    sale_user_count    bigint not null,
    sale_succeed_money double null,
    sale_ratio         double null,
    dt                 date   not null
) comment '商品金额TOP 10' ENGINE=InnoDB DEFAULT CHARSET=utf8;

create table shop_goods_sale
(
    shop_id               bigint,
    item_number           double comment '在架商品数',
    shop_name             varchar(255),
    sale_number           bigint comment '动销商品品种数',
    orders_user_number    bigint comment '下单客户数',
    orders_number         bigint comment '下单笔数',
    sale_goods_number     bigint comment '下单商品件数',
    sale_succeed_number   bigint comment '成交商品件数',
    sale_succeed_money    double comment '成交金额',
    goods_transform_ratio double comment '商品转换率',
    dt                    date not null
)comment '店铺-商品分析-商品销售' ENGINE=InnoDB DEFAULT CHARSET=utf8;
-- create table place_orders_goods
-- (
--     shop_id           bigint null,
--     sale_goods_number double null,
--     dt                date    not null
-- )comment '下单商品件数' ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- create table place_orders_user
-- (
--     shop_id         bigint null,
--     sale_user_count bigint not null,
--     dt              date    not null
-- )comment '下单客户数' ENGINE=InnoDB DEFAULT CHARSET=utf8;

create table putaway_item
(
    shop_id     bigint null,
    item_number bigint not null,
    dt          date   not null
)comment '统计在架商品数据' ENGINE=InnoDB DEFAULT CHARSET=utf8;
create table trading_goods
(
    shop_id             bigint null,
    sale_succeed_number double null,
    dt                  date not null
)comment '成交商品件数' ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- auto-generated definition
create table putaway_goods
(
    shop_id     bigint null comment '商铺ID',
    item_number bigint not null comment '在架商品数',
    dt          date   not null
) comment '统计在架商品数据' charset = utf8;


-- auto-generated definition
create table shop_goods_money_top
(
    shop_id            bigint,
    item_name          varchar(255),
    sale_user_count    bigint comment '支付人数',
    sale_succeed_money double comment '统计支付金额',
    sale_ratio         double comment '支付金额占总支付金额比例',
    dt                 date not null
) comment '商品金额TOP 10' charset = utf8;


-- auto-generated definition
create table shop_goods_profit_top
(
    shop_id             bigint null,
    item_name           varchar(255) null,
    sale_user_count     bigint comment '支付人数',
    sale_succeed_profit double comment '统计利润',
    sale_profit_ratio   double comment '统计利润比',
    dt                  date not null
) comment '商品利润TOP 10' charset = utf8;


create table shop_goods_purchase_info
(
    id       integer PRIMARY KEY AUTO_INCREMENT comment 'id',
    shop_id  bigint,
    name     varchar(255) null,
    pu_num   varchar(255) comment '采购数量',
    pu_money double comment '采购金额',
    dt       date not null
) comment '商品采购数据统计' charset = utf8;
