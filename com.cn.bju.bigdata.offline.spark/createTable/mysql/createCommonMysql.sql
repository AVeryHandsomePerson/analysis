create table shop_province_info
(
    shop_id            bigint comment '店铺id',
    shop_name          varchar(255) comment '店铺名称',
    order_type         varchar(10) comment '销售类型',
    province_name      varchar(10) comment '省份',
    sale_user_count    bigint comment '支付人数',
    sale_succeed_money double comment '支付金额',
    sale_ratio         double comment '支付比例',
    dt                 date not null
) comment '商品省份' ENGINE=InnoDB DEFAULT CHARSET=utf8;

create table goods_province_type_top
(
    shop_id            bigint null,
    order_type         varchar(10) null,
    province_name      text null,
    sale_user_count    bigint not null,
    sale_succeed_money double null,
    sale_ratio         double null,
    dt                 date   not null
) comment '商品省份TOP 10' ENGINE=InnoDB DEFAULT CHARSET=utf8;


create table successful_transaction
(
    shop_id               bigint null,
    order_type            varchar(5) null comment '订单类型',
    sale_succeed_money    double null comment '成交金额',
    succeed_orders_number double null comment '成交单量',
    sale_succeed_number   double null comment '成交件数',
    sale_user_number      double null comment '支付人数',
    dt                    date not null
) comment '成交商品' ENGINE=InnoDB DEFAULT CHARSET=utf8;



