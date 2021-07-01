drop table shop_analysis_number;
create table shop_analysis_number
(
    shop_number          bigint comment '商铺数',
    operation_number     bigint comment '营业数',
    no_operation_number  bigint not null comment '打烊数',
    own_shop_number      bigint not null comment '自营店',
    flagship_shop_number bigint not null comment '旗舰店',
    alone_shop_number    bigint not null comment '专营店',
    dt                   date   not null
) comment '商家数据分析' ENGINE=InnoDB DEFAULT CHARSET=utf8;

create table shop_cid_number
(
    cat_1t_name varchar(100) comment '类目名称',
    shop_number bigint comment '品牌数量',
    dt          date not null
) comment '商家数据分析' ENGINE=InnoDB DEFAULT CHARSET=utf8;

create table shop_industry_number
(
    industry             varchar(100) comment '行业编码',
    industry_name        varchar(100) comment '行业名称',
    payment_order_number bigint comment '各行业数量',
    dt                   date not null
) comment '商家数据分析-全平台行业分布比例' ENGINE=InnoDB DEFAULT CHARSET=utf8;
create table shop_refund_analysis
(
    shop_id               integer comment '商铺ID',
    shop_name             varchar(100) comment '商铺名称',
    all_payment_money     double comment '成功总金额',
    refund_moneys         double comment '待退款金额',
    refund_succeed_moneys double comment '已退款金额',
    refund_succeed_number integer comment '退款成功单数',
    count_shop_number     integer comment '店铺下支付成功的人数',
    refund_ratio          double comment '退款率',
    dt                    date not null
) comment '商家营收分析-商家交易' ENGINE=InnoDB DEFAULT CHARSET=utf8;


create table shop_orders_industry
(
    shop_id              integer comment '行业编码',
    industry             varchar(100) comment '行业编码',
    industry_name        varchar(100) comment '行业名称',
    payment_order_number bigint comment '各行业数量',
    dt                   date not null
) comment '商家数据分析-下单成功行业分布比例' ENGINE=InnoDB DEFAULT CHARSET=utf8;


create table platform_orders_sale
(
    order_number       integer comment '交易订单',
    order_money_money  double comment '订单金额',
    client_alone_money double comment '客单价',
    pay_number         bigint comment '支付单数',
    dt                 date not null
) comment '平台订单分布-平台订单信息' ENGINE=InnoDB DEFAULT CHARSET=utf8;


create table platform_orders_status
(
    status             integer comment '订单状态',
    status_name        integer comment '订单状态',
    status_type_number integer comment '订单数量',
    dt                 date not null
) comment '平台订单分布-订单状态分布' ENGINE=InnoDB DEFAULT CHARSET=utf8;

create table platform_orders_time
(
    payment_time         integer comment '支付时间',
    payment_order_number integer comment '订单数量',
    dt                   date not null
) comment '平台订单分布-消费时间段分布' ENGINE=InnoDB DEFAULT CHARSET=utf8;

create table platform_orders_source
(
    order_source        varchar(30) comment '渠道',
    payment_place_ratio double comment '渠道占比',
    dt                  date not null
) comment '平台订单分布-渠道比例' ENGINE=InnoDB DEFAULT CHARSET=utf8;


create table platform_cat_number
(
    cat_1t_number integer comment '1级类目',
    cat_2d_number integer comment '2级类目',
    cat_3d_number integer comment '3级类目',
    dt            date not null
) comment '平台级-商家商品数据-类目统计' ENGINE=InnoDB DEFAULT CHARSET=utf8;

create table shop_cat_number
(
    shop_id       integer comment '店铺ID',
    shop_name     varchar(100) comment '店铺名称',
    cat_1t_number integer comment '1级类目',
    cat_2d_number integer comment '2级类目',
    cat_3d_number integer comment '3级类目',
    dt            date not null
) comment '店铺级-商家商品数据-类目统计' ENGINE=InnoDB DEFAULT CHARSET=utf8;


create table platform_cat_number
(
    item_number       integer comment '1级类目',
    shelf_item_number integer comment '2级类目',
    sale_number       integer comment '3级类目',
    dt                date not null
) comment '平台级-商家商品数据-商品总数' ENGINE=InnoDB DEFAULT CHARSET=utf8;

create table shop_goods_number
(
    shop_id           integer comment '店铺ID',
    shop_name         varchar(100) comment '店铺名称',
    item_number       integer comment '1级类目',
    shelf_item_number integer comment '2级类目',
    sale_number       integer comment '3级类目',
    dt                date not null
) comment '平台级-商家商品数据-商品总数' ENGINE=InnoDB DEFAULT CHARSET=utf8;


create table platform_goods_info
(
    id                    integer  PRIMARY KEY AUTO_INCREMENT comment 'id',
    sku_id                integer comment 'sku_id',
    item_name             varchar(100) comment '商品名称',
    cat_name              varchar(100) comment '分类',
    status                varchar(50) comment '状态',
    orders_succeed_number bigint comment '销量',
    sale_order_number     double comment '总销售额',
    sale_succeed_money    double comment '成交金额',
    money                 double comment '客单价',
    dt                    date not null
) comment '商品数据分析'  AUTO_INCREMENT = 100  ENGINE=InnoDB DEFAULT CHARSET=utf8;


