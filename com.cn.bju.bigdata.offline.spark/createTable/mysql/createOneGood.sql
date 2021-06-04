create table shop_one_goods_info
(
    id                  integer PRIMARY KEY AUTO_INCREMENT comment 'id',
    shop_id             bigint comment '商铺ID',
    sku_id              bigint comment 'SKU商品',
    sku_price           decimal(18, 2) comment 'SKU单价',
    pv                  bigint comment '浏览量',
    uv                  bigint comment '访客数',
    picture_url         varchar(255) comment '商品图片地址',
    source_type         varchar(10) comment '渠道类型',
    sku_name            varchar(255) comment '上架商品名称',
    paid_number         double comment '支付件数',
    sale_user_number    bigint comment '支付人数',
    sale_succeed_money  double comment '支付金额',
    all_sale_user_count bigint comment '总下单用户数',
    sku_rate            double comment '支付转化率',
    dt                  date not null,
    INDEX (dt)
) comment '支付指标' ENGINE = InnoDB
                 DEFAULT CHARSET = utf8;


create table shop_one_goods_newputaway
(
    id          integer PRIMARY KEY AUTO_INCREMENT comment 'id',
    shop_id     bigint comment '商铺ID',
    shelve_time date comment '上架时间',
    item_id     bigint comment '上架商品ID',
    item_name   varchar(255) comment '上架商品名称',
    dt          date not null,
    INDEX (dt)
) comment '商铺-单品分析-最新上架商品数' ENGINE = InnoDB
                            DEFAULT CHARSET = utf8;

create table shop_one_goods_top
(
    id          integer PRIMARY KEY AUTO_INCREMENT comment 'id',
    shop_id     bigint comment '商铺ID',
    sku_id      bigint comment 'SKU_ID',
    sku_name    varchar(255) comment 'SKU名称',
    sale_number bigint comment '销量',
    dt          date not null,
    INDEX (dt)
) comment '商铺-单品分析-商品销量排行' ENGINE = InnoDB
                           DEFAULT CHARSET = utf8;

