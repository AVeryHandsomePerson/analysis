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
-- auto-generated definition
create table shop_one_goods_info
(
    id                  int auto_increment comment 'id'
        primary key,
    shop_id             bigint         null comment '商铺ID',
    sku_id              bigint         null comment 'SKU商品',
    picture_url         varchar(255)   null comment '商品图片地址',
    source_type         varchar(10)    null comment '渠道类型',
    shelve_time         varchar(50)    null comment '上架时间',
    item_original_price decimal(10, 2) null comment '销售价',
    cost_price          decimal(10, 2) null comment '成本价',
    cid_name            varchar(50)    null comment '类目名称',
    sku_name            varchar(255)   null comment '上架商品名称',
    paid_number         double         null comment '支付件数',
    sale_user_number    bigint         null comment '支付人数',
    sale_succeed_money  double         null comment '支付金额',
    all_sale_user_count bigint         null comment '总下单用户数',
    pv                  bigint         null comment '浏览量',
    uv                  bigint         null comment '访客数',
    uv_value            double         null comment 'UV价值',
    page_leave_ratio    double         null comment '详情页跳出率',
    orders_ratio        double         null comment '下单-转化率',
    dt                  date           not null
)
    comment '支付指标' charset = utf8;

create index dt
    on shop_one_goods_info (dt);



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

