
-- auto-generated definition
create table shop_one_goods_top
(
    id          int auto_increment comment 'id'
        primary key,
    shop_id     bigint       null comment '商铺ID',
    sku_id      bigint       null comment 'SKU_ID',
    sku_name    varchar(255) null comment 'SKU名称',
    sale_number bigint       null comment '销量',
    dt          date         not null
)
    comment '商铺-单品分析-商品销量排行' charset = utf8;

create index dt
    on shop_one_goods_top (dt);

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
    dt                  date           not null,
    avg_time            double         null comment '"平均停留时长"'
)
    comment '支付指标' charset = utf8;

create index dt
    on shop_one_goods_info (dt);

-- auto-generated definition
create table shop_one_goods_newputaway
(
    id          int auto_increment comment 'id'
        primary key,
    shop_id     bigint       null comment '商铺ID',
    shelve_time date         null comment '上架时间',
    item_id     bigint       null comment '上架商品ID',
    item_name   varchar(255) null comment '上架商品名称',
    dt          date         not null
)
    comment '商铺-单品分析-最新上架商品数' charset = utf8;

create index dt
    on shop_one_goods_newputaway (dt);

