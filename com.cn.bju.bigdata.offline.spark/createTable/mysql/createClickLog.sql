create table shop_clicklog_info
(
    id               int auto_increment comment 'id'
        primary key,
    shop_id          bigint null comment '商铺ID',
    sku_id           bigint null comment 'SKU商品',
    page_source      varchar(255) null comment '类型',
    pv               bigint null comment '浏览量',
    uv               bigint null comment '访客数',
    new_user         bigint null comment '新访客数',
    paid_user_number bigint null comment '上架时间',
    lose_ratio       decimal(10, 2) null comment '跳失率',
    avg_time         decimal(10, 2) null comment '平均停留时长',
    avg_pv           decimal(10, 2) null comment '人均浏览量',
    visit_paid_ratio decimal(10, 2) null comment '访问支付转化率',
    dt               date not null
) comment '支付指标' charset = utf8;

create
index dt
    on shop_one_goods_info (dt);