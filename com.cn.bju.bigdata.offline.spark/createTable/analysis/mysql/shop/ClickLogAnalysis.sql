-- auto-generated definition
create table shop_clicklog_info
(
    id               int auto_increment comment 'id'
        primary key,
    shop_id          bigint         comment '商铺ID',
    page_source      varchar(255)   comment '类型',
    pv               bigint         comment '浏览量',
    uv               bigint         comment '访客数',
    new_user         bigint         comment '新访客数',
    paid_user_number bigint         comment '支付用户数',
    lose_ratio       decimal(10, 2) comment '跳失率',
    avg_time         decimal(10, 2) comment '平均停留时长',
    avg_pv           decimal(10, 2) comment '人均浏览量',
    visit_paid_ratio decimal(10, 2) comment '访问支付转化率',
    dt               date
)
    comment '流量指标' charset = utf8;

create table shop_clicklog_info_month
(
    id               int auto_increment comment 'id'
        primary key,
    shop_id          bigint         comment '商铺ID',
    page_source      varchar(255)   comment '类型',
    pv               bigint         comment '浏览量',
    uv               bigint         comment '访客数',
    new_user         bigint         comment '新访客数',
    paid_user_number bigint         comment '支付用户数',
    lose_ratio       decimal(10, 2) comment '跳失率',
    avg_time         decimal(10, 2) comment '平均停留时长',
    avg_pv           decimal(10, 2) comment '人均浏览量',
    visit_paid_ratio decimal(10, 2) comment '访问支付转化率',
    dt               date
)
    comment '流量指标' charset = utf8;