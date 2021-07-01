-- auto-generated definition
create table shop_client_analysis
(
    id                      int auto_increment
        primary key,
    shop_id                 bigint      null,
    order_type              varchar(50) null comment '平台类型',
    money                   double      null comment '客单价',
    sale_succeed_money      double      null comment '成交金额',
    new_user_succeed_money  double      null comment '新用户成交金额',
    aged_user_succeed_money double      null comment '老用户成交金额',
    user_dis_number         bigint      null comment '全部成交客户占比',
    present_user_dis_number int         null comment '当天成交的用户数',
    aged_user_dis_number    int         null comment '当天成交的老用户数',
    new_user_dis_number     int         null comment '成交的新用户数',
    type_user_ratio         double      null comment '成交客户占比',
    new_user_ratio          double      null comment '新成交客户占比',
    aged_user_ratio         double      null comment '老成交客户占比',
    dt                      date        not null
)
    comment '客户销售分析' charset = utf8;


-- auto-generated definition
create table shop_client_members_info
(
    shop_id            text   null,
    all_user           bigint not null,
    new_user_number    bigint not null,
    user_access_number bigint null,
    dt                 date   not null
);

-- auto-generated definition
create table shop_client_sale_top
(
    shop_id             bigint       null,
    order_type          varchar(5)   null comment '订单类型',
    user_name           varchar(100) null comment '用户名',
    sale_succeed_money  int          null comment '采购金额',
    sale_succeed_profit int          null comment '采购利润',
    dt                  date         not null
)
    comment '客户采购排行榜' charset = utf8;

-- auto-generated definition
create table shop_client_store_info
(
    shop_id            text   null,
    all_user           bigint not null,
    new_user_number    bigint not null,
    user_access_number int    not null,
    dt                 date   not null
);

