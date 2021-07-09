-- auto-generated definition
create table shop_client_analysis
(
    id                      int auto_increment
        primary key,
    shop_id                 bigint     ,
    order_type              varchar(50) comment '平台类型',
    money                   double      comment '客单价',
    sale_succeed_money      double      comment '成交金额',
    new_user_succeed_money  double      comment '新用户成交金额',
    aged_user_succeed_money double      comment '老用户成交金额',
    user_dis_number         bigint      comment '全部成交客户占比',
    present_user_dis_number int         comment '当天成交的用户数',
    aged_user_dis_number    int         comment '当天成交的老用户数',
    new_user_dis_number     int         comment '成交的新用户数',
    type_user_ratio         double      comment '成交客户占比',
    new_user_ratio          double      comment '新成交客户占比',
    aged_user_ratio         double      comment '老成交客户占比',
    dt                      date
)
    comment '客户销售分析' charset = utf8;
-- auto-generated definition
create table shop_client_members_info
(
    shop_id              text        null,
    all_user             bigint      null,
    new_user_number      bigint      null,
    user_access_number   bigint      null,
    vip_user_up          int         null comment '升级会员数',
    attention_number     bigint(255) null comment '累计订阅人数',
    new_attention_number bigint(255) null comment '新增订阅人数',
    dt                   date        not null
);

-- auto-generated definition
create table shop_client_sale_top
(
    shop_id             bigint       ,
    order_type          varchar(5)    comment '订单类型',
    user_name           varchar(100)  comment '用户名',
    sale_succeed_money  int           comment '采购金额',
    sale_succeed_profit int           comment '采购利润',
    dt                  date
)
    comment '客户采购排行榜' charset = utf8;
-- auto-generated definition
create table shop_client_store_info
(
    shop_id            text,
    all_user           bigint,
    new_user_number    bigint,
    user_access_number int,
    dt                 date
);





create table shop_client_analysis_month
(
    id                      int auto_increment
        primary key,
    shop_id                 bigint     ,
    order_type              varchar(50) comment '平台类型',
    money                   double      comment '客单价',
    sale_succeed_money      double      comment '成交金额',
    new_user_succeed_money  double      comment '新用户成交金额',
    aged_user_succeed_money double      comment '老用户成交金额',
    user_dis_number         bigint      comment '全部成交客户占比',
    present_user_dis_number int         comment '当天成交的用户数',
    aged_user_dis_number    int         comment '当天成交的老用户数',
    new_user_dis_number     int         comment '成交的新用户数',
    type_user_ratio         double      comment '成交客户占比',
    new_user_ratio          double      comment '新成交客户占比',
    aged_user_ratio         double      comment '老成交客户占比',
    dt                      date
)
    comment '客户销售分析' charset = utf8;
-- auto-generated definition
create table shop_client_members_info_month
(
    shop_id              text        null,
    all_user             bigint      null,
    new_user_number      bigint      null,
    user_access_number   bigint      null,
    vip_user_up          int         null comment '升级会员数',
    attention_number     bigint(255) null comment '累计订阅人数',
    new_attention_number bigint(255) null comment '新增订阅人数',
    dt                   date        not null
);
-- auto-generated definition
create table shop_client_sale_top_month
(
    shop_id             bigint       ,
    order_type          varchar(5)    comment '订单类型',
    user_name           varchar(100)  comment '用户名',
    sale_succeed_money  int           comment '采购金额',
    sale_succeed_profit int           comment '采购利润',
    dt                  date
)
    comment '客户采购排行榜' charset = utf8;

-- auto-generated definition
create table shop_client_store_info_month
(
    shop_id            text,
    all_user           bigint,
    new_user_number    bigint,
    user_access_number int,
    dt                 date
);
