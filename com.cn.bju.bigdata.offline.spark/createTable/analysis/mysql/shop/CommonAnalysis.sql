-- auto-generated definition
create table shop_province_info
(
    shop_id            bigint       null comment '店铺id',
    shop_name          varchar(255) null comment '店铺名称',
    order_type         varchar(10)  null comment '销售类型',
    province_name      varchar(10)  null comment '省份',
    sale_user_count    bigint       null comment '支付人数',
    sale_succeed_money double       null comment '支付金额',
    sale_ratio         double       null comment '支付比例',
    dt                 date         not null
)
    comment '商品省份' charset = utf8;

