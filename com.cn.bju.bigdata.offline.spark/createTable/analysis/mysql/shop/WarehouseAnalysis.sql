-- auto-generated definition
create table shop_warehouse_inout
(
    id                int auto_increment comment 'id'
        primary key,
    shop_id           bigint       null,
    business_id       bigint       null comment '业务ID',
    business_name     varchar(255) null comment '业务名称',
    real_inbound_num  int          null comment '入库数量',
    real_outbound_num int          null comment '出库数量',
    business_type     double       null comment '1 商品 2仓库',
    dt                date         not null
)
    comment '出入统计' charset = utf8;

-- auto-generated definition
create table shop_warehouse_info
(
    id            int auto_increment comment 'id'
        primary key,
    shop_id       bigint       null,
    business_id   varchar(255) null comment '业务ID',
    business_name varchar(255) null comment '业务名称',
    inventory     int          null comment '库存',
    number_money  int          null comment '金额',
    business_type double       null comment '1 商品 2仓库 3品牌',
    dt            date         not null
)
    comment '库存成本统计' charset = utf8;

