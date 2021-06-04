create table shop_warehouse_inout
(
    id                integer PRIMARY KEY AUTO_INCREMENT comment 'id',
    shop_id           bigint,
    business_id       bigint  comment '业务ID',
    business_name     varchar(255) comment '业务名称',
    real_inbound_num  integer comment '入库数量',
    real_outbound_num integer comment '出库数量',
    business_type     double comment '1 商品 2仓库',
    dt                date not null
) comment '出入统计' charset = utf8;


create table shop_warehouse_info
(
    id            integer PRIMARY KEY AUTO_INCREMENT comment 'id',
    shop_id       bigint,
    business_id   bigint comment '业务ID',
    business_name varchar(255) comment '业务名称',
    inventory     integer comment '库存',
    number_money  integer comment '金额',
    business_type double comment '1 商品 2仓库  3品牌',
    dt            date not null
) comment '库存成本统计' charset = utf8;