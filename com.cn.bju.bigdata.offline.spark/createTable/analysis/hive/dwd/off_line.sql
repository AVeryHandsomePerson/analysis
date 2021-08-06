--拉链表
create
external table dwd.dwd_fact_order_info
(
order_id String,
shop_id String,
shop_name String,
buyer_id String,
buyer_name String,
seller_id  String,
seller_name  String,
order_source String,
paid int,
refund int ,
province_name String,
city_name String,
country_name String,
order_type String,
po_type String, -- 采购
cid bigint,
cat_3d_name String,
brand_id bigint,
item_id bigint,
sku_id bigint,
item_name String,
sku_pic_url String,
pick_id bigint, -- 自提点id
pick_name String, -- 自提点Name
pick_order_id String, -- 自提点订单
create_time String, --订单时间
freight_money decimal(10,2), --订单总运费
cost_price decimal(10,2),--订单成本价
payment_price decimal(10,2), --订单支付价
payment_num decimal(10,2),
item_original_price decimal(10,2), --商品原始价格
shelve_time String, --上架时间
cid_name String --类目名称
)
PARTITIONED BY (
dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/dwd_fact_order_info'
tblproperties ("orc.compression" = "snappy");

ALTER TABLE dwd.dwd_fact_order_info DROP IF EXISTS PARTITION (dt='20210804');

alter table dwd.dwd_fact_order_info add columns(shelve_time String,cid_name String);

create
external table dwd.dwd_fact_outbound_bill_info
(
id String,
shop_id String,
type String,
province_name String,
city_name String,
country_name String,
buyer_id String,
buyer_name String,
seller_id String,
seller_name String,
paid int,
po_type String,
freight_money decimal(10,2), --订单总运费
order_id String,
order_detail_id String,
cid bigint,
cat_3d_name String,
brand_id bigint,
item_id bigint,
item_name String,
sku_id bigint,
pick_id bigint, -- 自提点id
pick_name String, -- 自提点Name
pick_order_id String, -- 自提点订单
payment_num decimal(10,2),
payment_price decimal(10,2),
cost_price decimal(10,2),
sku_pic_url String,
item_original_price decimal(10,2), --商品原始价格
group_purchase_commission decimal(10,2), --商品原始价格
group_leader_shop_id String,
group_leader_user_id String,
shop_name String,
shelve_time String,
cid_name String
)
PARTITIONED BY (
dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/dwd_fact_outbound_bill_info'
tblproperties ("orc.compression" = "snappy");
alter table dwd.dwd_fact_outbound_bill_info add columns(shop_name String);
ALTER TABLE dwd.dwd_fact_outbound_bill_info DROP IF EXISTS PARTITION (dt='20210804');

alter table dwd.dwd_fact_outbound_bill_info add columns(shelve_time String,cid_name String);
DROP COLUMN [IF EXISTS] name


create
external table dwd.dwd_fact_order_refund_info
(
id String,
shop_id String,
order_id String,
refund_no String,
buyer_id String,
sku_id String,
sku_pic_url String,
item_name String,
refund_num decimal(10,2),
refund_price decimal(10,2),
create_time decimal(10,2),
modify_time decimal(10,2),
refund_status int,
refund_reason String,
po_type String,
order_type String,
avg_time decimal(10,2) --平均处理时间
)
PARTITIONED BY (
dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/dwd_fact_order_refund_info'
tblproperties ("orc.compression" = "snappy");
alter table dwd.dwd_fact_order_refund_info add columns(refund_group_commission String);
ALTER TABLE dwd.dwd_fact_order_refund_info DROP IF EXISTS PARTITION (dt='20210802');

create
external table dwd.dwd_dim_order_user_locus
(
    shop_id    bigint comment '商铺ID',
    order_type String comment '订单类型',
    po_type    String comment '是否采购',
    paid       bigint comment '支付状态',
    buyer_id   bigint comment '用户id',
    first_time String comment '第一次消费时间',
    last_time  String comment '最近一次消费时间',
    final_time String comment '最后一次消费时间'

)COMMENT '用户订单记录'
PARTITIONED BY (
dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/dwd_dim_order_user_locus'
tblproperties ("orc.compression"="snappy");



create
external table dwd.dwd_shop_refund_money
(
    shop_id    bigint comment '商铺ID',
    sku_id    bigint comment 'SKU_ID',
    refund_money String comment '退款金额'
)COMMENT '商品退款金额表'
PARTITIONED BY (
dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/dwd_shop_refund_money'
tblproperties ("orc.compression"="snappy");


create
external table dwd.dwd_dim_user_statistics
(
    shop_id string,
    vip_name String comment '商铺ID',
    user_id bigint comment '用户id',
    user_grade_code int comment '等级阶梯',
    vip_user_up int comment '1:等级上升 0:等级下降或相等',
    vip_user_down int comment '1: 等级下降 0:等级上升或相等',
    grade_name String comment '等级名称',
    vip_status int comment '会员状态',
    create_time String comment '创建时间'
)COMMENT '会员详情记录'
PARTITIONED BY (
dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/dwd_dim_user_statistics'
tblproperties ("orc.compression"="snappy");

create
external table dwd.dwd_dim_shop_store
(
    shop_id string,
    seller_id String comment '用户id',
    store_seller_id bigint,
    store_shop_id int ,
    store_shop_name String ,
    status int comment '关系状态 1：开启 2:停用 3：删除',
    type int comment '关系类型：1，虚拟门店（自建）；2，真实门店',
    create_time String comment '创建时间'
)COMMENT '我与我的的门店'
PARTITIONED BY (
dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/dwd_dim_shop_store'
tblproperties ("orc.compression"="snappy");

create
external table dwd.dwd_dim_warehouse_inout
(
in_shop_id int,
in_warehouse_code int,
in_warehouse_name String,
in_item_id int,
in_item_name String,
in_sku_id int,
in_price double,
inbound_num double,
out_shop_id int,
out_warehouse_code int,
out_warehouse_name String,
out_item_id int,
out_item_name String,
out_sku_id int,
out_price double,
outbound_num double,
types String
)COMMENT '出入库信息'
PARTITIONED BY (
dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/dwd_dim_warehouse_inout'
tblproperties ("orc.compression"="snappy");



create
external table dwd.dwd_inbound_bill_record
(
shop_id int,
item_name String,
sku_code String,
warehouse_code bigint,
warehouse_name String,
brand_id int,
brand_name String,
inbound_num double,
total_money double,
price double
)COMMENT '入库详情'
PARTITIONED BY (
dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/dwd_inbound_bill_record'
tblproperties ("orc.compression"="snappy");



