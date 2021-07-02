create table dwd.dwd_sku_name
(
    sku_id    bigint comment '主键',
    item_name String comment '商品名称',
    status    String comment '商品状态',
    cid       String comment '三级类目id'
)COMMENT '商品 sku 中间表'
PARTITIONED BY (
dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/dwd_sku_name'
tblproperties ("orc.compression"="snappy");

DROP TABLE IF EXISTS dwd.dim_goods_cat;
create table if not exists dwd.dim_goods_cat
(
    cat_3d_id
    string, -- 三级商品分类id
    cat_3d_name
    string, -- 三级商品分类名称
    cat_2d_id
    string, -- 二级商品分类Id
    cat_2d_name
    string, -- 二级商品分类名称
    cat_1t_id
    string, -- 一级商品分类id
    cat_1t_name
    string  -- 一级商品分类名称
)
    partitioned by
(
    dt string
)
    STORED AS parquet TBLPROPERTIES
(
    'parquet.compression'='SNAPPY'
);

create table if not exists dwd.dw_shop_order
(
    shop_id
    string,
    order_id
    string,
    order_source
    string,
    paid
    string,
    industry
    string,
    industry_name
    string
)
    partitioned by
(
    dt string
)
    STORED AS parquet TBLPROPERTIES
(
    'parquet.compression'='SNAPPY'
);


-- auto-generated definition
create
external table if not exists ods.ods_base_dictionary
(
    `id` bigint             comment 'id',
      `code` varchar(20)             comment '编码',
      `name` varchar(50)             comment '名称',
      `type` varchar(20)             comment '所属类型',
      `remark` varchar(200)             comment '备注',
      `create_time` string             comment '创建时间',
      `create_user` bigint             comment '创建人',
      `update_time` string             comment '修改时间',
      `update_user` bigint             comment '修改人',
      `yn` int             comment '是否有效 1:有效 0:无效',
      `delete_flag` int             comment '删除标志0：可删除 1：不可删除',
      `content_type` int             comment '字段类型：0文字 1图片',
      `url` varchar(200)             comment '图片地址',
      `shop_id` bigint             comment '店铺ID'
)
comment '基础字典表'
PARTITIONED BY (
dt string
)
stored as parquet
location '/user/hive/warehouse/ods.db/ods_base_dictionary'
tblproperties ("orc.compression"="snappy");
ALTER TABLE ods_base_dictionary
    ADD IF NOT EXISTS PARTITION (dt='20210325') location '/user/hive/warehouse/ods.db/ods_base_dictionary/dt=20210325/';

-- 订单维度表
create
external table dwd.dwd_dim_orders_detail
(
    order_id bigint,
    payment_total_money double,
    item_name string,
    cost_price double ,
    sku_id bigint,
    cid bigint,
    num double,
    shop_id bigint,
    order_type string,
    po_type string,
    buyer_id bigint,
    paid bigint,
    refund bigint,
    seller_id bigint,
    create_time string,
    payment_time string,
    status bigint,
    order_source string,
    item_id bigint,
    item_original_price double
) COMMENT '订单表 订单明细中间表'
PARTITIONED BY (
dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/dwd_dim_orders_detail'
tblproperties ("orc.compression"="snappy");





-- 订单收货地址 城市维度表
create
external table dwd.dwd_dim_orders_receive_city
(
    order_id bigint,
    shop_id bigint,
    order_type string,
    po_type string,
    buyer_id bigint,
    seller_id bigint,
    paid bigint,
    status bigint,
    create_time string,
    payment_time string,
    order_source string,
    total_money double,
    province_name string,
    city_name string,
    country_name string,
    refund  int
) COMMENT '订单表 订单明细中间表'
PARTITIONED BY (
dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/dwd_dim_orders_receive_city'
tblproperties ("orc.compression"="snappy");




--------shanchu
create table dwd.dw_refund_orders
(
    id                     bigint comment '主键',
    create_time            string comment '创建时间',
    modify_time            string comment '修改时间',
    refund_reason          string comment '退款原因',
    refund_money           double comment '退款金额',
    sku_id                 bigint comment 'skuID',
    shop_id                bigint comment '店铺ID',
    order_type             string comment '订单类型',
    buyer_id               bigint comment '买家id',
    refund                 bigint comment '退款状态',
    refund_status          int comment '退款退货(换货)状态：
(1)退款(换货)申请等待卖家确认中，
(2)卖家不同意协议,等待买家修改,
(3)退款(换货)申请达成,等待买家发货,
(4)买家已退货,等待卖家确认收货,
(5)退款(换货)关闭,
(6)退款(换货)成功,
(7)退款中',
    order_id               string comment '订单的主键ID',
    paid                   bigint comment '支付状态 1：未支付 2：已支付',
    actually_payment_money double comment '支付状态 1：未支付 2：已支付'
) COMMENT '订单表 订单退货中间表'
location '/user/hive/warehouse/dwd.db/dw_refund_orders';


create
external table dwd.dwd_dim_refund_detail
(
    id                     bigint comment '主键',
    shop_id                bigint comment '店铺ID',
    order_id               string comment '订单的主键ID',
    refund_no              bigint,
    sku_id                 bigint comment 'skuID',
    refund_num             decimal(24,8) comment '退货数量',
    refund_price           decimal(10,2) comment '退货金额',
    create_time            string comment '创建时间',
    modify_time            string comment '修改时间',
    refund_status          int comment '退款退货(换货)状态：
(1)退款(换货)申请等待卖家确认中，
(2)卖家不同意协议,等待买家修改,
(3)退款(换货)申请达成,等待买家发货,
(4)买家已退货,等待卖家确认收货,
(5)退款(换货)关闭,
(6)退款(换货)成功,
(7)退款中',
    refund_reason          string comment '退款原因',
    po_type                string comment '销售类型  PO 为采购 空为零售',
    order_type             string comment '平台划分 TB '
) COMMENT '订单退 退货维度表'
PARTITIONED BY (
dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/dwd_dim_refund_detail'
tblproperties ("orc.compression"="snappy");
alter table dwd.dwd_dim_refund_detail add columns(refund_no string);


create
external table dwd.dwd_user_order_locus
(
    shop_id    bigint comment '商铺ID',
    buyer_id   bigint comment '用户id',
    first_time String comment '第一次消费时间',
    last_time  String comment '最近一次消费时间',
    final_time String comment '最后一次消费时间'

)COMMENT '用户订单记录'
PARTITIONED BY (
dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/dwd_user_order_locus'
tblproperties ("orc.compression"="snappy");


create external table dwd.dwd_user_statistics
(
    vip_name String comment '商铺ID',
    user_id bigint comment '用户id',
    user_grade_code int comment '等级阶梯',
    grade_name String comment '等级名称',
    vip_status int comment '会员状态',
    create_time String comment '创建时间'
)COMMENT '会员详情记录'
PARTITIONED BY (
dt string,
shop_id string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/dwd_user_statistics'
tblproperties ("orc.compression"="snappy");

create external table dwd.dwd_shop_store
(
    seller_id String comment '用户id',
    store_seller_id bigint,
    store_shop_id int ,
    store_shop_name String ,
    status int comment '关系状态 1：开启 2:停用 3：删除',
    type int comment '关系类型：1，虚拟门店（自建）；2，真实门店',
    create_time String comment '创建时间'
)COMMENT '我与我的的门店'
PARTITIONED BY (
dt string,
shop_id string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/dwd_shop_store'
tblproperties ("orc.compression"="snappy");





create external table dwd.dwd_click_log
(
    domain String comment '域',
    ip bigint comment 'IP地址',
    referrer String comment '上级url',
    shopId String comment '店铺ID',
    timeIn String comment '进入时间',
    title String comment '头',
    url String comment 'url',
    event String comment '事件',
    page_source String comment '渠道类型',
    page_type String comment '事件类型',
    loginToken String,
    itemId String comment '店铺ID',
    skuId String comment '商品ID',
    userId String comment '用户ID',
    province String comment '省',
    city String comment '市'
)COMMENT '埋点信息记录'
PARTITIONED BY (
dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/dwd_click_log'
tblproperties ("orc.compression"="snappy");


create external table dwd.dwd_dim_outbound_bill
(
   id bigint,
   shop_id bigint,
   type bigint,
   order_id bigint,
   order_detail_id bigint,
   cid bigint,
   brand_id bigint,
   item_id bigint,
   sku_id bigint,
   order_num decimal(16, 4),
   price decimal(10, 2)
)COMMENT '出库和出库明细中间表'
PARTITIONED BY (
dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/dwd_dim_outbound_bill'
tblproperties ("orc.compression"="snappy");

create external table dwd.dwd_dim_orders_self_pick
(
   order_id bigint,
   shop_id bigint,
   pick_id bigint,
   status bigint,
   order_type string,
   cost_price decimal(10, 2),
   item_original_price decimal(10, 2),
   payment_total_money decimal(10, 2),
   num decimal(10, 2),
   order_num decimal(10, 2)
)COMMENT '自提点和订单表'
PARTITIONED BY (
dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/dwd_dim_orders_self_pick'
tblproperties ("orc.compression"="snappy");