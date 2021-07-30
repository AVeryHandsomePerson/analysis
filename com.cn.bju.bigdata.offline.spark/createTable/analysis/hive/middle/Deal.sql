create
external table ods_orders
(
   order_id bigint,
   parent_order_id bigint,
   order_type int,
   status int,
   buyer_status int,
   seller_type bigint,
   order_platform varchar(20),
   order_source varchar(20),
   total_money decimal(12,2),
   freight_money decimal(10,2),
   discount_money decimal(12,2),
   cut_money decimal(16,4),
   other_fee decimal(10,2),
   round_down decimal(10,2),
   actually_payment_money decimal(12,2),
   buyer_id bigint,
   buyer_name varchar(20),
   buyer_shop_id bigint,
   buyer_shop_name varchar(255),
   seller_id bigint,
   seller_name varchar(20),
   shop_id bigint,
   shop_name varchar(200),
   option varchar(500),
   paid int,
   payment_source varchar(255),
   payment_time string,
   refund int,
   exchange int,
   invoice int,
   buyer_memo varchar(500),
   seller_memo varchar(200),
   is_change_price int,
   change_price_user bigint,
   change_price_time string,
   settle_flag int,
   evaluation varchar(11),
   create_time string,
   modify_time string,
   create_user bigint,
   modify_user bigint,
   deposit decimal(10,2),
   retainage decimal(10,2),
   retainage_order_id bigint,
   presell_id bigint,
   presell_pay_type int,
   order_credit int,
   yn int,
   manage_user_id bigint,
   manage_username varchar(255),
   buyer_manage_user_id bigint,
   buyer_manage_username varchar(255),
   purchase_date string,
   warehouse_code varchar(255),
   warehouse_name varchar(255),
   reason varchar(255),
   audit_time string,
   audit_user_id int,
   audit_username varchar(255),
   remark varchar(255),
   seller_org_code varchar(255),
   seller_org_parent_code varchar(255),
   buyer_org_code varchar(255),
   buyer_org_parent_code varchar(255),
   delivery_type int,
   print_price int,
   consignment int,
   store_complete int,
   balance_amount decimal(14,2),
   balance_flag int,
   issue_flag int,
   self_pick_flag int,
   expect_receive_time string,
   delivery_remark varchar(255),
   sub_mchId                 varchar(100)                  null comment '子商户号',
   order_cancel_time         string                      null comment '订单取消时间',
   distribution_flag         int                           null comment '分销订单标识 0：普通订单 1：分销订单',
   dis_shop_id               bigint                        null comment '分销店铺ID',
   dis_user_id               bigint                        null comment '分销用户ID',
   dis_sub_mchId             varchar(100)                  null comment '分销二级商户号',
   total_commission          decimal(14, 2)                null,
   chain_store_id            bigint                        null comment '连锁门店id',
   group_leader_shop_id      bigint                        null comment '团长店铺ID',
   group_leader_user_id      bigint                        null comment '团长用户ID',
   group_leader_mchId        varchar(100)                  null comment '团长商户号',
   group_purchase_commission decimal(14, 2)                null comment '团购佣金',
   group_leader_remark       varchar(512)                  null comment '团长备注',
   group_purchase_code       varchar(100)                  null comment '团购活动编码',
   need_invoice_flag         int                           null comment '团购订单是否开票：1，否；2，是；'
) COMMENT '订单表'
PARTITIONED BY (
    dt string
)
stored as parquet
location '/user/hive/warehouse/ods.db/ods_orders'
tblproperties ("orc.compression" = "snappy");
ALTER TABLE ods.ods_orders DROP IF EXISTS PARTITION (dt='20210728');

alter table ods.ods_orders add columns(sub_mchId  varchar(100),order_cancel_time   string,distribution_flag    int,dis_shop_id   bigint,dis_user_id   bigint,dis_sub_mchId  varchar(100),
total_commission decimal(14, 2),chain_store_id bigint,group_leader_shop_id bigint,group_leader_user_id bigint,group_leader_mchId varchar(100),
group_purchase_commission decimal(14, 2),group_leader_remark varchar(512),group_purchase_code varchar(100),need_invoice_flag int);



ALTER TABLE ods_orders
    ADD IF NOT EXISTS PARTITION (dt = '20210329') location '/user/hive/warehouse/ods.db/ods_orders/dt=20210325/';

create
external table ods.ods_orders_detail
(
    id                  bigint,
    order_id            bigint comment '订单的主键ID',
    item_id             bigint comment '商品ID',
    cid                 bigint comment '商品的类目ID',
    brand_id            bigint,
    sku_id              bigint comment 'skuid',
    sku_code            varchar(255) comment '商家SKU编码',
    item_name           varchar(100) comment '商品名称',
    sku_pic_url         varchar(255) comment '商品图片',
    sku_sale_attr_str   varchar(255) comment '商品销售属性中文名称',
    item_original_price decimal(10, 2) comment '商品的原始价格',
    cost_price          decimal(10, 2) comment '成本价',
    payment_total_money decimal(10, 2) comment '支付的总价格（不含运费）',
    payment_price       decimal(10, 2) comment '支付的单价（不含运费）',
    cut_price           decimal(16, 4) comment '分切单价',
    cut_price_total     decimal(16, 4) comment '分切总价',
    refund              int comment '退款标记',
    exchange            int comment '是否换货  0未换货  1换货',
    num                 decimal(24, 8) comment '商品数量',
    sheet_num           int comment '张数',
    reel                int comment '卷数',
    discount_money      decimal(10, 2) comment '总的优惠金额',
    freight_template_id bigint comment '运费模板ID',
    delivery_type       int comment '运送方式',
    seller_id           bigint,
    shop_id             bigint,
    buyer_item_id       bigint comment '买方商品编码',
    buyer_sku_id        bigint comment '买方SKUID',
    buyer_sku_code      varchar(255) comment '买方商家SKU编码',
    evaluation          varchar(20) comment '订单商品评价状态1为评价，2以评价',
    create_time         string comment '创建时间',
    modify_time         string comment '修改时间',
    create_user         bigint comment '创建者',
    modify_user         bigint comment '修改者',
    is_gift             int comment '是否是赠品  1：是  0，不是',
    inbound_num         decimal(16, 4) comment '入库数量',
    outbound_num        decimal(16, 4) comment '出库数量',
    weight_unit         int comment '克重',
    width_unit          int comment '幅宽',
    length_unit         int comment '长度',
    purchase_num        decimal(16, 4) comment '采购量',
    divided_balance     decimal(14, 2) comment '分摊的余额金额',
    delivery_date       string comment '交货时间',
    urgent_type         int comment '是否加急 0不加急 1加急',
    already_cut         int comment '是否已分切 0未分切 1已分切',
    already_outbound    int comment '是否已出库 0未出库 1已出库',
    work_order_no       varchar(64) comment '工单号'
) comment '订单明细表'
PARTITIONED BY (
    dt string
    )
stored as parquet
location '/user/hive/warehouse/ods.db/ods_orders_detail'
tblproperties ("orc.compression" = "snappy");


ALTER TABLE ods.ods_orders_detail
    ADD IF NOT EXISTS PARTITION (dt = '20210329') location '/user/hive/warehouse/ods.db/ods_orders_detail/dt=20210329/';

--拉链表
create
external table ods.ods_orders_receive
(
      id                  bigint  comment '主键ID',
      order_id            bigint  comment '订单主键ID',
      consignee_name      varchar(32)  comment '收货人姓名',
      consignee_mobile    varchar(20) comment '收货人手机',
      consignee_phone     varchar(20)  comment '收货人固定电话',
      consignee_mail      varchar(32)  comment '收货人邮箱',
      province_code       bigint  comment '省ID',
      city_code           bigint  comment '市ID',
      country_code        bigint  comment '县区ID',
      town_code           bigint,
      province_name       varchar(200)  comment '省名称',
      city_name           varchar(200)  comment '市名称',
      country_name        varchar(200)  comment '区名称',
      town_name           varchar(255)  ,
      detail_address      varchar(200)  comment '详细地址',
      create_time string  comment '创建时间',
      modify_time string  comment '修改时间',
      create_user bigint  comment '创建者',
      modify_user bigint  comment '修改者',
      yn int comment '是否有效'
)
    comment '订单收货信息表'
    PARTITIONED BY (
        dt string
        )
    stored as parquet
    location '/user/hive/warehouse/ods.db/ods_orders_receive'
    tblproperties ("orc.compression" = "snappy");
ALTER TABLE ods.ods_orders_receive
    ADD IF NOT EXISTS PARTITION (dt = '20210329') location '/user/hive/warehouse/ods.db/ods_orders_receive/dt=20210325/';



create
external table ods.ods_refund_detail
(
    id bigint              comment '主键ID',
      refund_id bigint              comment '退款退货表的主键ID',
      order_id bigint              comment '订单ID',
      order_detail_id bigint              comment '订单详情的主键ID',
      item_id bigint              comment '商品的ID',
      item_name varchar(100)              comment '商品名称',
      sku_id bigint              comment 'skuID',
      sku_code varchar(255)              comment '商家SKU编码',
      sku_pic_url varchar(255)              comment '商品图片',
      sku_sale_attr_str varchar(255)              comment '商品销售属性中文名称',
      num decimal(24,8)              comment '数量',
      transaction_money decimal(10,2)              comment '交易金额',
      refund_money decimal(10,2)              comment '退款金额',
      refund_num decimal(24,8)              comment '退款退货数量',
      refund_price decimal(10,2)              comment '退款单价',
      notes varchar(200)              comment '备注',
      create_time string              comment '创建时间',
      modify_time string              comment '修改时间',
      create_user bigint              comment '创建者',
      modify_user bigint              comment '修改者',
      yn int              comment '是否有效',
      buyer_item_id bigint              comment '买方商品编码',
      buyer_sku_id bigint              comment '买方SKUID',
      buyer_sku_code varchar(255)              comment '买方商家SKU编码',
      inbound_num decimal(20,4)              comment '入库数量',
      outbound_num decimal(20,4)              comment '出库数量',
      divided_balance decimal(14)              comment '余额金额'
)
    comment '退货退款明细表'
    PARTITIONED BY (
        dt string
        )
    stored as parquet
    location '/user/hive/warehouse/ods.db/ods_refund_detail'
    tblproperties ("orc.compression" = "snappy");
ALTER TABLE ods.ods_refund_detail
    ADD IF NOT EXISTS PARTITION (dt = '20210329') location '/user/hive/warehouse/ods.db/ods_refund_detail/dt=20210329/';
--订单退货表
create
external table ods.ods_refund_apply
(
     id bigint comment '主键',
      refund_no varchar(20) comment '退款退货编号',
      type int comment '退单类型：1，零售退单； 2，采购退单（手工单退单，订单类型6）； 3，销售退单（手工单退单，订单类型7）； 4，采购退单（正常单，订单类型8）； 5，销售退单（正常单，订单类型8）',
      refund_type int comment '退款退货类型：（1）退款，（2）退货 ，（3）换货',
      order_id bigint comment '订单的主键ID',
      order_status int comment '订单的状态',
      refund_status int comment '退款退货(换货)状态：(1) 退款(换货)申请等待卖家确认中，(2)卖家不同意协议,等待买家修改,(3)退款(换货)申请达成,等待买家发货,(4)买家已退货,等待卖家确认收货,(5)退款(换货)关闭,(6)退款(换货)成功,(7)退款中',
      refund_reason varchar(100) comment '退款退货原因',
      question_description varchar(200) comment '问题描述',
      receiving int comment '是否收到货：1收到；2：未收到',
      audit_id bigint comment '审核者ID',
      audit_name varchar(32) comment '审核者姓名',
      audit_time string comment '审核时间',
      audit_notes varchar(100) comment '审核意见备注',
      refund_total_money decimal(10,2) comment '可退的总金额',
      apply_refund_money decimal(10,2) comment '申请的退款金额（含余额支付时，仅表示第三方支付的金额。总退款金额=apply_refund_money+balance_amout）',
      province_code bigint comment '省编码',
      province_name varchar(200) comment '省名称',
      city_code bigint comment '市编码',
      city_name varchar(200) comment '市名称',
      country_code bigint comment '区/县编码',
      country_name varchar(200) comment '区/县名称',
      town_code bigint comment '乡/镇编码',
      town_name varchar(200) comment '乡/镇名称',
      detail_address varchar(512) comment '详细地址',
      refund_address varchar(600) comment '退货地址',
      refund_receiver varchar(100) comment '退货收货人',
      refund_mobile varchar(100) comment '退货收货人电话',
      refund_directions varchar(600) comment '退款说明',
      create_time string comment '创建时间',
      modify_time string comment '修改时间',
      create_user bigint comment '创建者',
      modify_user bigint comment '修改者',
      yn int comment '是否有效',
      manage_user_id bigint comment '经销商业务员用户ID(卖方业务员ID)',
      manage_username varchar(255) comment '经销商业务员用户名(卖方业务员名称)',
      seller_org_code varchar(255) comment '归属卖家组织机构编码',
      seller_org_parent_code varchar(255) comment '归属卖家上级组织机构编码',
      buyer_manage_user_id bigint comment '买方业务员ID',
      buyer_manage_username varchar(255) comment '买方业务员名称',
      buyer_org_code varchar(255) comment '归属买家组织机构编码',
      buyer_org_parent_code varchar(255) comment '归属买家上级组织机构编码',
      create_flag int comment '创建标识 1：买方创建 2：卖方创建',
      buyer_id bigint comment '买家id',
      buyer_name varchar(20) comment '买家账号',
      buyer_shop_id bigint comment '买家店铺ID',
      buyer_shop_name varchar(255) comment '买家店铺名称',
      seller_id bigint comment '卖家id',
      seller_name varchar(20) comment '卖家账号',
      shop_id bigint comment '店铺ID',
      shop_name varchar(200) comment '店铺名称',
      inbound_num int comment '入库总数量',
      outbound_num int comment '出库总数量',
      all_item_num int comment '订单总商品数量',
      consignment int comment '发货完成标识：1，否；2，是',
      store_complete int comment '入库完成标识：1，否；2，是',
      balance_amount decimal(14,2) comment '余额金额',
      is_create_bound_bill int comment '是否创建入库单：0，否；1，是'
)
    comment '退款退货主表'
    PARTITIONED BY (
        dt string
        )
    stored as parquet
    location '/user/hive/warehouse/ods.db/ods_refund_apply'
    tblproperties ("orc.compression" = "snappy");
ALTER TABLE ods.ods_refund_apply
    ADD IF NOT EXISTS PARTITION (dt = '20210329') location '/user/hive/warehouse/ods.db/ods_refund_apply/dt=20210329/';


create table ods.ods_refund_process
(
    id              bigint,
    platform        varchar(20) comment '所属平台',
    process_type    int comment '进程类型：1.退款进程，2：投诉进程，3.换货进程',
    refund_id       bigint comment '退货退款单号',
    complain_id     int comment '投诉Id',
    buyer_id        bigint comment '买家Id',
    seller_id       bigint comment '卖家Id',
    type            int comment '类型：1,买家操作；2.卖家操作；0.平台操作',
    content         varchar(255) comment '操作内容',
    content_pic_url varchar(600) comment '操作图片',
    create_time     String comment '创建时间',
    modify_time     String comment '修改时间',
    create_user     bigint comment '创建人',
    modify_user     bigint comment '修改人',
    yn              int comment '是否有效'
) comment '退款进程表'
PARTITIONED BY (
    dt string
)
stored as parquet
location '/user/hive/warehouse/ods.db/ods_refund_process'
tblproperties ("orc.compression" = "snappy");


create external table ods.ods_orders_self_pick
(
    id             bigint,
    order_id       bigint comment '订单号',
    verification   string comment '核销码',
    pick_id        bigint comment '网点编码',
    pick_name      string comment '网点名称',
    province_code  bigint,
    province_name  string,
    city_code      bigint,
    city_name      string,
    country_code   bigint,
    country_name   string,
    town_code      bigint,
    town_name      string,
    detail_address string,
    contact_phone  string comment '联系电话',
    contact_name   string comment '联系人',
    confirm_time   string comment '核销时间',
    confirm_user   bigint comment '核销人',
    create_time    string,
    create_user    bigint,
    modify_time    string,
    modify_user    bigint
) comment '订单自提信息'
PARTITIONED BY (
    dt string
)
stored as parquet
location '/user/hive/warehouse/ods.db/ods_orders_self_pick'
tblproperties ("orc.compression" = "snappy");