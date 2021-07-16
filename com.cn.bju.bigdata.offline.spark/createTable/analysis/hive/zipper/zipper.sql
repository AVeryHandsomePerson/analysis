--动态分区 在spark-sql中执行
set
hive.exec.dynamici.partition=true;
set
hive.exec.dynamic.partition.mode=nonstrict;
-----暂时去除
create
external table dwd.fact_item
(
    item_id                  bigint comment '商品id',
    brand_id                 bigint comment '品牌',
    cid                      bigint comment '类目ID',
    seller_id                bigint comment '商家ID',
    shop_id                  bigint comment '店铺ID',
    shop_cid                 string comment '商品所属店铺类目id：可以绑定多个店铺类目',
    shop_freight_template_id bigint comment '运费模版ID',
    attributes               string comment '商品类目属性keyId:valueId',
    attr_sale                string comment '商品销售属性keyId:valueId',
    status                   int comment '商品状态,1:未发布、2:待审核、3:待上架、4:在售、5:已下架、6:锁定、7:申请解锁、8:平台下架、9:系统下架、10:定时下架、11:下线、20:审核驳回、30:删除 ',
    type                     int comment '1:普通商品 2:虚拟商品',
    item_name                string comment '商品名称',
    shelve_time              string comment '上架时间',
    off_shelve_time          string comment '下架时间',
    task_shelve_time         string comment '定时上架时间',
    task_off_shelve_time     string comment '定时下架时间',
    origin                   string comment '商品产地',
    weight                   string comment '商品毛重',
    volume                   string comment '商品体积',
    length                   string comment '长',
    width                    string comment '宽',
    height                   string comment '高',
    ad                       string comment '广告词',
    keyword                  string comment '关键字：每个商品可设置5个关键字，英文逗号分隔',
    remark                   string,
    unit_code                string comment '单位编码',
    unit_name                string comment '单位名称',
    quotation_way            string comment '报价方式  1: 按产品数量报价  2：按产品规格报价',
    create_time              string comment '创建日期',
    create_user              bigint,
    update_time              string comment '修改日期',
    update_user              bigint,
    yn                       int comment '是否有效',
    sign                     string comment '标记',
    status_change_reason     string comment '平台方下架或锁定或审核驳回时给出的理由',
    platform                 string comment '所属平台',
    give_away                int comment '是否是赠品  1:是  2:否',
    pay_type                 string comment '支付方式  1.货到付款  2:在线支付',
    sale_channel             string comment '销售渠道  1:web  2:h5  3:app  4:微信',
    points                   float comment '积分比例',
    upc                      string comment '商品通用条码：管理商品_单个商品层面',
    goods_code               string comment '货号：管理商品_批发层面',
    attr_template_id         bigint comment '属性模板ID',
    `describe`               string comment '商品描述',
    ad_url                   string comment '广告词链接',
    source_item_id           bigint comment '原商品ID',
    master_item_id           bigint comment '主商品ID',
    rebate_flag              int comment '是否设置商品交易扣点0:未设置 1:已设置',
    shop_sales_terr_temp_id  bigint comment '店铺销售区域模板ID',
    create_zipper_time       string comment '有效开始时间',
    end_zipper_time          string comment '有效结束时间'
)
PARTITIONED BY (
    dt string
    )
stored as parquet
location '/user/hive/warehouse/dwd.db/fact_item'
tblproperties ("orc.compression" = "snappy");

set
hive.exec.dynamici.partition=true;
set
hive.exec.dynamic.partition.mode=nonstrict;
--初始化商品拉链表
insert
overwrite table dwd.fact_item
select item_id,
       brand_id,
       cid,
       seller_id,
       shop_id,
       shop_cid,
       shop_freight_template_id,
       attributes,
       attr_sale,
       status,
       type,
       item_name,
       shelve_time,
       off_shelve_time,
       task_shelve_time,
       task_off_shelve_time,
       origin,
       weight,
       volume,
       length,
       width,
       height,
       ad,
       keyword,
       remark,
       unit_code,
       unit_name,
       quotation_way,
       create_time,
       create_user,
       update_time,
       update_user,
       yn,
       sign,
       status_change_reason,
       platform,
       give_away,
       pay_type,
       sale_channel,
       points,
       upc,
       goods_code,
       attr_template_id,
       `describe`,
       ad_url,
       source_item_id,
       master_item_id,
       rebate_flag,
       shop_sales_terr_temp_id,
       case
           when update_time is not null
               then to_date(update_time)
           else to_date(create_time)
           end      as create_zipper_time,
       '9999-12-31' as end_zipper_time,
       date_format(create_time, 'yyyyMMdd')
from ods.ods_item


create
external table dwd.fact_orders
(
    order_id               bigint comment '订单编号',
    parent_order_id        bigint comment '父订单编号',
    order_type             int comment '订单类型
     * 1 普通订单
     * 2 预售订单
     * 3 线下订单
     * 4 积分订单
     * 5 无价格订单
     * 6 B端采购订单 (手工单-向虚拟店铺购买)
     * 7 B端销售订单 (手工单-向虚拟门店销售)
     * 8 B端采购订单 (正常单-向真实店铺购买)
     * 9 门店手工创建销售订单（暂时作废）
     * 10 客户预下单(定制)',
    status                 int comment '订单状态 0:系统处理中、1:待付款、2:待配送、3:待收货、4:待评价(已无效由其他字段控制)、5:已完成、6:已取消、7:已关闭、8:已删除、9:订单过期、10:待付尾款、11:未付尾款、12:锁单（冻结）、13:待付定金、14:待审核、15:已审核 、16:已驳回、17:待仓库发货',
    buyer_status           int comment '买家状态',
    seller_type            int comment '销售类型 1普通 2定制',
    order_platform         string comment '所属平台',
    order_source           string comment '订单的来源
（1）PC商城
（2）H5商城',
    total_money            DOUBLE comment '订单总金额',
    freight_money          DOUBLE comment '订单总运费',
    discount_money         DOUBLE comment '订单优惠总金额',
    cut_money              DOUBLE comment '分切总金额',
    other_fee              DOUBLE comment '其他费用',
    round_down             DOUBLE comment '抹零金额',
    actually_payment_money DOUBLE comment '应支付的的总金额',
    buyer_id               bigint comment '买家id',
    buyer_name             string comment '买家账号',
    buyer_shop_id          bigint comment '买家店铺ID',
    buyer_shop_name        string comment '买家店铺名称',
    seller_id              bigint comment '卖家id',
    seller_name            string comment '卖家账号',
    shop_id                bigint comment '店铺ID',
    shop_name              string comment '店铺名称',
    `option`               string comment '订单标记',
    paid                   int comment '支付状态 1：未支付 2：已支付',
    payment_source         string comment '支付渠道',
    payment_time           string comment '付款时间',
    refund                 int comment '是否售后  0未退款退货  1已退款退货',
    `exchange`             int comment '是否换货  0未换货  1换货',
    invoice                int comment '是否需要发票 —— 0不需要  1普通发票  2 增值税发票',
    buyer_memo             string comment '买家备注',
    seller_memo            string comment '卖家备注',
    is_change_price        int comment '是否改价  —— 0未改价 1已改价',
    settle_flag            int comment '结算标识',
    evaluation             string comment '评价状态(1:无,2:是)',
    create_time            string comment '创建时间',
    modify_time            string comment '修改时间',
    create_user            bigint comment '创建者',
    modify_user            bigint comment '修改者',
    deposit                DOUBLE comment '预售定金',
    retainage              DOUBLE comment '预售尾款',
    retainage_order_id     bigint comment '尾款支付订单编号',
    presell_id             bigint comment '预售Id',
    presell_pay_type       int comment '预售付款类型：0：仅全款，1：仅定金',
    order_credit           int comment '订单使用积分',
    yn                     int comment '是否有效： 0无效  1有效
（注：该字段做彻底删除用）',
    manage_user_id         bigint comment '经销商业务员用户ID(卖方业务员ID)',
    manage_username        string comment '经销商业务员用户名(卖方业务员名称)',
    buyer_manage_user_id   bigint comment '买方业务员ID',
    buyer_manage_username  string comment '买方业务员名称',
    purchase_date          string comment '采购日期',
    warehouse_code         string comment '仓库编码(买方入库仓库)',
    warehouse_name         string comment '仓库名称(买方入库仓库)',
    reason                 string comment '驳回原因',
    audit_time             string comment '审核时间',
    audit_user_id          int comment '审核人ID',
    audit_username         string comment '审核人用户名',
    remark                 string comment '备注',
    seller_org_code        string comment '归属卖家组织机构编码',
    seller_org_parent_code string comment '归属卖家上级组织机构编码',
    buyer_org_code         string comment '归属买家组织机构编码',
    buyer_org_parent_code  string comment '归属买家上级组织机构编码',
    delivery_type          int comment '配送方式 1：自提 2：汽运',
    print_price            int comment '是否打印价格： 1，否；2，是',
    consignment            int comment '发货完成标识：1，否；2，是',
    store_complete         int comment '入库完成标识：1，否；2，是',
    balance_amount         DOUBLE comment '余额支付金额',
    balance_flag           int comment '余额支付标识（0，否；1，是）',
    issue_flag             int comment '订单下发标识 0：未下发 1：已下发 2：下发失败 3:无仓发货（目前只针对c端订单）',
    self_pick_flag         int comment '1:自提订单 0:非自提订单',
    expect_receive_time    string comment '期望收货时间',
    delivery_remark        string comment '发货备注',
    create_zipper_time     string comment '有效开始时间',
    end_zipper_time        string comment '有效结束时间'
) COMMENT '订单拉链表'
PARTITIONED BY (
    dt string
    )
stored as parquet
location '/user/hive/warehouse/dwd.db/fact_orders'
tblproperties ("orc.compression" = "snappy");
--动态分区 在spark-sql中执行
set
hive.exec.dynamici.partition=true;
set
hive.exec.dynamic.partition.mode=nonstrict;
--初始化拉链表
insert
overwrite table dwd.fact_orders
select order_id,
       parent_order_id,
       order_type,
       status,
       buyer_status,
       seller_type,
       order_platform,
       order_source,
       total_money,
       freight_money,
       discount_money,
       cut_money,
       other_fee,
       round_down,
       actually_payment_money,
       buyer_id,
       buyer_name,
       buyer_shop_id,
       buyer_shop_name,
       seller_id,
       seller_name,
       shop_id,
       shop_name,
       `option`,
       paid,
       payment_source,
       payment_time,
       refund,
       `exchange`,
       invoice,
       buyer_memo,
       seller_memo,
       is_change_price,
       settle_flag,
       evaluation,
       create_time,
       modify_time,
       create_user,
       modify_user,
       deposit,
       retainage,
       retainage_order_id,
       presell_id,
       presell_pay_type,
       order_credit,
       yn,
       manage_user_id,
       manage_username,
       buyer_manage_user_id,
       buyer_manage_username,
       purchase_date,
       warehouse_code,
       warehouse_name,
       reason,
       audit_time,
       audit_user_id,
       audit_username,
       remark,
       seller_org_code,
       seller_org_parent_code,
       buyer_org_code,
       buyer_org_parent_code,
       delivery_type,
       print_price,
       consignment,
       store_complete,
       balance_amount,
       balance_flag,
       issue_flag,
       self_pick_flag,
       expect_receive_time,
       delivery_remark,
       case
           when modify_time is not null
               then to_date(modify_time)
           else to_date(create_time)
           end      as create_zipper_time,
       '9999-12-31' as end_zipper_time,
       date_format(create_time, 'yyyyMMdd')
from ods.ods_orders;

--订单明细拉链表
create
external table dwd.fact_orders_detail
(
    `id`                  bigint,
    `order_id`            bigint comment '订单的主键ID',
    `item_id`             bigint comment '商品ID',
    `cid`                 bigint comment '商品的类目ID',
    `brand_id`            bigint,
    `sku_id`              bigint comment 'skuid',
    `sku_code`            varchar(255) comment '商家SKU编码',
    `item_name`           varchar(100) comment '商品名称',
    `sku_pic_url`         varchar(255) comment '商品图片',
    `sku_sale_attr_str`   varchar(255) comment '商品销售属性中文名称',
    `item_original_price` decimal(10, 2) comment '商品的原始价格',
    `cost_price`          decimal(10, 2) comment '成本价',
    `payment_total_money` decimal(10, 2) comment '支付的总价格（不含运费）',
    `payment_price`       decimal(10, 2) comment '支付的单价（不含运费）',
    `cut_price`           decimal(16, 4) comment '分切单价',
    `cut_price_total`     decimal(16, 4) comment '分切总价',
    `refund`              int comment '退款标记',
    `exchange`            int comment '是否换货  0未换货  1换货',
    `num`                 decimal(24, 8) comment '商品数量',
    `sheet_num`           int comment '张数',
    `reel`                int comment '卷数',
    `discount_money`      decimal(10, 2) comment '总的优惠金额',
    `freight_template_id` bigint comment '运费模板ID',
    `delivery_type`       int comment '运送方式',
    `seller_id`           bigint,
    `shop_id`             bigint,
    `buyer_item_id`       bigint comment '买方商品编码',
    `buyer_sku_id`        bigint comment '买方SKUID',
    `buyer_sku_code`      varchar(255) comment '买方商家SKU编码',
    `evaluation`          varchar(20) comment '订单商品评价状态1为评价，2以评价',
    `create_time`         string comment '创建时间',
    `modify_time`         string comment '修改时间',
    `create_user`         bigint comment '创建者',
    `modify_user`         bigint comment '修改者',
    `is_gift`             int comment '是否是赠品  1：是  0，不是',
    `inbound_num`         decimal(16, 4) comment '入库数量',
    `outbound_num`        decimal(16, 4) comment '出库数量',
    `weight_unit`         int comment '克重',
    `width_unit`          int comment '幅宽',
    `length_unit`         int comment '长度',
    `purchase_num`        decimal(16, 4) comment '采购量',
    `divided_balance`     decimal(14, 2) comment '分摊的余额金额',
    `delivery_date`       string comment '交货时间',
    `urgent_type`         int comment '是否加急 0不加急 1加急',
    `already_cut`         int comment '是否已分切 0未分切 1已分切',
    `already_outbound`    int comment '是否已出库 0未出库 1已出库',
    `work_order_no`       varchar(64) comment '工单号',
    `create_zipper_time`     string comment '有效开始时间',
    `end_zipper_time`        string comment '有效结束时间'
) comment '订单明细表'
PARTITIONED BY (
dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/fact_orders_detail'
tblproperties ("orc.compression" = "snappy");


--初始化拉链表
insert
overwrite table dwd.fact_orders_detail
select id,
       order_id,
       item_id,
       cid,
       brand_id,
       sku_id,
       sku_code,
       item_name,
       sku_pic_url,
       sku_sale_attr_str,
       item_original_price,
       cost_price,
       payment_total_money,
       payment_price,
       cut_price,
       cut_price_total,
       refund,
       exchange,
       num,
       sheet_num,
       reel,
       discount_money,
       freight_template_id,
       delivery_type,
       seller_id,
       shop_id,
       buyer_item_id,
       buyer_sku_id,
       buyer_sku_code,
       evaluation,
       create_time,
       modify_time,
       create_user,
       modify_user,
       is_gift,
       inbound_num,
       outbound_num,
       weight_unit,
       width_unit,
       length_unit,
       purchase_num,
       divided_balance,
       delivery_date,
       urgent_type,
       already_cut,
       already_outbound,
       work_order_no,
       case
           when modify_time is not null
               then to_date(modify_time)
           else to_date(create_time)
           end      as create_zipper_time,
       '9999-12-31' as end_zipper_time,
       date_format(create_time, 'yyyyMMdd')
from ods.ods_orders_detail;


--收货地址拉链表
create table dwd.fact_orders_receive
(
    id                 bigint comment '主键ID',
    order_id           bigint comment '订单主键ID',
    consignee_name     string comment '收货人姓名',
    consignee_mobile   string comment '收货人手机',
    consignee_phone    string comment '收货人固定电话',
    consignee_mail     string comment '收货人邮箱',
    province_code      bigint comment '省ID',
    city_code          bigint comment '市ID',
    country_code       bigint comment '县区ID',
    town_code          bigint,
    province_name      string comment '省名称',
    city_name          string comment '市名称',
    country_name       string comment '区名称',
    town_name          string,
    detail_address     string comment '详细地址',
    create_time        string comment '创建时间',
    modify_time        string comment '修改时间',
    create_user        bigint comment '创建者',
    modify_user        bigint comment '修改者',
    yn                 int comment '是否有效',
    create_zipper_time string comment '有效开始时间',
    end_zipper_time    string comment '有效结束时间'
) comment '订单收货信息拉链表'
PARTITIONED BY (
    dt string
    )
stored as parquet
location '/user/hive/warehouse/dwd.db/fact_orders_receive'
tblproperties ("orc.compression" = "snappy");

--初始化拉链表
insert
overwrite table dwd.fact_orders_receive
select id,
       order_id,
       consignee_name,
       consignee_mobile,
       consignee_phone,
       consignee_mail,
       province_code,
       city_code,
       country_code,
       town_code,
       province_name,
       city_name,
       country_name,
       town_name,
       detail_address,
       create_time,
       modify_time,
       create_user,
       modify_user,
       yn,
       case
           when modify_time is not null
               then to_date(modify_time)
           else to_date(create_time)
           end      as create_zipper_time,
       '9999-12-31' as end_zipper_time,
       date_format(create_time, 'yyyyMMdd')
from ods.ods_orders_receive


------退款拉链表
create
external table dwd.fact_refund_detail
(
    id                 bigint comment '主键ID',
    refund_id          bigint comment '退款退货表的主键ID',
    order_id           bigint comment '订单ID',
    order_detail_id    bigint comment '订单详情的主键ID',
    item_id            bigint comment '商品的ID',
    item_name          string comment '商品名称',
    sku_id             bigint comment 'skuID',
    sku_code           string comment '商家SKU编码',
    sku_pic_url        string comment '商品图片',
    sku_sale_attr_str  string comment '商品销售属性中文名称',
    num                double comment '数量',
    transaction_money  double comment '交易金额',
    refund_money       double comment '退款金额',
    refund_num         double comment '退款退货数量',
    refund_price       double comment '退款单价',
    notes              string comment '备注',
    create_time        string comment '创建时间',
    modify_time        string comment '修改时间',
    create_user        bigint comment '创建者',
    modify_user        bigint comment '修改者',
    yn                 int comment '是否有效',
    buyer_item_id      bigint comment '买方商品编码',
    buyer_sku_id       bigint comment '买方SKUID',
    buyer_sku_code     string comment '买方商家SKU编码',
    inbound_num        double comment '入库数量',
    outbound_num       double comment '出库数量',
    divided_balance    double comment '余额金额',
    create_zipper_time string comment '有效开始时间',
    end_zipper_time    string comment '有效结束时间'
)
    comment '退货退款明细拉链表'
    PARTITIONED BY (
        dt string
        )
    stored as parquet
    location '/user/hive/warehouse/dwd.db/fact_refund_detail'
    tblproperties ("orc.compression" = "snappy");
---------初始化退货拉链表

insert
overwrite table dwd.fact_refund_detail
select id,
       refund_id,
       order_id,
       order_detail_id,
       item_id,
       item_name,
       sku_id,
       sku_code,
       sku_pic_url,
       sku_sale_attr_str,
       num,
       transaction_money,
       refund_money,
       refund_num,
       refund_price,
       notes,
       create_time,
       modify_time,
       create_user,
       modify_user,
       yn,
       buyer_item_id,
       buyer_sku_id,
       buyer_sku_code,
       inbound_num,
       outbound_num,
       divided_balance,
       case
           when modify_time is not null
               then to_date(modify_time)
           else to_date(create_time)
           end      as create_zipper_time,
       '9999-12-31' as end_zipper_time,
       date_format(create_time, 'yyyyMMdd')
from ods.ods_refund_detail


------退款拉链表
create
external table dwd.fact_refund_apply
(
    id                     bigint comment '主键',
    refund_no              string comment '退款退货编号',
    type                   int comment '退单类型：1，零售退单； 2，采购退单（手工单退单，订单类型6）； 3，销售退单（手工单退单，订单类型7）； 4，采购退单（正常单，订单类型8）； 5，销售退单（正常单，订单类型8）',
    refund_type            int comment '退款退货类型：（1）退款，（2）退货 ，（3）换货',
    order_id               bigint comment '订单的主键ID',
    order_status           int comment '订单的状态',
    refund_status          int comment '退款退货(换货)状态：
(1) 退款(换货)申请等待卖家确认中，
(2)卖家不同意协议,等待买家修改,
(3)退款(换货)申请达成,等待买家发货,
(4)买家已退货,等待卖家确认收货,
(5)退款(换货)关闭,
(6)退款(换货)成功,
(7)退款中',
    refund_reason          string comment '退款退货原因',
    question_description   string comment '问题描述',
    receiving              int comment '是否收到货：1收到；2：未收到',
    audit_id               bigint comment '审核者ID',
    audit_name             string comment '审核者姓名',
    audit_time             string comment '审核时间',
    audit_notes            string comment '审核意见备注',
    refund_total_money     string comment '可退的总金额',
    apply_refund_money     string comment '申请的退款金额（含余额支付时，仅表示第三方支付的金额。总退款金额=apply_refund_money+balance_amout）',
    province_code          bigint comment '省编码',
    province_name          string comment '省名称',
    city_code              bigint comment '市编码',
    city_name              string comment '市名称',
    country_code           bigint comment '区/县编码',
    country_name           string comment '区/县名称',
    town_code              bigint comment '乡/镇编码',
    town_name              string comment '乡/镇名称',
    detail_address         string comment '详细地址',
    refund_address         string comment '退货地址',
    refund_receiver        string comment '退货收货人',
    refund_mobile          string comment '退货收货人电话',
    refund_directions      string comment '退款说明',
    create_time            string comment '创建时间',
    modify_time            string comment '修改时间',
    create_user            bigint comment '创建者',
    modify_user            bigint comment '修改者',
    yn                     int comment '是否有效',
    manage_user_id         bigint comment '经销商业务员用户ID(卖方业务员ID)',
    manage_username        string comment '经销商业务员用户名(卖方业务员名称)',
    seller_org_code        string comment '归属卖家组织机构编码',
    seller_org_parent_code string comment '归属卖家上级组织机构编码',
    buyer_manage_user_id   bigint comment '买方业务员ID',
    buyer_manage_username  string comment '买方业务员名称',
    buyer_org_code         string comment '归属买家组织机构编码',
    buyer_org_parent_code  string comment '归属买家上级组织机构编码',
    create_flag            int comment '创建标识 1：买方创建 2：卖方创建',
    buyer_id               bigint comment '买家id',
    buyer_name             string comment '买家账号',
    buyer_shop_id          bigint comment '买家店铺ID',
    buyer_shop_name        string comment '买家店铺名称',
    seller_id              bigint comment '卖家id',
    seller_name            string comment '卖家账号',
    shop_id                bigint comment '店铺ID',
    shop_name              string comment '店铺名称',
    inbound_num            int comment '入库总数量',
    outbound_num           int comment '出库总数量',
    all_item_num           int comment '订单总商品数量',
    consignment            int comment '发货完成标识：1，否；2，是',
    store_complete         int comment '入库完成标识：1，否；2，是',
    balance_amount         string comment '余额金额',
    is_create_bound_bill   int comment '是否创建入库单：0，否；1，是',
    create_zipper_time     string comment '有效开始时间',
    end_zipper_time        string comment '有效结束时间'
)
    comment '退款退货主表拉链表'
    PARTITIONED BY (
        dt string
        )
    stored as parquet
    location '/user/hive/warehouse/dwd.db/fact_refund_apply'
    tblproperties ("orc.compression" = "snappy");


insert
overwrite table dwd.fact_refund_apply
select id,
       refund_no,
       `type`,
       refund_type,
       order_id,
       order_status,
       refund_status,
       refund_reason,
       question_description,
       receiving,
       audit_id,
       audit_name,
       audit_time,
       audit_notes,
       refund_total_money,
       apply_refund_money,
       province_code,
       province_name,
       city_code,
       city_name,
       country_code,
       country_name,
       town_code,
       town_name,
       detail_address,
       refund_address,
       refund_receiver,
       refund_mobile,
       refund_directions,
       create_time,
       modify_time,
       create_user,
       modify_user,
       yn,
       manage_user_id,
       manage_username,
       seller_org_code,
       seller_org_parent_code,
       buyer_manage_user_id,
       buyer_manage_username,
       buyer_org_code,
       buyer_org_parent_code,
       create_flag,
       buyer_id,
       buyer_name,
       buyer_shop_id,
       buyer_shop_name,
       seller_id,
       seller_name,
       shop_id,
       shop_name,
       inbound_num,
       outbound_num,
       all_item_num,
       consignment,
       store_complete,
       balance_amount,
       is_create_bound_bill,
       case
           when modify_time is not null
               then to_date(modify_time)
           else to_date(create_time)
           end      as create_zipper_time,
       '9999-12-31' as end_zipper_time,
       date_format(create_time, 'yyyyMMdd')
from ods.ods_refund_apply

---------------------------出库拉链表
create
external table dwd.fact_outbound_bill
(
    id                    bigint,
    type                  int             comment '出库单类型 1:销售出库 2：采购退货出库 3:手工创建 4:分切单原料出库 6:调拨出库 7:虚拟销售出库 8：虚拟采购出库  9:虚拟销售退货出库 10：虚拟采购退货出库 ',
    order_id              bigint          comment '订单号',
    shop_id               bigint          comment '店铺ID',
    shop_name             String    comment '店铺名称',
    buyer_shop_id         bigint          comment '买方店铺ID',
    buyer_shop_name       String    comment '买方店铺名称',
    status                int             comment '状态 0:待拣货 1：待出库 2：待审核 3：审核拒绝 4：待发运 5：已完成 6：已关闭 7:拒收待审核 8：拒收审核通过
 9 拒收已入库 10 差异签收待审核 11差异签收审核通过 12差异签收审核驳回',
    sign_status           int             comment '签收状态 0：未签收 1：已签收 2：已拒收',
    sign_time             String        comment '签收日期',
    sign_shop_id          bigint          comment '签收店铺ID',
    sign_user_id          bigint          comment '签收用户ID',
    real_sign_user        String    comment '实际签收人',
    sign_remark           String    comment '签收意见',
    order_amount          decimal(10, 2)  comment '订单金额',
    total_amount          decimal(16, 2)  comment '总金额',
    create_time           String        comment '创建时间',
    audit_time            String        comment '审核时间',
    audit_user_id         bigint          comment '审核人ID',
    audit_user_name       String    comment '审核人名称',
    audit_remark          String    comment '审核备注',
    outbound_time         String        comment '出库日期',
    modify_time           String        comment '修改时间',
    modify_user           bigint          comment '修改人',
    warehouse_code        String    comment '仓库编码',
    warehouse_name        String    comment '仓库名称',
    manage_user_id        bigint          comment '业务员ID',
    manage_username       String    comment '业务员名称',
    org_code              String    comment '归属组织机构编码',
    org_parent_code       String    comment '归属上级组织机构编码',
    outbound_remark       String    comment '出库备注',
    operate_user_id       bigint          comment '操作人用户id',
    operate_user_name     String    comment '操作人名称',
    operate_time          String        comment '操作时间',
    self_pick_flag        int  comment '0:非自提订单 1:自提订单 ',
    license_plate_num     String    comment '车牌号',
    driver_id             bigint          comment '司机ID',
    driver_name           String    comment '司机名称',
    driver_mobile         String    comment '司机手机号',
    logistics_code        String    comment '物流商ID,对应字典表code',
    logistics_no          String    comment '物流单号，多个单号就用逗号隔开',
    freight_label_num     int             comment '货签数',
    cut_order_id          String     comment '分切单编码',
    consignee_name        String     comment '收货人姓名',
    consignee_mobile      String     comment '收货人手机',
    consignee_mail        String     comment '收货人邮箱',
    province_code         bigint          comment '省ID',
    city_code             bigint          comment '市ID',
    country_code          bigint          comment '县区ID',
    town_code             bigint         ,
    province_name         String    comment '省名称',
    city_name             String    comment '市名称',
    country_name          String    comment '区名称',
    town_name             String   ,
    detail_address        String    comment '详细地址',
    expect_receive_time   String        comment '期望收货时间',
    reclaim_status        int             comment '回收状态：1，未回收；2，已回收',
    sign_voucher_pic      String   comment '签收凭证',
    in_warehouse_code     String   comment '入库仓库编码',
    in_warehouse_name     String   comment '入库仓库名称',
    cut_price_total       decimal(16, 2)  comment '分切总价',
    other_fee             decimal(16, 2)  comment '其他费用',
    freight_money         decimal(10, 2)  comment '运费',
    cost_price            decimal(16, 2)  comment '成本价 计算库存成本',
    industry              String    comment '行业类型',
    industry_item_type    bigint         comment '行业商品类型  1卷筒 2平张',
    create_user_id        bigint          comment '创建人id',
    shipment_time         String        comment '发运时间',
    buyer_manage_user_id  bigint          comment '采购员ID',
    buyer_manage_username String    comment '采购员名称',
   create_zipper_time     string comment '有效开始时间',
   end_zipper_time        string comment '有效结束时间'
)
comment '出库表'
PARTITIONED BY (
  dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/fact_outbound_bill'
tblproperties ("orc.compression"="snappy");

insert
overwrite table dwd.fact_outbound_bill
select id,
       type,
       order_id,
       shop_id,
       shop_name,
       buyer_shop_id,
       buyer_shop_name,
       status,
       sign_status,
       sign_time,
       sign_shop_id,
       sign_user_id,
       real_sign_user,
       sign_remark,
       order_amount,
       total_amount,
       create_time,
       audit_time,
       audit_user_id,
       audit_user_name,
       audit_remark,
       outbound_time,
       modify_time,
       modify_user,
       warehouse_code,
       warehouse_name,
       manage_user_id,
       manage_username,
       org_code,
       org_parent_code,
       outbound_remark,
       operate_user_id,
       operate_user_name,
       operate_time,
       self_pick_flag,
       license_plate_num,
       driver_id,
       driver_name,
       driver_mobile,
       logistics_code,
       logistics_no,
       freight_label_num,
       cut_order_id,
       consignee_name,
       consignee_mobile,
       consignee_mail,
       province_code,
       city_code,
       country_code,
       town_code,
       province_name,
       city_name,
       country_name,
       town_name,
       detail_address,
       expect_receive_time,
       reclaim_status,
       sign_voucher_pic,
       in_warehouse_code,
       in_warehouse_name,
       cut_price_total,
       other_fee,
       freight_money,
       cost_price,
       industry,
       industry_item_type,
       create_user_id,
       shipment_time,
       buyer_manage_user_id,
       buyer_manage_username,
       to_date(create_time) as create_zipper_time,
       '9999-12-31'         as end_zipper_time,
       date_format(create_time, 'yyyyMMdd')
from ods.ods_outbound_bill


-----------------------------------自提点
create
external table dwd.fact_orders_self_pick
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
    modify_user    bigint,
    create_zipper_time     string comment '有效开始时间',
    end_zipper_time        string comment '有效结束时间'
)
comment '订单自提信息-拉链表'
PARTITIONED BY (
  dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/fact_orders_self_pick'
tblproperties ("orc.compression"="snappy");

insert
overwrite table dwd.fact_orders_self_pick
select id,
       order_id,
       verification,
       pick_id,
       pick_name,
       province_code,
       province_name,
       city_code,
       city_name,
       country_code,
       country_name,
       town_code,
       town_name,
       detail_address,
       contact_phone,
       contact_name,
       confirm_time,
       confirm_user,
       create_time,
       create_user,
       modify_time,
       modify_user,
       to_date(create_time) as create_zipper_time,
       '9999-12-31'         as end_zipper_time,
       date_format(create_time, 'yyyyMMdd')
from ods.ods_orders_self_pick



create
external table dwd.dim_user
(
      id bigint                              ,
      platform String          comment '所属平台',
      tenant_id bigint          comment '租户id',
      seller_id bigint          comment '卖家ID',
      parent_id bigint          comment '父账号id(如果父账号id为1，则该账号为父账号，默认为1)',
      name String   comment '姓名',
      mobile String         comment '联系手机号',
      email String         comment '联系邮箱',
      nickname String         comment '昵称',
      sex int          comment '性别 1男 2女',
      birthday string          comment '生日',
      hobbies String          comment '兴趣爱好',
      icon String          comment '头像',
      type int        comment '用户类型（1-普通用户，2-买家，3-卖家, 4-平台）',
      flag int          comment '1:个人 2：企业 3超级管理员 4:店员',
      pay_password String          comment '买家支付密码',
      status int          comment '1-普通用户待验证，2-普通用户验证通过，3-买家待审核，4-买家审核通过，5-卖家待审核，6-卖家审核通过9:删除',
      create_time string          comment '创建时间',
      modify_time string          comment '更新时间',
      create_user bigint          comment '创建人id',
      modify_user bigint          comment '修改人id',
      failed_login_count int          comment '失败登录次数',
      yn int          comment '0:无效,1:有效',
      login_time string          comment '上次登录时间',
      login_num int          comment '登录次数',
      pay_password_safe int          comment '支付密码强度：1低、2中、3高',
      seller_pay_password String          comment '卖家支付密码',
      logout_time string          comment '注销时间',
      job_number String          comment '员工编号',
      remark String          comment '备注',
      dis_flag int          comment '分销标识 1申请 2通过',
      group_flag int          comment '团长标识 1申请 2通过',
      create_zipper_time     string comment '有效开始时间',
    end_zipper_time        string comment '有效结束时间'
)
comment '用户信息-拉链表'
PARTITIONED BY (
  dt string
)
stored as parquet
location '/user/hive/warehouse/dwd.db/dim_user'
tblproperties ("orc.compression"="snappy");

insert
overwrite table dwd.dim_user
select
id,
platform,
tenant_id,
seller_id,
parent_id,
name,
mobile,
email,
nickname,
sex,
birthday,
hobbies,
icon,
type,
flag,
pay_password,
status,
create_time,
modify_time,
create_user,
modify_user,
failed_login_count,
yn,
login_time,
login_num,
pay_password_safe,
seller_pay_password,
logout_time,
job_number,
remark,
dis_flag,
group_flag,
to_date(create_time) as create_zipper_time,
'9999-12-31'         as end_zipper_time,
date_format(create_time, 'yyyyMMdd')
from ods.ods_user