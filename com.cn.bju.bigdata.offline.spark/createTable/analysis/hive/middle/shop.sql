--商城表
create external table ods.ods_shop_info
(
    `shop_id` bigint          comment  '店铺id',
      `seller_id` bigint          comment '卖家id',
      `shop_name` varchar(200)          comment '店铺名称',
      `platform` varchar(20)          comment '所属平台',
      `status` int          comment '状态;1是申请，2是通过，3是驳回， 4是平台关闭，5是开通(启用) 6:停用 7:删除',
      `option` varchar(1000)          comment '标记',
      `operation_status` int          comment '营业状态（只能允许卖家操作，默认为不开启），1表示卖家开启店铺，2表示卖家关闭店铺',
      `domain` varchar(20)          comment '店铺域名',
      `logo` varchar(200)          comment '店铺logo',
      `type` int          comment '店铺类型 1：普通店铺(经销商) 2：门店 3:品牌商店铺 4:虚拟店铺',
      `intro` varchar(1000)          comment '介绍',
      `keyword` varchar(1000)          comment '关键字',
      `description` varchar(1000)          comment '主营业务描述',
      `open_time` string          comment '开通时间',
      `create_time` string          comment '创建时间',
      `create_user` bigint          comment '创建人',
      `update_time` string          comment '修改时间',
      `update_user` bigint          comment '修改人',
      `yn` int          comment '是否有效(0:无效,1:有效)',
      `month_num` int          comment '结算周期（按月）',
      `payment_status` int          comment '缴费状态 1：待确认，2：已确认',
      `autotrophy_type` int          comment '自营标志：1：自营,2：非自营',
      `company_name` varchar(255)          comment '公司名称',
      `shop_type` varchar(50)          comment '经营类型 1自营店 2旗舰店 3专营店',
      `customer_phone` varchar(20)          comment '客服电话',
      `shop_grade` varchar(20)          comment '平台店铺等级',
      `fax` varchar(16)          comment '传真号码',
      `postcode` varchar(6)          comment '邮编',
      `contact` varchar(64)          comment '联系人',
      `remark` varchar(255)          comment '备注',
      `province_code` bigint          comment '省编码',
      `province_name` varchar(255)          comment '省名称',
      `city_code` bigint          comment '市编码',
      `city_name` varchar(255)          comment '市名称',
      `country_code` bigint          comment '区县编码',
      `country_name` varchar(255)          comment '区县名称',
      `town_code` bigint          comment '乡镇编码',
      `town_name` varchar(255)          comment '乡镇名称',
      `detail_address` varchar(255)          comment '详细地址',
      `tax_number` varchar(255)          comment '税号',
      `bank_name` varchar(255)          comment '开户银行',
      `bank_account` varchar(255)          comment '银行账号',
      `manage_user_id` bigint          comment '业务员用户ID',
      `account_book_no` varchar(64)          comment '账套编码',
      `account_owner` varchar(255)          comment '开户人',
      `org_code` varchar(255)          comment '归属组织机构编码',
      `inventory_sync` int          comment '仓库库存与销售库存同步标识 0:不同步 1：同步',
      `plat_industry_rel_id` bigint         ,
      `industry` varchar(50)          comment '行业编码'
)comment '店铺信息表'
PARTITIONED BY (
  `dt` string
)
stored as parquet
location '/user/hive/warehouse/ods.db/ods_shop_info'
tblproperties ("orc.compression"="snappy");
ALTER TABLE ods.shop_info ADD IF NOT EXISTS PARTITION (dt='20210329') location '/user/hive/warehouse/ods.db/shop_info/dt=20210329/';

create table ods.ods_plat_industry_rel
(
     `id` bigint      ,
      `platform` varchar(50)     comment '所属平台',
      `platform_name` varchar(255)      ,
      `industry` varchar(100)     comment '行业',
      `industry_name` varchar(255)        ,
      `create_time` string        ,
      `update_time` string        ,
      `create_user` varchar(50)        ,
      `update_user` varchar(50)        ,
      `yn` int   comment '是否有效 1:有效 0:无效',
      `description` varchar(255)  comment '描述'
)comment '平台-行业关联表'
PARTITIONED BY (
  `dt` string
)
stored as parquet
location '/user/hive/warehouse/ods.db/ods_plat_industry_rel'
tblproperties ("orc.compression"="snappy");


ALTER TABLE ods.ods_plat_industry_rel ADD IF NOT EXISTS PARTITION (dt='20210325') location '/user/hive/warehouse/ods.db/ods_plat_industry_rel/dt=20210325/';
