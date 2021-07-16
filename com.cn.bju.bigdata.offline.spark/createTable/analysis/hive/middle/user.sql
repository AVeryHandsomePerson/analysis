create
external table ods.ods_refund_detail

create
external table ods.ods_user
(
    `id` bigint                              ,
      `platform` varchar(20)          comment '所属平台',
      `tenant_id` bigint          comment '租户id',
      `seller_id` bigint          comment '卖家ID',
      `parent_id` bigint          comment '父账号id(如果父账号id为1，则该账号为父账号，默认为1)',
      `name` varchar(128)        comment '姓名',
      `mobile` varchar(200)          comment '联系手机号',
      `email` varchar(200)          comment '联系邮箱',
      `nickname` varchar(255)          comment '昵称',
      `sex` int          comment '性别 1男 2女',
      `birthday` string          comment '生日',
      `hobbies` varchar(100)          comment '兴趣爱好',
      `icon` varchar(200)          comment '头像',
      `type` int        comment '用户类型（1-普通用户，2-买家，3-卖家, 4-平台）',
      `flag` int          comment '1:个人 2：企业 3超级管理员 4:店员',
      `pay_password` varchar(200)          comment '买家支付密码',
      `status` int          comment '1-普通用户待验证，2-普通用户验证通过，3-买家待审核，4-买家审核通过，5-卖家待审核，6-卖家审核通过9:删除',
      `create_time` string          comment '创建时间',
      `modify_time` string          comment '更新时间',
      `create_user` bigint          comment '创建人id',
      `modify_user` bigint          comment '修改人id',
      `failed_login_count` int          comment '失败登录次数',
      `yn` int          comment '0:无效,1:有效',
      `login_time` string          comment '上次登录时间',
      `login_num` int          comment '登录次数',
      `pay_password_safe` int          comment '支付密码强度：1低、2中、3高',
      `seller_pay_password` varchar(200)          comment '卖家支付密码',
      `logout_time` string          comment '注销时间',
      `job_number` varchar(255)          comment '员工编号',
      `remark` varchar(255)          comment '备注',
      `dis_flag` int          comment '分销标识 1申请 2通过',
      `group_flag` int          comment '团长标识 1申请 2通过'
)
comment '用户表'
PARTITIONED BY (
  dt string
)
stored as parquet
location '/user/hive/warehouse/ods.db/ods_user'
tblproperties ("orc.compression"="snappy");



stored
as PARQUET
location '/user/hive/warehouse/ods.db/ods_user'
tblproperties ("parquet.compression"="snappy");

ALTER TABLE ods.ods_user
    ADD IF NOT EXISTS PARTITION (dt='20210325') location '/user/hive/warehouse/ods.db/ods_user/dt=20210325/';


create table ods_user_statistics
(
    id                 bigint,
    vip_name           string ,
    shop_id            bigint ,
    user_id            bigint ,
    user_grade_id      bigint  comment '会员等级id关联',
    user_grade_code    int  comment '等级阶梯12345',
    grade_name         string ,
    vip_status         int  comment '会员状态 1正式会员 2预会员 3准会员',
    trade_num          bigint  comment '交易笔数',
    trade_money        decimal(16, 2)  comment '交易金额',
    initiation_time    string comment '入会时间',
    nearest_trade_time string comment '最近交易时间',
    cancel_order_num   int  comment '取消订单数',
    exchange_goods_num decimal(20, 4)  comment '换货数量',
    refund_money       decimal(16, 2)  comment '退款金额',
    refund_num         decimal(20, 4)  comment '退款数量',
    create_time        string ,
    modify_time        string ,
    exchange_order_num int  comment '换货订单数量',
    refund_order_num   int  comment '退款退货订单数量'
) comment '会员统计详情表'
PARTITIONED BY (
  dt string
)
stored as parquet
location '/user/hive/warehouse/ods.db/ods_user_statistics'
tblproperties ("orc.compression"="snappy");

create table ods.ods_shop_store
(
    id                bigint,
    seller_id         bigint         ,
    shop_id           bigint         ,
    store_seller_id   bigint         ,
    store_shop_id     bigint         ,
    store_shop_name   string   ,
    status            int             comment '关系状态 1：开启 2:停用 3：删除',
    type              int             comment '关系类型：1，虚拟门店（自建）；2，真实门店',
    contact_person    string   ,
    contact_phone     string   ,
    postcode          string   ,
    fax               string   ,
    account_book_no   string   ,
    province_code     bigint         ,
    province_name     string   ,
    city_code         bigint         ,
    city_name         string   ,
    country_code      bigint         ,
    country_name      string   ,
    town_code         bigint         ,
    town_name         string   ,
    address_detail    string   ,
    remark            string   ,
    manager_user_id   bigint         ,
    manager_username  string   ,
    org_id            bigint         ,
    org_code          string   ,
    org_parent_code   string   ,
    create_time       string       ,
    create_user       bigint         ,
    modify_time       string       ,
    modify_user       bigint         ,
    open_time         string       ,
    should_receipt    decimal(10, 2)  comment '应收',
    real_receipt      decimal(10, 2)  comment '实收',
    last_receipt_time string        comment '最后收款时间',
    store_level_code  varchar(255)    comment '门店级别编码(数据字典配置)',
    store_level_name  varchar(255)    comment '门店级别名称(数据字典配置)',
    store_type_code   varchar(255)    comment '门店类型编码',
    store_type_name   varchar(255)    comment '门店类型名称'
)
comment '我与我的的门店'
PARTITIONED BY (
  dt string
)
stored as parquet
location '/user/hive/warehouse/ods.db/ods_shop_store'
tblproperties ("orc.compression"="snappy");


create table ods.ods_shop_user_attention
(
    id          bigint,
    user_id     bigint,
    shop_id     bigint,
    create_time String,
    yn          int
)
comment '用户关注店铺'
PARTITIONED BY (
  dt string
)
stored as parquet
location '/user/hive/warehouse/ods.db/ods_shop_user_attention'
tblproperties ("orc.compression"="snappy");