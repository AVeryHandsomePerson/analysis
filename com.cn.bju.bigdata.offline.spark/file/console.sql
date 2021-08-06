select a.shopId, a.userNumber, a.orderNumber, a.totalMoney
from (


         select shopId,
                count(distinct multiIf(paid = 2, null, buyerId))      as userNumber,
                sum(multiIf(paid = 1, 0, 1))                          as orderNumber,
                sum(multiIf(paid = 1, 0, totalMoney - discountMoney)) as totalMoney
         from dwd_order_all
         where shopId = 2000000027
           and day = cast(toDate(now()) as String)
           and orderType = 1
         group by shopId


         ) a
         left join (
    select shopId,
           count(distinct multiIf(paid = 2, null, buyerId))      as userNumber,
           sum(multiIf(paid = 1, 0, 1))                          as orderNumber,
           sum(multiIf(paid = 1, 0, totalMoney - discountMoney)) as totalMoney
    from dwd_order_all
    where shopId = 2000000027
      and day = cast(toDate(now()) as String)
      and orderType != 1
    group by shopId
    ) b on a.shopId = b.shopId;



ALTER
TABLE
default.dwd_order_all
DELETE
WHERE groupLeaderShopId = '2000000428'
  and day = '2021-08-06';
----------  实时退款
drop table default.dwd_refund_apply_cluster on cluster bip_ck_cluster;
CREATE TABLE default.dwd_refund_apply_cluster on cluster bip_ck_cluster
(
    id                  Int64,
    orderId             Int64,
    shopId              String,
    orderStatus         Int64,
    refundStatus        Int64,
    refundReason        String,
    questionDescription String,
    refundTotalMoney    Double,
    applyRefundMoney    Double,
    updateTime          Int64,
    hour                String,
    day                 String
)
    ENGINE = ReplacingMergeTree(orderId)
        primary key orderId
        PARTITION BY (day, hour)
        ORDER BY (orderId, day, hour)
        SETTINGS index_granularity = 8192;
ALTER TABLE default.dwd_refund_apply_all
    ADD COLUMN applyRefundMoney String AFTER refundTotalMoney;
-- //创建分布式表
drop table dwd_refund_apply_all on cluster bip_ck_cluster;
create table dwd_refund_apply_all on cluster bip_ck_cluster
(
    id                  Int64,
    orderId             Int64,
    shopId              String,
    orderStatus         Int64,
    refundStatus        Int64,
    refundReason        String,
    questionDescription String,
    refundTotalMoney    Double,
    applyRefundMoney    Double,
    updateTime          Int64,
    hour                String,
    day                 String
)
    engine = Distributed(bip_ck_cluster, default, dwd_refund_apply_cluster, hiveHash(id));

drop table dwd_refund_apply_view;
CREATE MATERIALIZED VIEW dwd_refund_apply_view TO dwd_refund_apply_all
AS
SELECT *
FROM dwd_kafka_refund_apply;

-- 退款明细
CREATE TABLE dwd_kafka_refund_detail
(
    id          Int64,
    refundId    Int64,
    itemId      Int64,
    itemName    String,
    skuId       Int64,
    skuPicUrl   String,
    num         Decimal(24, 8),
    refundNum   Decimal(24, 8),
    refundPrice Decimal(10, 2),
    hour        String,
    day         String
)
    ENGINE = Kafka('bogon2:9092,bogon:9092,bogon3:9092', 'dwd_refund_detail', 'buryDot', 'JSONEachRow');
CREATE TABLE dwd_refund_detail_cluster on cluster bip_ck_cluster
(
    id          Int64,
    refundId    Int64,
    itemId      Int64,
    itemName    String,
    skuId       Int64,
    skuPicUrl   String,
    num         Decimal(24, 8),
    refundNum   Decimal(24, 8),
    refundPrice Decimal(10, 2),
    hour        String,
    day         String
)
    ENGINE = ReplacingMergeTree(id)
        primary key id
        PARTITION BY (day, hour)
        ORDER BY (id, day, hour)
        SETTINGS index_granularity = 8192;
-- //创建分布式表
create table dwd_refund_detail_all on cluster bip_ck_cluster
(
    id          Int64,
    refundId    Int64,
    itemId      Int64,
    itemName    String,
    skuId       Int64,
    skuPicUrl   String,
    num         Decimal(24, 8),
    refundNum   Decimal(24, 8),
    refundPrice Decimal(10, 2),
    hour        String,
    day         String
)
    engine = Distributed(bip_ck_cluster, default, dwd_refund_detail_cluster, hiveHash(id));


CREATE MATERIALIZED VIEW dwd_refund_detail_view TO dwd_refund_detail_all
AS
SELECT *
FROM dwd_kafka_refund_detail;

-- dwd_refund_detail


select *
from dwd_refund_apply_all final;

select *
from dwd_order_detail_all
where shopId = 2000000027
  and day = cast(toDate(now()) as String);
select *
from dwd_order_all
where shopId = 2000000027
  and day = cast(toDate(now()) as String);



select sum(paid_goods_number)     "总订单数",
       sum(money_number)          "总订单金额",
       sum(income_money)       as "总收入",
       count(distinct buyerId) as "下单用户"
from (
      select sum(a.num)                                                                                     as paid_goods_number,
             sum(paid_money_number) + b.freightMoney                                                        as money_number,
             cast(
                     sum((itemOriginal_price - costPrice) * cast(num as Decimal(10, 2))) as Decimal(10, 2)) as income_money,
             b.buyerId
      from (select orderId,
                   itemId,
                   itemName,
                   num,
                   num * paymentPrice as paid_money_number,
                   itemOriginal_price,
                   costPrice
            from dwd_order_detail_all
            where shopId = 2000000027
              and day = cast(toDate(now()) as String)) a
               left join
           (select orderId, freightMoney, buyerId
            from dwd_order_all
            where shopId = 2000000027
              and day = cast(toDate(now()) as String)
            group by orderId, freightMoney, buyerId) b
           on a.orderId = b.orderId
      group by a.orderId, b.freightMoney, b.buyerId
         );


-- 移动端H5-全部数据-实时
select concat(a.hour, ':00') as hour,
       b.totalMoney          as "总订单金额",
       b.orderNumber         as "总订单数",
       b.userNumber          as "总下单人数",
       a.income_money        as "总收入",
       c.uv                  as "浏览人数",
       c.pv                  as "浏览次数",
       b.newUser             as "新下单用户",
       d.refund_money        as "退款金额"
from (
         select shopId,
                hour,
                cast(
                        sum((itemOriginal_price - costPrice) * cast(num as Decimal(10, 2))) as Decimal(10, 2)) as income_money
         from dwd_order_detail_all
         where shopId = 2000000027
--          where shopId = cast(#{shop_id} as Int64)
           and day = cast(toDate(now()) as String)
         group by shopId, hour) a
         left join
     (select shopId,
             hour,
             count(distinct multiIf(paid = 2, null, buyerId))      as userNumber,
             sum(multiIf(paid = 1, 0, 1))                          as orderNumber,
             sum(multiIf(paid = 1, 0, totalMoney - discountMoney)) as totalMoney,
             sum(multiIf(paid = 1, 0, CAST(oldNewUser as UInt8)))  as newUser
      from dwd_order_all
      where shopId = 2000000027
        and day = cast(toDate(now()) as String)
      group by shopId, hour) b
     on a.shopId = b.shopId and a.hour = b.hour
         left join
     (
         select shopId,
                hour,
                count(1)                                                                                     as pv,
                count(distinct case when loginToken == '' or loginToken is null then ip else loginToken end) as uv
         from dwd_page_log_all
         where shopId = '2000000027'
           and day = cast(toDate(now()) as String)
         group by shopId, hour
         ) c
     on a.shopId = cast(c.shopId as Int64) and a.hour = c.hour
         left join (
    select shopId,
           hour,
           cast(
                   sum(case when refundStatus = 6 then refundTotalMoney else 0 end) as Decimal(10, 2)) as refund_money --成功退款金额
    from dwd_refund_apply_all
--     where shopId = #{shop_id}
    where shopId = '2000000027'
      and day = cast(toDate(now()) as String)
    group by shopId, hour
    ) d
                   on a.shopId = cast(d.shopId as Int64) and a.hour = d.hour
;
-- 移动端H5-全部数据-核心数据实时
select b.totalMoney   as "总订单金额",
       b.orderNumber  as "总订单数",
       b.userNumber   as "总下单人数",
       a.income_money as "总收入",
       c.uv           as "浏览人数",
       c.pv           as "浏览次数",
       b.newUser      as "新下单用户",
       d.refund_money as "退款金额"
from (
         select shopId,
                cast(
                        sum((itemOriginal_price - costPrice) * cast(num as Decimal(10, 2))) as Decimal(10, 2)) as income_money
         from dwd_order_detail_all
         where shopId = 2000000027
--          where shopId = cast(#{shop_id} as Int64)
           and day = cast(toDate(now()) as String)
         group by shopId) a
         left join
     (select shopId,
             count(distinct case when paid = 2 and buyerId != 0 then buyerId end) as userNumber,
             sum(case when paid = 1 then 0 else 1 end)                            as orderNumber,
             sum(case when paid = 1 then 0 else totalMoney - discountMoney end)   as totalMoney,
             sum(case when paid = 1 then 0 else CAST(oldNewUser as UInt8) end)    as newUser
      from dwd_order_all final
      where shopId = 2000000027
        and day = cast(toDate(now()) as String)
      group by shopId) b
     on a.shopId = b.shopId
         left join
     (
         select shopId,
                count(1)                                                                                     as pv,
                count(distinct case when loginToken == '' or loginToken is null then ip else loginToken end) as uv
         from dwd_page_log_all
         where shopId = '2000000027'
           and day = cast(toDate(now()) as String)
         group by shopId
         ) c
     on a.shopId = cast(c.shopId as Int64)
         left join (
    select shopId,
           cast(
                   sum(case when refundStatus = 6 then refundTotalMoney else 0 end) as Decimal(10, 2)) as refund_money --成功退款金额
    from dwd_refund_apply_all
--     where shopId = #{shop_id}
    where shopId = '2000000027'
      and day = cast(toDate(now()) as String)
    group by shopId
    ) d
                   on a.shopId = cast(d.shopId as Int64);



select sum(multiIf(CAST(oldNewUser as UInt8) != 1, 1, 0)) as "老用户",
       sum(CAST(oldNewUser as UInt8))                     as "新用户"
from dwd_order_all
where shopId = 2000000027
  and day = cast(toDate(now()) as String)
  and paid = 2;



select shopId,
       hour,
       cast(sum(case when refundStatus = 6 then refundTotalMoney else 0 end) as Decimal(10, 2)) as refund_money --成功退款金额
from dwd_refund_apply_all
where shopId = '2000000027'
  and day = cast(toDate(now()) as String)
group by shopId, hour;
select *
from dwd_refund_apply_all
where shopId = '2000000027'
  and day = cast(toDate(now()) as String);

select a.shopId,
       CAST(CAST(b.income_money as Decimal(10, 2)) /
            CAST(a.income_money + b.income_money as Decimal(10, 2)) as Decimal(10, 2)) as tb_ratio,
       CAST(CAST(a.income_money as Decimal(10, 2)) /
            CAST(a.income_money + b.income_money as Decimal(10, 2)) as Decimal(10, 2)) as tc_ratio
from (
         select shopId,
                CAST(sum(CAST((a.itemOriginal_price - a.costPrice) as Decimal(10, 2)) *
                         CAST(a.num as Decimal(10, 2))) as Decimal(10, 2)) as income_money
         from (
                  select *
                  from dwd_order_detail_all
                  where shopId = 2000000027
                    and day = cast(toDate(now()) as String)
                  ) a
                  inner join (
             select *
             from dwd_order_all final
             where shopId = 2000000027
               and day = cast(toDate(now()) as String)
               and orderType = 1
               and paid = 2
             ) c
                             on a.orderId = c.orderId and a.hour = c.hour
         group by shopId
         ) b
         left join
     (select shopId, sum(b.costPrice) as income_money
      from (select *
            from dwd_outbound_bill_detail_all
            where shopId = 2000000027
              and day = cast(toDate(now()) as String)) a
               left join
           (
               select *
               from dwd_order_detail_all
               where shopId = 2000000027
                 and day = cast(toDate(now()) as String)
               ) b
           on a.hour = b.hour and a.orderId = b.orderId
      group by shopId
         ) a
     on a.shopId = b.shopId;

-- 移动端H5-全部数据-收入分析-实时
select type as "收入类型", income_money as "收入", b.tb_ratio as "占比"
from (
         select shopId, '渠道收入' as type, sum(b.costPrice) as income_money
         from (select *
               from dwd_outbound_bill_detail_all
               where shopId = 2000000027
                 and day = cast(toDate(now()) as String)) a
                  left join
              (
                  select *
                  from dwd_order_detail_all
                  where shopId = 2000000027
                    and day = cast(toDate(now()) as String)
                  ) b
              on a.hour = b.hour and a.orderId = b.orderId
         group by shopId ) a
         left join (


    select shopId,
           CAST(CAST(b.income_money_tc as Decimal(10, 2)) /
                CAST(a.income_money_tb + b.income_money_tc as Decimal(10, 2)) as Decimal(10, 2)) as tc_ratio,
           CAST(CAST(a.income_money_tb as Decimal(10, 2)) /
                CAST(a.income_money_tb + b.income_money_tc as Decimal(10, 2)) as Decimal(10, 2)) as tb_ratio
    from (
             select shopId,
                    CAST(sum(CAST((a.itemOriginal_price - a.costPrice) as Decimal(10, 2)) *
                             CAST(a.num as Decimal(10, 2))) as Decimal(10, 2)) as income_money_tc
             from (
                      select *
                      from dwd_order_detail_all
                      where shopId = 2000000027
                        and day = cast(toDate(now()) as String)
                      ) a
                      inner join (
                 select *
                 from dwd_order_all final
                 where shopId = 2000000027
                   and day = cast(toDate(now()) as String)
                   and orderType = 1
                   and paid = 2
                 ) c
                                 on a.orderId = c.orderId and a.hour = c.hour
             group by shopId
             ) b
             left join
         ( select shopId, sum(b.costPrice) as income_money_tb
           from (select *
                 from dwd_outbound_bill_detail_all
                 where shopId = 2000000027
                   and day = cast(toDate(now()) as String)) a
                    left join
                (
                    select *
                    from dwd_order_detail_all
                    where shopId = 2000000027
                      and day = cast(toDate(now()) as String)
                    ) b
                on a.hour = b.hour and a.orderId = b.orderId
           group by shopId
             ) a
         on a.shopId = b.shopId


    ) b
                   on a.shopId = b.shopId
union all
select type as "收入类型", income_money as "收入", b.tc_ratio as "占比"
from (
         select shopId,
                '零售收入'                                                     as type,
                CAST(sum(CAST((a.itemOriginal_price - a.costPrice) as Decimal(10, 2)) *
                         CAST(a.num as Decimal(10, 2))) as Decimal(10, 2)) as income_money

         from (
                  select *
                  from dwd_order_detail_all
                  where shopId = 2000000027
                    and day = cast(toDate(now()) as String)
                  ) a
                  inner join (
             select *
             from dwd_order_all final
             where shopId = 2000000027
               and day = cast(toDate(now()) as String)
               and orderType = 1
               and paid = 2
             ) c
                             on a.orderId = c.orderId and a.hour = c.hour
         group by shopId
         ) a
         left join (
    select shopId,
           CAST(CAST(b.income_money_tc as Decimal(10, 2)) /
                CAST(a.income_money_tb + b.income_money_tc as Decimal(10, 2)) as Decimal(10, 2)) as tc_ratio,
           CAST(CAST(a.income_money_tb as Decimal(10, 2)) /
                CAST(a.income_money_tb + b.income_money_tc as Decimal(10, 2)) as Decimal(10, 2)) as tb_ratio
    from (
             select shopId,
                    CAST(sum(CAST((a.itemOriginal_price - a.costPrice) as Decimal(10, 2)) *
                             CAST(a.num as Decimal(10, 2))) as Decimal(10, 2)) as income_money_tc
             from (
                      select *
                      from dwd_order_detail_all
                      where shopId = 2000000027
                        and day = cast(toDate(now()) as String)
                      ) a
                      inner join (
                 select *
                 from dwd_order_all final
                 where shopId = 2000000027
                   and day = cast(toDate(now()) as String)
                   and orderType = 1
                   and paid = 2
                 ) c
                                 on a.orderId = c.orderId and a.hour = c.hour
             group by shopId
             ) b
             left join
         ( select shopId, sum(b.costPrice) as income_money_tb
           from (select *
                 from dwd_outbound_bill_detail_all
                 where shopId = 2000000027
                   and day = cast(toDate(now()) as String)) a
                    left join
                (
                    select *
                    from dwd_order_detail_all
                    where shopId = 2000000027
                      and day = cast(toDate(now()) as String)
                    ) b
                on a.hour = b.hour and a.orderId = b.orderId
           group by shopId
             ) a
         on a.shopId = b.shopId
    ) b
                   on a.shopId = b.shopId;

ALTER TABLE dws_shop_deal_info
    DROP PARTITION '2021-08-02';
select *
from shop_client_vip_info
where dt = '2021-07-28'
  and shop_id = '2000000091';

-- 小程序-团购实时数据
-- select count(distinct case when paid = 2 and buyerId != 0 then buyerId end)  as userNumber,
--        sum(case when paid = 1 then 0 else 1 end) as orderNumber,
--        sum(case when paid = 1 then 0 else totalMoney - discountMoney end)  as money
-- from default.dwd_order_all final
-- where shopId = 2000000192 and day = '2021-08-02'


select count(distinct case when paid = 2 and refund = 0 and buyerId != 0 then buyerId end)                as userNumber,
       sum(case when paid = 2 and refund = 0 then 1 else 0 end)                                           as orderNumber,
       sum(case when paid = 2 and refund = 0 then (totalMoney - discountMoney) + freightMoney else 0 end) as money
from default.dwd_order_all final
where groupLeaderShopId = '2000000428'
  and day = '2021-08-03';

select count(distinct case when paid = 2 and refund = 0 and buyerId != 0 then buyerId end)                      as userNumber,
       sum(case when paid = 2 and b.refundStatus = 6 then 1 else 0 end)                                         as orderNumber,
       sum(case when paid = 2 and b.refundStatus = 6 then totalMoney - discountMoney else 0 end) + freightMoney as money
from (select *
      from default.dwd_order_all final
      where groupLeaderShopId = '2000000428'
        and day = '2021-08-02'
         ) a
         left join
     (select orderId,
             refundStatus
      from default.dwd_refund_apply_all final
      where day = '2021-08-02'
         ) b
     on a.orderId = b.orderId
group by day, freightMoney


select a.pv,
       a.uv,
       case
           when b.hour is null or b.hour = '' then a.hour
           when a.hour is null or a.hour = '' then b.hour
           when a.hour is not null and b.hour is not null then a.hour
           end as hour,
       b.orderNumber,
       b.money,
       b.userNumber
from (select count()                                                                                         pv,
             count(distinct case when loginToken == '' or loginToken is null then ip else loginToken end) as uv,
             concat(hour, ':00')                                                                          as hour
      from dwd_page_log_all
      where shopId = '2000000027'
        and day = cast(toDate(now()) as String)
        and hour >= '00'
      group by hour) a
         full join
     (
         --             select
--             count (distinct case when paid = 2 and buyerId != 0 then buyerId end) as userNumber,
--             sum (case when paid = 1 then 0 else 1 end) as orderNumber,
--             sum (case when paid = 1 then 0 else (totalMoney - discountMoney)+freightMoney end) as money,
--             concat(hour, ':00') AS hour
--             from dwd_order_all final
--             where shopId = #{shopId} and day = cast (toDate(now()) as String)
--             group by hour
         select userNumber,
                orderNumber,
                money + discountMoney as money,
                hour
         from (select orderId,
                      count(distinct
                            case when paid = 2 and b.refundStatus != 6 and buyerId != 0 then buyerId end) as userNumber,
                      sum(case when paid = 2 and b.refundStatus != 6 then 1 else 0 end)                   as orderNumber,
                      sum(case when paid = 2 and b.refundStatus != 6 then totalMoney end)                 as money,
                      concat(hour, ':00')                                                                 AS hour
               from (
                        select hour, orderId, paid, refund, buyerId, totalMoney
                        from dwd_order_all final
                        where shopId = 2000000027
                          and day = cast(toDate(now()) as String)
                        ) a
                        left join
                    (select orderId,
                            refundStatus
                     from default.dwd_refund_apply_all final
                     where day = cast(toDate(now()) as String)
                        ) b
                    on a.orderId = b.orderId
               group by hour, orderId
                  ) a
                  left join
              (
                  select orderId, discountMoney
                  from dwd_order_all final
                  where shopId = 2000000027
                    and day = cast(toDate(now()) as String)
                  ) c
              on a.orderId = c.orderId
         group by hour
         ) b
     on a.hour = b.hour


select count(distinct case when paid = 2 and b.refundStatus != 6 and buyerId != 0 then buyerId end) as userNumber,
       sum(case when paid = 2 and b.refundStatus != 6 then 1 else 0 end)                            as orderNumber,
       sum(case when paid = 2 and b.refundStatus != 6 then totalMoney - discountMoney end) +
       freightMoney                                                                                 as money
from (
         select day,
                orderId,
                paid,
                refund,
                buyerId,
                totalMoney,
                discountMoney,
                freightMoney
         from dwd_order_all final
         where shopId = 2000000027
           and day = cast(toDate(now()) as String)
         ) a
         left join
     (select orderId,
             refundStatus
      from default.dwd_refund_apply_all final
      where day = cast(toDate(now()) as String)
         ) b
     on a.orderId = b.orderId
group by day, freightMoney


select count(distinct case when paid = 2 and b.refundStatus != 6 and buyerId != 0 then buyerId end) as userNumber,
       sum(case when paid = 2 and b.refundStatus != 6 then 1 else 0 end)                            as orderNumber,
       sum(case when paid = 2 and b.refundStatus != 6 then totalMoney end)                          as money
from (
         select day, orderId, paid, refund, buyerId, totalMoney, discountMoney
         from dwd_order_all final
         where groupLeaderShopId = '2000000428'
           and day = cast(toDate(now()) as String)
         ) a
         left join
     (select orderId,
             refundStatus
      from default.dwd_refund_apply_all final
      where day = cast(toDate(now()) as String)
        and refundStatus = 6
         ) b
     on a.orderId = b.orderId
group by day;

select orderId, refundStatus
from (
         select day, orderId, paid, refund, buyerId, totalMoney, discountMoney
         from dwd_order_all final
         where groupLeaderShopId = '2000000428'
           and day = cast(toDate(now()) as String)
         ) a
         left join
     (select orderId,
             refundStatus
      from default.dwd_refund_apply_all final
      where day = cast(toDate(now()) as String)
        and refundStatus = 6
         ) b
     on a.orderId = b.orderId


select userNumber,
       orderNumber,
       money + freightMoney as money
from (
         select count(distinct
                      case when paid = 2 and b.refundStatus != 6 and buyerId != 0 then buyerId end) as userNumber,
                sum(case when paid = 2 and b.refundStatus != 6 then 1 else 0 end)                   as orderNumber,
                sum(case when paid = 2 and b.refundStatus != 6 then totalMoney - discountMoney end) as money,
                sum(case when paid = 2 and b.refundStatus != 6 then freightMoney end)               as freightMoney
         from (
                  select day,
                         orderId,
                         paid,
                         refund,
                         buyerId,
                         totalMoney,
                         discountMoney,
                         freightMoney
                  from dwd_order_all final
                  where shopId = 2000000027
                    and day = cast(toDate(now()) as String)
                  ) a
                  left join
              (select orderId,
                      refundStatus
               from default.dwd_refund_apply_all final
               where day = cast(toDate(now()) as String)
                 and refundStatus = 6
                  ) b
              on a.orderId = b.orderId
         group by day
         ) select count(distinct case when paid = 2 and b.refundStatus != 6 and buyerId != 0 then buyerId end) as userNumber,
       sum(case when paid = 2 and b.refundStatus != 6 then 1 else 0 end)                            as orderNumber,
       sum(case when paid = 2 and b.refundStatus != 6 then totalMoney - discountMoney end) +
       freightMoney                                                                                 as money
from (
    select day, orderId, paid, refund, buyerId, totalMoney, discountMoney, freightMoney
    from dwd_order_all final
    where groupLeaderShopId = cast (#{shopId} as String) and day = cast (toDate(now()) as String)
    ) a
    left join
    (select orderId,
    refundStatus
    from default.dwd_refund_apply_all final
    where day = cast (toDate(now()) as String) and refundStatus = 6
    ) b
on a.orderId = b.orderId
group by day, freightMoney


select userNumber,
       orderNumber,
       money + freightMoney - refundTotalMoney as money
from (
      select count(distinct case when paid = 2 and b.refundStatus != 6 and buyerId != 0 then buyerId end) as userNumber,
             sum(case when paid = 2 and b.refundStatus != 6 then 1 else 0 end)                            as orderNumber,
             sum(case when paid = 2 then totalMoney - discountMoney end)                                  as money,
             sum(case when paid = 2 then freightMoney end)                                                as freightMoney,
             sum(case when b.refundStatus = 6 then c.reundMoney end)                                      as refundTotalMoney
      from (
               select orderId,
                      paid,
                      refund,
                      buyerId,
                      totalMoney,
                      discountMoney,
                      freightMoney
               from dwd_order_all final
               where groupLeaderShopId = '2000000428'
                 and day = cast(toDate(now()) as String)
               ) a
               left join
           (select id,
                   orderId,
                   refundStatus
            from default.dwd_refund_apply_all final
            where day = cast(toDate(now()) as String)
              and refundStatus = 6
               ) b
           on a.orderId = b.orderId
               left join (
          select refundId,
                 CAST((refundNum * refundPrice) as Decimal(10, 2)) as reundMoney
          from dwd_refund_detail_all
          where day = cast(toDate(now()) as String)
          ) c
                         on b.id = c.refundId
         )
;


select a.pv,
       a.uv,
       case
           when b.hour is null or b.hour = '' then a.hour
           when a.hour is null or a.hour = '' then b.hour
           when a.hour is not null and b.hour is not null then a.hour
           end as hour,
       case
           when b.orderNumber is null then 0
           else b.orderNumber
           end as orderNumber,
       case
           when b.money is null then 0
           else b.money
           end as money,
       case
           when b.userNumber is null then 0
           else b.userNumber
           end as userNumber
from (select count()                                                                                         pv,
             count(distinct case when loginToken == '' or loginToken is null then ip else loginToken end) as uv,
             concat(hour, ':00')                                                                          as hour
      from dwd_page_log_all
      where shopId = '${shopId}'
        and day = cast(toDate(now()) as String)
        and hour >= '00'
      group by hour) a
         full join
     (
         select hour,
                userNumber,
                orderNumber,
                money + freightMoney - refundTotalMoney as money
         from (
               select count(distinct
                            case when paid = 2 and b.refundStatus != 6 and buyerId != 0 then buyerId end) as userNumber,
                      sum(case when paid = 2 and b.refundStatus != 6 then 1 else 0 end)                   as orderNumber,
                      sum(case when paid = 2 then totalMoney - discountMoney end)                         as money,
                      sum(case when paid = 2 then freightMoney end)                                       as freightMoney,
                      sum(case when b.refundStatus = 6 then c.reundMoney end)                             as refundTotalMoney,
                      hour
               from (
                        select hour,
                               orderId,
                               paid,
                               refund,
                               buyerId,
                               totalMoney,
                               discountMoney,
                               freightMoney
                        from dwd_order_all final
                        where groupLeaderShopId = '2000000428'
                          and day = cast(toDate(now()) as String)
                        ) a
                        left join
                    (select id,
                            orderId,
                            refundStatus
                     from default.dwd_refund_apply_all final
                     where day = cast(toDate(now()) as String)
                       and refundStatus = 6
                        ) b
                    on a.orderId = b.orderId
                        left join (
                   select refundId,
                          CAST((refundNum * refundPrice) as Decimal(10, 2)) as reundMoney
                   from dwd_refund_detail_all
                   where day = cast(toDate(now()) as String)
                   ) c
                                  on b.id = c.refundId
               group by hour
                  )
         ) b
     on
         a.hour = b.hour

select userNumber,
       orderNumber,
       money + freightMoney - refundTotalMoney as money
from (
      select count(distinct case when paid = 2 and b.refundStatus != 6 and buyerId != 0 then buyerId end) as userNumber,
             sum(case when paid = 2 and b.refundStatus != 6 then 1 else 0 end)                            as orderNumber,
             sum(case when paid = 2 then totalMoney - discountMoney end)                                  as money,
             sum(case when paid = 2 then freightMoney end)                                                as freightMoney,
             case
                 when sum(case when b.refundStatus = 6 then b.refundMoney end) is null then 0
                 else
                     sum(case when b.refundStatus = 6 then b.refundMoney end) end                         as refundTotalMoney
      from (
               select day,
                      orderId,
                      paid,
                      refund,
                      buyerId,
                      totalMoney,
                      discountMoney,
                      freightMoney
               from dwd_order_all final
               where groupLeaderShopId = '2000000428'
                 and day = cast(toDate(now()) as String)
               ) a
               left join
           (
               select
                   orderId,
                   refundStatus,
                   sum(refundMoney) as refundMoney
               from
                   (select id,
                           orderId,
                           refundStatus
                    from default.dwd_refund_apply_all final
                    where day = cast(toDate(now()) as String)
                      and refundStatus = 6
                       ) b
                       inner join (
                       select refundId,
                              CAST((refundNum * refundPrice) as Decimal (10, 2)) as refundMoney
                       from dwd_refund_detail_all final
                       where day = cast (toDate(now()) as String)
                       ) c
                                  on b.id = c.refundId
               group by orderId,refundStatus
         ) b
           on a.orderId = b.orderId
         );


