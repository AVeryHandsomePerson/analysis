select a.pv  as "浏览量",
       a.uv  as "访客数",
       case
           when b.hour is null or b.hour = '' then a.hour
           when a.hour is null or a.hour = '' then b.hour
           when a.hour is not null and b.hour is not null then a.hour
           end as hour, b.orderNumber as "订单量", CAST(b.money as String) as "money", b.userNumber as "订单用户"
from (select count () pv, count(distinct case when loginToken == '' or loginToken is null then ip else loginToken end) as uv, concat(hour, ':00') as hour
      from dwd_page_log_all
      where shopId = '2000000027' and day = cast (timestamp_sub(DAY, 1, toDate(now())) as String) and hour >= '00'
      group by hour) a
         full join
     (
         select
             count(distinct case when paid = 2 and buyerId != 0 then buyerId end)  as userNumber,
             sum(case when paid = 1 then 0 else 1 end) as orderNumber,
             sum(case when paid = 1 then 0 else totalMoney - discountMoney end)  as money,
             concat(hour, ':00') AS hour
         from dwd_order_all final
         where shopId = CAST('2000000027' as Int64) and day = cast (timestamp_sub(DAY, 1, toDate(now())) as String) and hour >= '00'
         group by hour
         ) b
     on a.hour = b.hour order by hour



select
    count(distinct case when paid = 2 and buyerId != 0 then buyerId end)  as userNumber,
    sum(case when paid = 1 then 0 else 1 end) as orderNumber,
    sum(case when paid = 1 then 0 else totalMoney - discountMoney end)  as money
from dwd_order_all final
where shopId = 2000000272  and day = cast(toDate(now()) as String) and orderType=11

