--- 车流-折线图
select t1.year,     -- 年
       t1.month,    -- 月
       t1.in_sum,   -- 进客流
       t2.out_sum  -- 出客流
from (
         select substr(FROM_UNIXTIME(t1.in_at), 0, 4) year,
                substr(FROM_UNIXTIME(t1.in_at), 6, 2) month,
                count(*)                              in_sum
         from ods.ods_pl_blpark_traffic_dt t1
         group by substr(FROM_UNIXTIME(t1.in_at), 0, 4), substr(FROM_UNIXTIME(t1.in_at), 6, 2)
     ) t1
         left join
     (
         select substr(FROM_UNIXTIME(t1.out_at), 0, 4) year,
                substr(FROM_UNIXTIME(t1.out_at), 6, 2) month,
                count(*)                               out_sum
         from ods.ods_pl_blpark_traffic_dt t1
         group by substr(FROM_UNIXTIME(t1.out_at), 0, 4), substr(FROM_UNIXTIME(t1.out_at), 6, 2)
     ) t2 on t1.year = t2.year and t1.month = t2.month
