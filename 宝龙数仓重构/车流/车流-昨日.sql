-- 车流-昨日
select area_name,                                                                                                   -- 区域名称
       id,                                                                                                          -- 区域id
       bis_project_id,                                                                                              -- 项目id
       short_name,                                                                                                  -- 项目
       num_year,                                                                                                    -- 当前年
       max(today_flow)                                                                        today_flow,           -- 今日车流
       max(yestoday_flow)                                                                     yestoday_flow,        -- 昨日车流
       max(before_yestoday_flow)                                                              before_yestoday_flow, -- 前日车流
       round((max(yestoday_flow) - max(before_yestoday_flow)) / max(before_yestoday_flow), 2) hb,                   -- 前日车流环比
       null as                                                                                dcl                   -- 昨日车流达成率
from (
         select t2.area_name,
                t2.id, -- 区域id
                bis_project_id,
                short_name,
                num_year,
                str_date,
                case when str_date = date_format(current_date(), 'yyyyMMdd') then day_flow else 0 end as today_flow,
                case
                    when str_date = replace(date_sub(date_format(current_date(), 'yyyy-MM-dd'), 1), '-', '')
                        then day_flow
                    else 0 end                                                                        as yestoday_flow,
                case
                    when str_date = replace(date_sub(date_format(current_date(), 'yyyy-MM-dd'), 2), '-', '')
                        then day_flow
                    else 0 end                                                                        as before_yestoday_flow
         from (
                  select t2.project_id bis_project_id,
                         t2.name SHORT_NAME,
                         substr(substr(FROM_UNIXTIME(t1.in_at), 0, 10), 0, 4)     NUM_YEAR,
                         replace(substr(FROM_UNIXTIME(t1.in_at), 0, 10), '-', '') STR_DATE,
                         count(*)                                                 day_flow
                  from ods.ods_pl_blpark_traffic_dt t1
                           left join ods.ods_pl_blpark_park_dt t2 on t1.park_id = t2.park_id
                  where (replace(substr(FROM_UNIXTIME(t1.in_at), 0, 10), '-', '') = date_format(current_date(), 'yyyyMMdd') or
                         replace(substr(FROM_UNIXTIME(t1.in_at), 0, 10), '-', '') = replace(date_sub(date_format(current_date(), 'yyyy-MM-dd'), 1), '-', '')
                      or replace(substr(FROM_UNIXTIME(t1.in_at), 0, 10), '-', '') = replace(date_sub(date_format(current_date(), 'yyyy-MM-dd'), 2), '-', '')
                            )
                  group by t2.project_id ,  t2.name,  substr(substr(FROM_UNIXTIME(t1.in_at), 0, 10), 0, 4) , replace(substr(FROM_UNIXTIME(t1.in_at), 0, 10), '-', '')
              ) t
                  left join
              (
                  select bs_area.id,
                         bs_area.area_name,  -- 区域名称
                         bs_mall.area,
                         bs_mall.out_mall_id -- 项目id
                  from ods.ods_pl_pms_bis_db_bs_area_dt bs_area
                           left join ods.ods_pl_pms_bis_db_bs_mall_dt bs_mall
                                     on bs_area.id = bs_mall.area
                  where bs_mall.is_del = '0'
                    and stat = '2'
              ) t2 on t.bis_project_id = t2.out_mall_id
     ) t1
group by area_name, id, bis_project_id, SHORT_NAME, NUM_YEAR;




