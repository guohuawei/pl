-- 车流-当月

select t2.area_name,                                                 -- 区域名称
       t2.id,                                                        -- 区域id
       t.bis_project_id,                                             -- 项目id
       t.short_name,                                                 -- 项目名称
       t.year,                                                       -- 年
       t.month,                                                      -- 月
       t.in_flow_total,                                              -- 月总进客流
       t.out_flow_total,                                             -- 月总出客流
       bis_project.park_num,                                         -- 停车车位数量
       t1.dayNum,                                                    -- 每个月有多少天,
       round(t.in_flow_total / t1.dayNum / bis_project.park_num, 2), -- 周转率
       null                                                          -- 月车流指标
from (
         select t1.bis_project_id, -- 项目id
                t1.short_name,     -- 项目名称
                t1.year,           -- 年
                t1.month,          -- 月
                t1.in_flow_total,  -- 进客流
                t2.out_flow_total  -- 出客流
         from (
                  select t2.project_id                         bis_project_id,
                         t2.name                               short_name,
                         substr(FROM_UNIXTIME(t1.in_at), 0, 4) year,
                         substr(FROM_UNIXTIME(t1.in_at), 6, 2) month,
                         count(*)                              in_flow_total
                  from ods.ods_pl_blpark_traffic_dt t1
                           left join ods.ods_pl_blpark_park_dt t2 on t1.park_id = t2.park_id
                  group by t2.project_id, t2.name, substr(FROM_UNIXTIME(t1.in_at), 0, 4),
                           substr(FROM_UNIXTIME(t1.in_at), 6, 2)
              ) t1
                  left join
              (
                  select t2.project_id                          bis_project_id,
                         t2.name                                short_name,
                         substr(FROM_UNIXTIME(t1.out_at), 0, 4) year,
                         substr(FROM_UNIXTIME(t1.out_at), 6, 2) month,
                         count(*)                               out_flow_total
                  from ods.ods_pl_blpark_traffic_dt t1
                           left join ods.ods_pl_blpark_park_dt t2 on t1.park_id = t2.park_id
                  group by t2.project_id, t2.name, substr(FROM_UNIXTIME(t1.out_at), 0, 4),
                           substr(FROM_UNIXTIME(t1.out_at), 6, 2)
              ) t2 on t1.bis_project_id = t2.bis_project_id and  t1.year = t2.year and t1.month = t2.month
     ) t
         left join ods.ods_pl_powerdes_bis_project_dt bis_project on t.bis_project_id = bis_project.bis_project_id
         inner join
     (
         select year,
                month,
                year_month1,
                max(int(day)) dayNum -- 每个月有多少天
         from dim.dim_pl_date
         where year between '2010' and '2030'
         group by year, month,year_month1
     ) t1 on t.year = t1.year and t.month = substring(t1.year_month1,6,2)
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
     ) t2 on t.bis_project_id = t2.out_mall_id;
