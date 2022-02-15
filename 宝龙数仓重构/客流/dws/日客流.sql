select *
from (
         select t.area_name,                                                                                   -- 区域名称
                t.id,                                                                                          -- 区域id
                t.bis_project_id,                                                                              -- 项目id
                t.short_name,                                                                                  -- 项目名称
                t.site_key,                                                                                    -- 场所编码
                t.day_id,                                                                                      -- 天(20200815)
                t.date_id,                                                                                     -- 日期(2020/08/15)
                t.budget_flow,                                                                                 -- 日期望客流
                t.actual_flow,                                                                                 -- 日实际客流
                t.is_holiday,                                                                                  -- 是否是节假日
                t.month_id,                                                                                    -- 月份(202008)
                -- /*按月合计*/
                sum(budget_flow) over (partition by bis_project_id, month_id order by month_id) budget_flow_m, -- 月期望客流
                sum(actual_flow) over (partition by bis_project_id, month_id order by month_id) actual_flow_m, -- 月实际客流
                t.stage,                                                                                       -- 项目阶段('1、筹备期 2、培育期 3、稳定期 4、调改期')
                t.province,                                                                                    -- 项目所属省份
                t.total_rent_area,                                                                              -- 项目全场计租面积
                current_date                       -- ETL时间
         from (
                  select prodt.area_name,                        -- 区域名称
                         prodt.id,                               -- 区域id
                         prodt.bis_project_id,                   -- 项目id
                         prodt.bis_short_name as    short_name,  -- 项目名称
                         prodt.site_key,                         -- 场所编码
                         prodt.day_id,                           -- 天
                         prodt.date_id,                          -- 日期
                         nvl(budget.index * 10000, 0) budget_flow, -- 日期望客流
                         nvl(actual.insum, 0)       actual_flow, -- 日实际客流
                         case
                             when prodt.WEEK_NUM = '5' or prodt.WEEK_NUM = '6' or prodt.WEEK_NUM = '7' then '1'
                             else '0' end           is_holiday,  -- 是否是节假日
                         prodt.month_id,                         -- 月份
                         prodt.stage,                            -- 项目阶段
                         prodt.province,                         -- 项目所属省份
                         prodt.total_rent_area                   -- 项目全场计租面积
                  from (
                           select dt.date_id,        -- YYYY/MM/DD
                                  dt.day_id,         -- YYYYMMDD
                                  dt.month_id,       -- YYYYMM
                                  dt.year_month1,
                                  dt.WEEK_NUM,       -- 一周中的第几天
                                  proj.site_key,
                                  proj.site_name,
                                  proj.bis_project_id,
                                  proj.bis_project_name,
                                  proj.bis_short_name,
                                  t.area_name,
                                  t.id,
                                  t2.stage,          -- 项目阶段
                                  t2.province,       -- 省份
                                  t2.total_rent_area -- 项目全场计租面积

                           from (
                                    select year_month_day2    as day_id,
                                           year_month_day3    as date_id,
                                           year_month4        as month_id,
                                           day_number_of_week as WEEK_NUM,
                                           year_month1

                                    from dim.dim_pl_date
                                    where year between '2012' and '2025'
                                ) dt
                                    cross join
                                (select site_key,
                                        site_name,
                                        bis_project_name as bis_project_id,
                                        bis_project_id   as bis_project_name,
                                        bis_short_name
                                 from ods.ods_pl_powerdes_traffic_sites_project_dt
                                ) proj
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
                                ) t on proj.bis_project_id = t.out_mall_id
                                    left join
                                (
                                    select bis_project_id,
                                           SHORT_NAME,
                                           STAGE,
                                           PROVINCE,
                                           sum(bis_store.rent_square) as total_rent_area
                                    from ods.ods_pl_powerdes_bis_project_dt BIS_PROJECT
                                             left join ods.ods_pl_powerdes_bis_cont_dt bis_cont
                                                       on BIS_PROJECT.bis_project_id = bis_cont.bis_project_id
                                             left join ods.ods_pl_powerdes_bis_store_dt bis_store
                                                       on bis_store.bis_store_id = bis_cont.bis_store_ids
                                                           and bis_store.bis_project_id = bis_cont.bis_project_id
                                    where BIS_PROJECT.is_business_project = '1'
                                      and BIS_PROJECT.oper_status = '2'
                                      and bis_cont.store_type = '1' -- 购物中心
                                      and bis_cont.effect_flg <> 'D'
                                      and BIS_PROJECT.stage is not null
                                      and bis_store.is_delete = 'N'
                                      AND bis_store.status_cd = '1'
                                    group by BIS_PROJECT.bis_project_id, BIS_PROJECT.SHORT_NAME, BIS_PROJECT.stage,
                                             BIS_PROJECT.province
                                ) t2 on proj.bis_project_id = t2.bis_project_id
                       ) prodt
                           left outer join
                       ods.ods_pl_data_center_index_passenger_flow_dt budget
                       on prodt.year_month1 = budget.query_date and prodt.bis_project_id = budget.bis_project_id and is_del = '0'
                           left outer join
                       (select distinct a.sitekey,
                                        a.sitename,
                                        a.sitetype,
                                        a.sitetypename,
                                        a.cityid,
                                        a.countdate,
                                        a.insum,
                                        a.outsum,
                                        a.modifytime,
                                        a.customercode,
                                        a.synchro_dt,
                                        replace(a.countdate, '-', '')  day_id,
                                        replace(a.countdate, '-', '/') date_id
                        from (
                                 -- 从sqlserver抽取的日客流
                                 select sitekey,
                                        sitename,
                                        sitetype,
                                        sitetypename,
                                        cityid,
                                        countdate,
                                        insum,
                                        outsum,
                                        modifytime,
                                        customercode,
                                        date_format(current_date(), 'yyyyMMdd') as Synchro_dt
                                 from ods.ods_sqlserver_intsummary_day_dt
                                 where sitetype = '300' -- 300是广场，  400是区域   500是楼层    600是店铺   700是通道

                                 union all

                                 -- 从mongodb抽取的日客流
                                 select SITEKEY,
                                        SITENAME,
                                        SITETYPE,
                                        null                     as SITETYPENAME,
                                        null                     as CITYID,
                                        substr(COUNTDATE, 0, 10) as COUNTDATE,
                                        int(INSUM),
                                        int(OUTSUM),
                                        MODIFYTIME,
                                        null                     as CUSTOMERCODE,
                                        date_format(current_date(), 'yyyyMMdd')
                                 from (select a.*,
                                              RANK()
                                                      OVER (PARTITION BY countdate, sitetype, sitename ORDER BY systime desc) systime_rank
                                       from dwd.dwd_mongodb_passenger_flow_day_big_dt a
                                       where dt = date_format(current_date(), 'yyyyMMdd')
                                      ) days
                                 where systime_rank = 1
                             ) a
                        where SiteType = '300'
                          and sitename <> '测试'
                       ) actual on prodt.day_id = actual.day_id and prodt.site_key = actual.sitekey
                  where prodt.day_id <= replace(date_sub(current_date, 1), '-', '')
              ) t
     ) b
     --  /*去除预算/实际当月合计为0的数据*/
where (budget_flow_m <> 0 or actual_flow_m <> 0);