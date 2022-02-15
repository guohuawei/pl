
insert OVERWRITE table dws.dws_bis_rent_mgr_multi_current_month_big_dt partition (dt = '${hiveconf:nowdate}')
select t.area_name,                    -- 区域名称
       t.area_id,                      -- 区域id
       t.bis_project_id,               -- 项目id
       t.short_name,                   -- 项目名称
       t.query_year,                   -- 年
       t.query_month,                  -- 月
       t.query_date,                   -- 年月 2021-12
       t.fee_type,                     -- 费项（1：租金 2：物管 3：多经）
       t.store_type,                   -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
       t.total_qz_must_money,          -- 权责月应收金额
       t.total_qz_fact_money,          -- 权责月实收金额
       t.total_cont_must_money,        -- 每月合同口径应收金额
       t.total_cont_fact_money,        -- 每月合同口径实收金额
       t.budget_money,                 -- 租金、物管 预算金额
       t1.total_qz_month_fact_money,   -- 权责月月累计（月递归累计）实收金额
       t.total_qz_year_fact_money,     -- 权责月年累计实收金额
       t.total_qz_month_must_money,    -- 权责月月累计（月递归累计）应收金额
       t.total_qz_year_must_money,     -- 权责月年累计应收金额
       t2.total_cont_month_fact_money, -- 合同口径（应收月）月累计实收金额
       t3.total_cont_year_fact_money,  -- 合同口径（应收月）年累计实收金额
       t4.total_cont_month_must_money, -- 合同口径（应收月）月累计（月递归累计）应收金额
       t5.total_cont_year_must_money,  -- 合同口径（应收月）年累计应收金额
       t.etl_time                      -- ETL时间,

from dwd.dwd_bis_rent_mgr_multi_current_month_big_dt t
         left join
     (
         -- 租金 物管
         select t.bis_project_id,
                t.query_date,
                t.fee_type,
                t.store_type,
                sum(t1.fact_money) as total_qz_month_fact_money

         from dwd.dwd_bis_rent_mgr_multi_current_month_big_dt t
                  left join
              (
                  select sum(nvl(fact_money, 0)) fact_money,
                         fact_type,
                         fact_date,
                         store_type,
                         bis_project_id,
                         qz_year_month
                  from dwd.dwd_bis_fact_basic_big_dt
                  where dt = date_format(current_date, 'yyyyMMdd')
                    and fact_type in ('1','2')
                  group by fact_type,
                           fact_date,
                           store_type,
                           bis_project_id,
                           qz_year_month
              ) t1 on t1.store_type = t.store_type
                  and t1.fact_type = t.fee_type
                  and t1.bis_project_id = t.bis_project_id
                  and substr(t1.qz_year_month, 0, 4) = t.query_year
         where t.dt = date_format(current_date, 'yyyyMMdd')
           and substr(t1.fact_date, 1, 10) <= last_day(concat(t.query_date, '-01'))
           and t1.qz_year_month <= t.query_date
         group by t.bis_project_id, t.query_date, t.fee_type, t.store_type

         union all
         -- 多经
         select t.bis_project_id,
                t.query_date,
                t.fee_type,
                t.store_type,
                sum(t1.fact_money) as total_qz_month_fact_money

         from dwd.dwd_bis_rent_mgr_multi_current_month_big_dt t
                  left join
              (
                  select sum(nvl(fact_money, 0)) fact_money,
                         '3' as                  fact_type,
                         fact_date,
                         store_type,
                         bis_project_id,
                         qz_year_month
                  from dwd.dwd_bis_fact_basic_big_dt
                  where dt = date_format(current_date, 'yyyyMMdd')
                    and fact_type in ('34', '57')
                  group by fact_date,
                           store_type,
                           bis_project_id,
                           qz_year_month
              ) t1 on t1.store_type = t.store_type
                  and t1.fact_type = t.fee_type
                  and t1.bis_project_id = t.bis_project_id
                  and substr(t1.qz_year_month, 0, 4) = t.query_year
         where t.dt = date_format(current_date, 'yyyyMMdd')
           and substr(t1.fact_date, 1, 10) <= last_day(concat(t.query_date, '-01'))
           and t1.qz_year_month <= t.query_date
         group by t.bis_project_id, t.query_date, t.fee_type, t.store_type
     ) t1 on t.bis_project_id = t1.bis_project_id and t.store_type = t1.store_type and t.fee_type = t1.fee_type and
             t.query_date = t1.query_date
         left join
     (
         -- 租金 物管
         select t.bis_project_id,
                t.query_date,
                t.fee_type,
                t.store_type,
                sum(t1.fact_money) as total_cont_month_fact_money

         from dwd.dwd_bis_rent_mgr_multi_current_month_big_dt t
                  left join
              (
                  select sum(nvl(fact_money, 0))          fact_money,
                         fact_type,
                         fact_date,
                         store_type,
                         bis_project_id,
                         substr(must_year_month, 0, 7) as must_year_month
                  from dwd.dwd_bis_fact_basic_big_dt
                  where dt = date_format(current_date, 'yyyyMMdd')
                    and fact_type in ('1','2')
                  group by fact_type,
                           fact_date,
                           store_type,
                           bis_project_id,
                           must_year_month
              ) t1 on t1.store_type = t.store_type
                  and t1.fact_type = t.fee_type
                  and t1.bis_project_id = t.bis_project_id
                  and substr(t1.must_year_month, 0, 4) = t.query_year
         where t.dt = date_format(current_date, 'yyyyMMdd')
           and substr(t1.fact_date, 1, 10) <= last_day(concat(t.query_date, '-01'))
           and t1.must_year_month <= t.query_date
           and t1.must_year_month is not null
         group by t.bis_project_id, t.query_date, t.fee_type, t.store_type

         union all
         -- 多经
         select t.bis_project_id,
                t.query_date,
                t.fee_type,
                t.store_type,
                sum(t1.fact_money) as total_cont_month_fact_money

         from dwd.dwd_bis_rent_mgr_multi_current_month_big_dt t
                  left join
              (
                  select sum(nvl(fact_money, 0))          fact_money,
                         '3'                           as fact_type,
                         fact_date,
                         store_type,
                         bis_project_id,
                         substr(must_year_month, 0, 7) as must_year_month
                  from dwd.dwd_bis_fact_basic_big_dt
                  where dt = date_format(current_date, 'yyyyMMdd')
                    and fact_type in ('34', '57')
                  group by fact_date,
                           store_type,
                           bis_project_id,
                           must_year_month
              ) t1 on t1.store_type = t.store_type
                  and t1.fact_type = t.fee_type
                  and t1.bis_project_id = t.bis_project_id
                  and substr(t1.must_year_month, 0, 4) = t.query_year
         where t.dt = date_format(current_date, 'yyyyMMdd')
           and substr(t1.fact_date, 1, 10) <= last_day(concat(t.query_date, '-01'))
           and t1.must_year_month <= t.query_date
           and t1.must_year_month is not null
         group by t.bis_project_id, t.query_date, t.fee_type, t.store_type
     ) t2 on t.bis_project_id = t2.bis_project_id and t.store_type = t2.store_type and t.fee_type = t2.fee_type and
             t.query_date = t2.query_date
         left join
     (
         -- 租金  物管
         select t.bis_project_id,
                t.query_date,
                t.fee_type,
                t.store_type,
                sum(t1.fact_money) as total_cont_year_fact_money

         from dwd.dwd_bis_rent_mgr_multi_current_month_big_dt t
                  left join
              (
                  select sum(nvl(fact_money, 0))          fact_money,
                         fact_type,
                         fact_date,
                         store_type,
                         bis_project_id,
                         substr(must_year_month, 0, 7) as must_year_month
                  from dwd.dwd_bis_fact_basic_big_dt
                  where dt = date_format(current_date, 'yyyyMMdd')
                    and fact_type in ('1','2')
                  group by fact_type,
                           fact_date,
                           store_type,
                           bis_project_id,
                           must_year_month
              ) t1 on t1.store_type = t.store_type
                  and t1.fact_type = t.fee_type
                  and t1.bis_project_id = t.bis_project_id
                  -- and t1.must_year_month = t.query_date
                  and substr(t1.must_year_month, 0, 4) = t.query_year
         where t.dt = date_format(current_date, 'yyyyMMdd')
           and substr(t1.fact_date, 1, 10) <= last_day(concat(t.query_date, '-01'))
           --and t1.must_year_month <= concat(t.query_year, '-12')
           --and t1.must_year_month >= concat(t.query_year, '-01')
           and t1.must_year_month is not null
         group by t.bis_project_id, t.query_date, t.fee_type, t.store_type

         union all

         -- 多经
         select t.bis_project_id,
                t.query_date,
                t.fee_type,
                t.store_type,
                sum(t1.fact_money) as total_cont_year_fact_money

         from dwd.dwd_bis_rent_mgr_multi_current_month_big_dt t
                  left join
              (
                  select sum(nvl(fact_money, 0))          fact_money,
                         '3'                           as fact_type,
                         fact_date,
                         store_type,
                         bis_project_id,
                         substr(must_year_month, 0, 7) as must_year_month
                  from dwd.dwd_bis_fact_basic_big_dt
                  where dt = date_format(current_date, 'yyyyMMdd')
                    and fact_type in ('34', '57')
                  group by fact_date,
                           store_type,
                           bis_project_id,
                           must_year_month
              ) t1 on t1.store_type = t.store_type
                  and t1.fact_type = t.fee_type
                  and t1.bis_project_id = t.bis_project_id
                  -- and t1.must_year_month = t.query_date
                  and substr(t1.must_year_month, 0, 4) = t.query_year
         where t.dt = date_format(current_date, 'yyyyMMdd')
           and substr(t1.fact_date, 1, 10) <= last_day(concat(t.query_date, '-01'))
           --and t1.must_year_month <= concat(t.query_year, '-12')
           --and t1.must_year_month >= concat(t.query_year, '-01')
           and t1.must_year_month is not null
         group by t.bis_project_id, t.query_date, t.fee_type, t.store_type
     ) t3 on t.bis_project_id = t3.bis_project_id and t.store_type = t3.store_type and t.fee_type = t3.fee_type and
             t.query_date = t3.query_date
         left join
     (
         select t.bis_project_id,
                t.query_date,
                t.fee_type,
                t.store_type,
                sum(t.mustMoney)
                    over (partition by t.bis_project_id,substr(t.query_date, 0, 4) ,t.fee_type,t.store_type order by t.bis_project_id,t.query_date ,t.fee_type,t.store_type) total_cont_month_must_money -- 合同口径月累计应收

         from (
                  select t.bis_project_id,
                         t.query_date,
                         t.fee_type,
                         t.store_type,
                         sum(t.total_cont_must_money) mustMoney -- 合同口径月应收

                  from (
                           select t.bis_project_id,       -- 项目id
                                  t.query_date,           -- 应收月
                                  t.fee_type,
                                  t.store_type,
                                  t.total_cont_must_money -- 月应收
                           from dwd.dwd_bis_rent_mgr_multi_current_month_big_dt t
                           where t.dt = date_format(current_date, 'yyyyMMdd')
                       ) t
                       --where t.bis_project_id = '6D7E1C7AFAFB43E986670A81CF444253'
                       -- and t.fee_type = '1'
                       --  and substr(t.query_date, 0, 4) = '2021'
                  group by t.bis_project_id, t.query_date, t.fee_type, t.store_type
              ) t
     ) t4 on t.bis_project_id = t4.bis_project_id and t.store_type = t4.store_type and t.fee_type = t4.fee_type and
             t.query_date = t4.query_date
         left join
     (
         select t.bis_project_id,
                t.query_year,
                t.fee_type,
                t.store_type,
                sum(t.total_cont_must_money) total_cont_year_must_money -- 合同口径年累计应收

         from (
                  select t.bis_project_id,       -- 项目id
                         t.query_year,           -- 应收月
                         t.fee_type,
                         t.store_type,
                         t.total_cont_must_money -- 月应收
                  from dwd.dwd_bis_rent_mgr_multi_current_month_big_dt t
                  where t.dt = date_format(current_date, 'yyyyMMdd')
              ) t
--           where t.bis_project_id = '6D7E1C7AFAFB43E986670A81CF444253'
--            and t.fee_type = '1'
--            and t.query_year = '2021'
         group by t.bis_project_id, t.query_year, t.fee_type, t.store_type
     ) t5 on t.bis_project_id = t5.bis_project_id and t.store_type = t5.store_type and t.fee_type = t5.fee_type and
             t.query_year = t5.query_year
where t.dt = date_format(current_date, 'yyyyMMdd');


