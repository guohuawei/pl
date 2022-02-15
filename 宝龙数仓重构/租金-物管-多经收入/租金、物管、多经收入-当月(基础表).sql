insert OVERWRITE table dwd.dwd_bis_rent_mgr_multi_current_month_big_dt partition (dt = '${hiveconf:nowdate}')
-- 租金、物管、多经收入-当月
-- 租金、物管收入-当月
select t2.area_name,                   -- 区域名称
       t2.id,                          -- 区域id
       t.bis_project_id,               -- 项目id
       t.short_name,                   -- 项目名称
       t.year,                         -- 年
       t.month,                        -- 月
       t.year_month1,                  -- 年月 2021-12
       t.must_type,                    -- 费项（1：租金 2：物管）
       t.store_type,                   -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
       t.must_money_qz,                -- 权责月应收金额
       t.fact_money_qz,                -- 权责月实收金额
       t1.must_money_cont,             -- 每月合同口径应收金额
       t1.fact_money_cont,             -- 每月合同口径实收金额
       t3.budget_money,                -- 租金、物管 预算金额
       t.total_qz_month_fact_money,    -- 权责月月累计（月递归累计）实收金额
       t.total_qz_year_fact_money,     -- 权责月年累计实收金额
       t.total_qz_month_must_money,    -- 权责月月累计（月递归累计）应收金额
       t.total_qz_year_must_money,     -- 权责月年累计应收金额
       t1.total_cont_month_fact_money, -- 合同口径（应收月）月累计（月递归累计）实收金额
       t1.total_cont_year_fact_money,  -- 合同口径（应收月）年累计实收金额
       t1.total_cont_month_must_money, -- 合同口径（应收月）月累计（月递归累计）应收金额
       t1.total_cont_year_must_money,  -- 合同口径（应收月）年累计应收金额
       current_date                    -- ETL时间
from (
         -- 权责口径--应收/实收金额
         select t.bis_project_id,                                                   -- 项目id
                t.short_name,                                                       -- 项目名称
                t.year,                                                             -- 年
                t.month,                                                            -- 月
                t.year_month1,                                                      -- 年月 2021-12
                t.store_type,                                                       -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                t.must_type,                                                        -- 费项（1：租金 2：物管）
                sum(nvl(t.must_money, 0))                must_money_qz,             -- 月权责应收金额
                sum(nvl(t.fact_money, 0))                fact_money_qz,             -- 月权责实收金额
                sum(nvl(t.total_qz_month_fact_money, 0)) total_qz_month_fact_money, -- 权责月月累计（月递归累计）实收金额
                sum(nvl(t.total_qz_year_fact_money, 0))  total_qz_year_fact_money,  -- 权责月年累计实收金额
                sum(nvl(t.total_qz_month_must_money, 0)) total_qz_month_must_money, -- 权责月月累计（月递归累计）应收金额
                sum(nvl(t.total_qz_year_must_money, 0))  total_qz_year_must_money   -- -- 权责月年累计应收金额
         from (
                  select t1.bis_project_id,                        -- 项目id
                         t1.short_name,                            -- 项目名称
                         t1.year,                                  -- 年
                         t1.month,                                 -- 月
                         t1.year_month1,                           -- 年月 2021-12
                         t1.must_type,                             -- 费项（1：租金 2：物管）
                         t1.store_type,                            -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                         bis_must_basic.must_money,                -- 应收金额
                         bis_must_basic.total_qz_month_must_money, -- 权责月月累计（月递归累计）应收金额
                         bis_must_basic.total_qz_year_must_money,  -- -- 权责月年累计应收金额
                         bis_fact_basic.fact_money,                -- 实收金额
                         bis_fact_basic.total_qz_month_fact_money, -- 权责月月累计（月递归累计）实收金额
                         bis_fact_basic.total_qz_year_fact_money   -- 权责月年累计实收金额
                  from (
                           select distinct bis_project_id,
                                           short_name,
                                           year,
                                           month,
                                           year_month1,
                                           store_type,
                                           must_type
                           from dwd.dwd_bis_cross_join_result_big_dt
                           where dt = date_format(current_date(), 'yyyyMMdd')
                       ) t1
                           left join
                       (
                           -- 应收
                           select bis_project_id,
                                  qz_year_month,
                                  store_type,
                                  must_type,
                                  round(sum(must_money), 2) must_money,
                                  round(sum(round(sum(must_money), 2))
                                            over (partition by bis_project_id, substr(qz_year_month, 0, 4),store_type, must_type order by bis_project_id,qz_year_month,store_type,must_type),
                                        2)                  total_qz_month_must_money, -- 权责月月累计（月递归累计）应收金额
                                  round(sum(round(sum(must_money), 2))
                                            over (partition by bis_project_id, substr(qz_year_month, 0, 4),store_type, must_type order by bis_project_id, substr(qz_year_month, 0, 4),store_type, must_type),
                                        2)                  total_qz_year_must_money   -- 权责月年累计应收金额
                           from (
                                    select t1.bis_project_id,
                                           t1.year_month1 as     qz_year_month,
                                           t1.store_type,
                                           nvl(t2.must_money, 0) must_money,
                                           t1.must_type
                                    from (
                                             select distinct bis_project_id,
                                                             short_name,
                                                             year,
                                                             month,
                                                             year_month1,
                                                             store_type,
                                                             must_type
                                             from dwd.dwd_bis_cross_join_result_big_dt
                                             where dt = date_format(current_date(), 'yyyyMMdd')
                                         ) t1
                                             left join dwd.dwd_bis_must_qz_basic_big_dt t2
                                                       on t1.bis_project_id = t2.bis_project_id and
                                                          t1.year_month1 = t2.qz_year_month
                                                           and t1.store_type = t2.store_type and
                                                          t1.must_type = t2.must_type and
                                                          t2.must_type in ('1', '2')
                                                           and t2.dt = date_format(current_date, 'yyyyMMdd')
                                ) bis_must_basic
                           group by bis_project_id, qz_year_month, substr(qz_year_month, 0, 4), store_type, must_type
                       ) bis_must_basic
                       on t1.bis_project_id = bis_must_basic.bis_project_id
                           and t1.year_month1 = bis_must_basic.qz_year_month and
                          t1.store_type = bis_must_basic.store_type and
                          t1.must_type = bis_must_basic.must_type

                           left join
                       (
                           -- 实收
                           select bis_project_id,
                                  store_type,
                                  qz_year_month,
                                  fact_type,
                                  round(sum(nvl(fact_money, 0)), 2) fact_money,
                                  null                              total_qz_month_fact_money, -- 权责月月累计（月递归累计）实收金额
                                  round(sum(round(sum(nvl(fact_money, 0)), 2))
                                            over (partition by bis_project_id, substr(qz_year_month, 0, 4),store_type, fact_type order by bis_project_id, substr(qz_year_month, 0, 4),store_type, fact_type),
                                        2)                          total_qz_year_fact_money   -- 权责月年累计实收金额
                           from (
                                    select t1.bis_project_id,
                                           t1.short_name,
                                           t1.store_type,
                                           t1.year_month1 qz_year_month,
                                           t1.must_type   fact_type,
                                           t2.fact_money
                                    from (
                                             select distinct bis_project_id,
                                                             short_name,
                                                             year,
                                                             month,
                                                             year_month1,
                                                             store_type,
                                                             must_type
                                             from dwd.dwd_bis_cross_join_result_big_dt
                                             where dt = date_format(current_date(), 'yyyyMMdd')
                                         ) t1
                                             left join dwd.dwd_bis_fact_basic_big_dt t2
                                                       on t1.bis_project_id = t2.bis_project_id and
                                                          t1.year_month1 = t2.qz_year_month
                                                           and t1.store_type = t2.store_type and
                                                          t1.must_type = t2.fact_type and
                                                          t2.fact_type in ('1', '2')
                                                           and t2.dt = date_format(current_date, 'yyyyMMdd')
                                                           and substr(t2.fact_date, 0, 10) <=
                                                               last_day(concat(t2.qz_year_month, '-01')) -- 权责月最后一天
                                ) bis_fact_basic
                                -- where bis_project_id = '6D7E1C7AFAFB43E986670A81CF444253'
                                --  and qz_year_month like '2021%'
                                -- and fact_type = '1'
                                -- and store_type = '2'
                           group by bis_project_id, store_type, qz_year_month, substr(qz_year_month, 0, 4), fact_type
                       ) bis_fact_basic
                       on bis_must_basic.bis_project_id = bis_fact_basic.bis_project_id
                           and bis_must_basic.qz_year_month = bis_fact_basic.qz_year_month
                           and bis_must_basic.must_type = bis_fact_basic.fact_type
                           and bis_must_basic.store_type = bis_fact_basic.store_type
              ) t
         group by t.bis_project_id, t.short_name, t.year_month1, t.year, t.month, t.store_type, t.must_type
     ) t
         left join
     (
         -- 合同口径应收/实收金额
         select t4.bis_project_id,                                 -- 项目id
                t4.short_name,                                     -- 项目名称
                t4.year,                                           -- 年
                t4.month,                                          -- 月
                t4.year_month1,                                    -- 应收年月
                t4.must_type,                                      -- 费项（1：租金 2：物管）
                t4.store_type,                                     -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                t6.mustMoney          must_money_cont,             -- 月应收金额
                t6.month_lj_mustMOney total_cont_month_must_money, -- 合同口径（应收月）月累计（月递归累计）应收金额
                t6.year_lj_mustMoney  total_cont_year_must_money,  -- 合同口径（应收月）年累计应收金额
                t6.factMoney          fact_money_cont,             -- 月实收金额
                t6.month_lj_factMoney total_cont_month_fact_money, -- 合同口径（应收月）月累计（月递归累计）实收金额
                t6.year_lf_factMoney  total_cont_year_fact_money   -- 合同口径（应收月）年累计实收金额

         from (
                  select distinct bis_project_id,
                                  short_name,
                                  year,
                                  month,
                                  year_month1,
                                  store_type,
                                  must_type
                  from dwd.dwd_bis_cross_join_result_big_dt
                  where dt = date_format(current_date(), 'yyyyMMdd')
              ) t4
                  left join
              (
                  select t2.BIS_PROJECT_ID,                                  -- 项目id
                         t2.MUST_TYPE,                                       -- 费项
                         t3.STORE_TYPE,                                      -- 物业类型
                         t2.must_year_month,                                 -- 应收年月
                         sum(nvl(t2.mustMoney, 0))       mustMoney,          -- 月应收
                         sum(nvl(t2.factMoney, 0))       factMoney,          -- 月实收
                         sum(nvl(month_lj_mustMOney, 0)) month_lj_mustMOney, -- 月累计应收
                         sum(nvl(month_lj_factMoney, 0)) month_lj_factMoney, -- 月累计实收
                         sum(nvl(year_lj_mustMoney, 0))  year_lj_mustMoney,  -- 年累计应收
                         sum(nvl(year_lf_factMoney, 0))  year_lf_factMoney   -- 年累计实收
                  from ods.ods_pl_powerdes_bis_cont_dt t3
                           inner join
                       (
                           select table1.bis_project_id,     -- 项目id
                                  table1.bis_cont_id,        -- 合同id
                                  table1.MUST_TYPE,          -- 费项（1：租金 2：物管）
                                  table1.must_year_month,    -- 应收月
                                  table1.factMoney,          -- 月实收
                                  table1.month_lj_factMoney, -- 月累计实收
                                  table1.year_lf_factMoney,  -- 年累计实收
                                  table3.mustMoney,          -- 月应收
                                  table3.month_lj_mustMOney, -- 月累计应收
                                  table3.year_lj_mustMoney   -- 年累计应收
                           from (
                                    select t.bis_project_id,                                      -- 项目id
                                           t.bis_cont_id,                                         -- 合同id
                                           t.MUST_TYPE,                                           -- 费项（1：租金 2：物管）
                                           substr(t.rent_last_pay_date, 0, 7) must_year_month,    -- 应收月

                                           sum(case
                                                   when substr(fact.fact_date, 0, 7) <= substr(t.rent_last_pay_date, 0, 7)
                                                       then nvl(fact.factMoney, 0)
                                                   else 0 end)                factMoney,          -- 月实收

                                           null                               month_lj_factMoney, -- 月累计实收

                                           null                               year_lf_factMoney   -- 年累计实收


                                    from (
                                             -- 应收
                                             SELECT BIS_MUST2.bis_project_id,
                                                    BIS_MUST2.bis_cont_id,
                                                    BIS_MUST2.qz_year_month,
                                                    BIS_MUST2.MUST_TYPE,
                                                    BIS_MUST2.billing_period_end,
                                                    BIS_MUST2.billing_period_begin,
                                                    BIS_MUST2.bis_must_id,
                                                    BIS_MUST2.rent_last_pay_date, -- 应收日期
                                                    sum(nvl(BIS_MUST2.money, 0)) as mustMoney
                                             FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                             where BIS_MUST2.must_type in ('1', '2')
                                               and BIS_MUST2.is_show = 1
                                               and BIS_MUST2.is_delete = 0
                                             group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                                      BIS_MUST2.bis_must_id,
                                                      BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                                                      BIS_MUST2.billing_period_end,
                                                      BIS_MUST2.billing_period_begin,
                                                      BIS_MUST2.rent_last_pay_date
                                         ) t
                                             left join
                                         (
                                             select must.bis_must_id,
                                                    adj.adjMoney
                                             from (
                                                      -- 应收
                                                      SELECT BIS_MUST2.bis_project_id,
                                                             BIS_MUST2.bis_cont_id,
                                                             BIS_MUST2.qz_year_month,
                                                             BIS_MUST2.MUST_TYPE,
                                                             BIS_MUST2.billing_period_end,
                                                             BIS_MUST2.billing_period_begin,
                                                             min(BIS_MUST2.rent_last_pay_date) rent_last_pay_date, -- 应收日期
                                                             min(BIS_MUST2.bis_must_id)   as   bis_must_id,
                                                             sum(nvl(BIS_MUST2.money, 0)) as   mustMoney
                                                      FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                                      where BIS_MUST2.must_type in ('1', '2')
                                                        and BIS_MUST2.is_show = 1
                                                        and BIS_MUST2.is_delete = 0
                                                      group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                                               BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                                                               BIS_MUST2.billing_period_end,
                                                               BIS_MUST2.billing_period_begin
                                                  ) must
                                                      left join
                                                  (
                                                      -- 减免
                                                      SELECT bis_mf_adjust.bis_cont_id,
                                                             bis_mf_adjust.billing_period_end,
                                                             bis_mf_adjust.billing_period_begin,
                                                             bis_mf_adjust.qz_year_month,
                                                             bis_mf_adjust.FEE_TYPE,
                                                             sum(nvl(bis_mf_adjust.adjust_money, 0)) as adjMoney
                                                      FROM ods.ods_pl_powerdes_bis_mf_adjust_dt bis_mf_adjust
                                                      where bis_mf_adjust.is_del = 1
                                                        and bis_mf_adjust.fee_type in ('1', '2')
                                                      group by bis_mf_adjust.bis_cont_id,
                                                               bis_mf_adjust.qz_year_month,
                                                               bis_mf_adjust.FEE_TYPE,
                                                               bis_mf_adjust.billing_period_end,
                                                               bis_mf_adjust.billing_period_begin
                                                  ) adj
                                                      -- 合同 权责 费项 账期开始  账期结束
                                                  on must.bis_cont_id = adj.bis_cont_id and
                                                     must.qz_year_month = adj.qz_year_month and
                                                     must.MUST_TYPE = adj.FEE_TYPE and
                                                     must.billing_period_end = adj.billing_period_end and
                                                     must.billing_period_begin = adj.billing_period_begin
                                         ) t1 on t.bis_must_id = t1.bis_must_id
                                             left join
                                         (
                                             -- 实收
                                             SELECT bis_fact2.bis_cont_id,
                                                    bis_fact2.bis_must_id,
                                                    bis_fact2.fact_type,
                                                    bis_fact2.fact_date,
                                                    bis_fact2.must_rent_last_pay_date,
                                                    sum(nvl(bis_fact2.money, 0)) as factMoney
                                             FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
                                             where bis_fact2.is_delete = 1
                                               and bis_fact2.fact_type in ('1', '2')
                                               and bis_fact2.bis_must_id is not null
                                             group by bis_fact2.bis_cont_id, bis_fact2.bis_must_id,
                                                      bis_fact2.fact_type,
                                                      bis_fact2.fact_date,
                                                      bis_fact2.must_rent_last_pay_date
                                         ) fact
                                         on t.bis_cont_id = fact.bis_cont_id and
                                            t.bis_must_id = fact.bis_must_id and
                                            t.MUST_TYPE = fact.FACT_TYPE
                                    group by t.bis_project_id, t.bis_cont_id,
                                             substr(t.rent_last_pay_date, 0, 4),
                                             substr(t.rent_last_pay_date, 0, 7), t.MUST_TYPE
                                ) table1
                                    left join
                                (
                                    select t.bis_project_id,                                                  -- 项目id
                                           t.bis_cont_id,                                                     -- 合同id
                                           t.MUST_TYPE,                                                       -- 费项（1：租金 2：物管）
                                           substr(t.rent_last_pay_date, 0, 7)             must_year_month,    -- 应收月
                                           sum(nvl(t.mustMoney, 0) + nvl(t1.adjMoney, 0)) mustMoney,          -- 月应收
                                           null                                           month_lj_mustMOney, -- 月累计应收
                                           null                                           year_lj_mustMoney   -- 年累计应收

                                    from (
                                             -- 应收
                                             SELECT BIS_MUST2.bis_project_id,
                                                    BIS_MUST2.bis_cont_id,
                                                    BIS_MUST2.qz_year_month,
                                                    BIS_MUST2.MUST_TYPE,
                                                    BIS_MUST2.billing_period_end,
                                                    BIS_MUST2.billing_period_begin,
                                                    BIS_MUST2.bis_must_id,
                                                    BIS_MUST2.rent_last_pay_date, -- 应收日期
                                                    sum(nvl(BIS_MUST2.money, 0)) as mustMoney
                                             FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                             where BIS_MUST2.must_type in ('1', '2')
                                               and BIS_MUST2.is_show = 1
                                               and BIS_MUST2.is_delete = 0
                                             group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                                      BIS_MUST2.bis_must_id,
                                                      BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                                                      BIS_MUST2.billing_period_end,
                                                      BIS_MUST2.billing_period_begin,
                                                      BIS_MUST2.rent_last_pay_date
                                         ) t
                                             left join
                                         (
                                             select must.bis_must_id,
                                                    adj.adjMoney
                                             from (
                                                      -- 应收
                                                      SELECT BIS_MUST2.bis_project_id,
                                                             BIS_MUST2.bis_cont_id,
                                                             BIS_MUST2.qz_year_month,
                                                             BIS_MUST2.MUST_TYPE,
                                                             BIS_MUST2.billing_period_end,
                                                             BIS_MUST2.billing_period_begin,
                                                             min(BIS_MUST2.rent_last_pay_date) rent_last_pay_date, -- 应收日期
                                                             min(BIS_MUST2.bis_must_id)   as   bis_must_id,
                                                             sum(nvl(BIS_MUST2.money, 0)) as   mustMoney
                                                      FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                                      where BIS_MUST2.must_type in ('1', '2')
                                                        and BIS_MUST2.is_show = 1
                                                        and BIS_MUST2.is_delete = 0
                                                      group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                                               BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                                                               BIS_MUST2.billing_period_end,
                                                               BIS_MUST2.billing_period_begin
                                                  ) must
                                                      left join
                                                  (
                                                      -- 减免
                                                      SELECT bis_mf_adjust.bis_cont_id,
                                                             bis_mf_adjust.billing_period_end,
                                                             bis_mf_adjust.billing_period_begin,
                                                             bis_mf_adjust.qz_year_month,
                                                             bis_mf_adjust.FEE_TYPE,
                                                             sum(nvl(bis_mf_adjust.adjust_money, 0)) as adjMoney
                                                      FROM ods.ods_pl_powerdes_bis_mf_adjust_dt bis_mf_adjust
                                                      where bis_mf_adjust.is_del = 1
                                                        and bis_mf_adjust.fee_type in ('1', '2')
                                                      group by bis_mf_adjust.bis_cont_id,
                                                               bis_mf_adjust.qz_year_month,
                                                               bis_mf_adjust.FEE_TYPE,
                                                               bis_mf_adjust.billing_period_end,
                                                               bis_mf_adjust.billing_period_begin
                                                  ) adj
                                                      -- 合同 权责 费项 账期开始  账期结束
                                                  on must.bis_cont_id = adj.bis_cont_id and
                                                     must.qz_year_month = adj.qz_year_month and
                                                     must.MUST_TYPE = adj.FEE_TYPE and
                                                     must.billing_period_end = adj.billing_period_end and
                                                     must.billing_period_begin = adj.billing_period_begin
                                         ) t1 on t.bis_must_id = t1.bis_must_id
                                    group by t.bis_project_id, t.bis_cont_id,
                                             substr(t.rent_last_pay_date, 0, 4),
                                             substr(t.rent_last_pay_date, 0, 7), t.MUST_TYPE
                                ) table3
                                on table1.bis_project_id = table3.bis_project_id
                                    and table1.bis_cont_id = table3.bis_cont_id
                                    and table1.must_type = table3.must_type
                                    and table1.must_year_month = table3.must_year_month
                       ) t2 on t3.BIS_PROJECT_ID = t2.BIS_PROJECT_ID and t3.BIS_CONT_ID = t2.BIS_CONT_ID
                  group by t2.BIS_PROJECT_ID,
                           t2.MUST_TYPE,
                           t3.STORE_TYPE,
                           t2.must_year_month
              ) t6
              on t4.bis_project_id = t6.bis_project_id and t4.year_month1 = t6.must_year_month and
                 t4.store_type = t6.store_type and
                 t4.must_type = t6.must_type
     ) t1
     on t.bis_project_id = t1.bis_project_id and t.short_name = t1.short_name
         and t.year_month1 = t1.year_month1 and t.store_type = t1.store_type and t.must_type = t1.must_type
         left join
     (
         -- 租金 预算
         select t.bis_project_id,                                                                  -- 项目id
                t.annual,                                                                          -- 年
                substr(month_budget_money, 0, instr(month_budget_money, '-') - 1) as budget_month, -- 月
                t.charge_type,                                                                     -- 物业类型 （1购物中心2商业街3住宅4住宅底商5写字楼 6住宅公寓7住宅别墅）
                t.store_type,                                                                      -- 1 铺位  2 多径
                substr(month_budget_money, instr(month_budget_money, '-') + 1, 1) as fee_type,     -- 费项 (1:租金 2：物管 3： 多经)
                substr(month_budget_money, instr(month_budget_money, '=') + 1)    as budget_money  -- 租金 物管 多经 每个月的预算金额
         from (
                  SELECT out_mall_id as bis_project_id, -- 项目id
                         lease.annual,                  -- 年
                         lease.charge_type,             -- 物业类型 （1购物中心2商业街3住宅4住宅底商5写字楼 6住宅公寓7住宅别墅）
                         lease.store_type,              -- 1 铺位  2 多径
-- if(lease.store_type = '1 ：铺位','1：租金','3：多经')   -- if(lease.store_type = '1 ：铺位','1：物管','无效‘)
                         concat_ws(',',
                                   concat('1', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(jan_zj))),
                                   concat('2', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(feb_zj))),
                                   concat('3', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(march_zj))),
                                   concat('4', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(april_zj))),
                                   concat('5', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(may_zj))),
                                   concat('6', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(june_zj))),
                                   concat('7', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(july_zj))),
                                   concat('8', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(aug_zj))),
                                   concat('9', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(sep_zj))),
                                   concat('10', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(oct_zj))),
                                   concat('11', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(nov_zj))),
                                   concat('12', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(dece_zj))),
                                   if(string(sum(jan_wg)) is null, null,
                                      concat('1', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(jan_wg)))),
                                   if(string(sum(feb_wg)) is null, null,
                                      concat('2', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(feb_wg)))),
                                   if(string(sum(march_wg)) is null, null,
                                      concat('3', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(march_wg)))),
                                   if(string(sum(april_wg)) is null, null,
                                      concat('4', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(april_wg)))),
                                   if(string(sum(may_wg)) is null, null,
                                      concat('5', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(may_wg)))),
                                   if(string(sum(june_wg)) is null, null,
                                      concat('6', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(june_wg)))),
                                   if(string(sum(july_wg)) is null, null,
                                      concat('7', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(july_wg)))),
                                   if(string(sum(aug_wg)) is null, null,
                                      concat('8', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(aug_wg)))),
                                   if(string(sum(sep_wg)) is null, null,
                                      concat('9', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(sep_wg)))),
                                   if(string(sum(oct_wg)) is null, null,
                                      concat('10', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(oct_wg)))),
                                   if(string(sum(nov_wg)) is null, null,
                                      concat('11', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(nov_wg)))),
                                   if(string(sum(dece_wg)) is null, null,
                                      concat('12', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(dece_wg))))
                             )       as res
                  FROM ods.ods_pl_pms_bis_db_bs_mall_dt m
                           inner join ods.ods_pl_pms_budget_db_budget_instance_dt instance
                                      on m.id = instance.project_id
                           INNER JOIN ods.ods_pl_pms_budget_db_budget_instance_sheet_dt instanceSheet
                                      ON instance.id = instanceSheet.budget_instance_id
                                          AND instance.instance_type = 1
                                          AND instanceSheet.budget_sheet_id in (29, 52)
                           inner join ods.ods_pl_pms_budget_db_budget_instance_biz_lease_budget_dt lease
                                      on instanceSheet.id = lease.budget_instance_sheet_id and
                                         lease.is_del = false and lease.store_type = '1'
                  where m.is_del = '0'
                    and m.stat = '2'
                    and lease.curr_cont_type in ('1', '2', '3', '5')
                  group by out_mall_id, lease.annual, lease.charge_type, lease.store_type
              ) t
                  lateral view
                      explode(split(t.res, ",")) t1 as month_budget_money
         where length(month_budget_money) > 0
           and substr(month_budget_money, instr(month_budget_money, '-') + 1, 1) = '1'

         union all
         -- 物管 预算
         select t.bis_project_id,                                                                  -- 项目id
                t.annual,                                                                          -- 年
                substr(month_budget_money, 0, instr(month_budget_money, '-') - 1) as budget_month, -- 月
                t.charge_type,                                                                     -- 物业类型 （1购物中心2商业街3住宅4住宅底商5写字楼 6住宅公寓7住宅别墅）
                t.store_type,                                                                      -- 1 铺位  2 多径
                substr(month_budget_money, instr(month_budget_money, '-') + 1, 1) as fee_type,     -- 费项 (1:租金 2：物管 3： 多经)
                substr(month_budget_money, instr(month_budget_money, '=') + 1)    as budget_money  -- 租金 物管 多经 每个月的预算金额
         from (
                  SELECT out_mall_id as bis_project_id, -- 项目id
                         lease.annual,                  -- 年
                         lease.charge_type,             -- 物业类型 （1购物中心2商业街3住宅4住宅底商5写字楼 6住宅公寓7住宅别墅）
                         lease.store_type,              -- 1 铺位  2 多径
-- if(lease.store_type = '1 ：铺位','1：租金','3：多经')   -- if(lease.store_type = '1 ：铺位','1：物管','无效‘)
                         concat_ws(',',
                                   concat('1', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(jan_zj))),
                                   concat('2', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(feb_zj))),
                                   concat('3', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(march_zj))),
                                   concat('4', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(april_zj))),
                                   concat('5', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(may_zj))),
                                   concat('6', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(june_zj))),
                                   concat('7', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(july_zj))),
                                   concat('8', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(aug_zj))),
                                   concat('9', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(sep_zj))),
                                   concat('10', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(oct_zj))),
                                   concat('11', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(nov_zj))),
                                   concat('12', '-', if(lease.store_type = '1', '1', '3'), '=',
                                          string(sum(dece_zj))),
                                   if(string(sum(jan_wg)) is null, null,
                                      concat('1', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(jan_wg)))),
                                   if(string(sum(feb_wg)) is null, null,
                                      concat('2', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(feb_wg)))),
                                   if(string(sum(march_wg)) is null, null,
                                      concat('3', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(march_wg)))),
                                   if(string(sum(april_wg)) is null, null,
                                      concat('4', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(april_wg)))),
                                   if(string(sum(may_wg)) is null, null,
                                      concat('5', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(may_wg)))),
                                   if(string(sum(june_wg)) is null, null,
                                      concat('6', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(june_wg)))),
                                   if(string(sum(july_wg)) is null, null,
                                      concat('7', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(july_wg)))),
                                   if(string(sum(aug_wg)) is null, null,
                                      concat('8', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(aug_wg)))),
                                   if(string(sum(sep_wg)) is null, null,
                                      concat('9', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(sep_wg)))),
                                   if(string(sum(oct_wg)) is null, null,
                                      concat('10', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(oct_wg)))),
                                   if(string(sum(nov_wg)) is null, null,
                                      concat('11', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(nov_wg)))),
                                   if(string(sum(dece_wg)) is null, null,
                                      concat('12', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(dece_wg))))
                             )       as res
                  FROM ods.ods_pl_pms_bis_db_bs_mall_dt m
                           inner join ods.ods_pl_pms_budget_db_budget_instance_dt instance
                                      on m.id = instance.project_id
                           INNER JOIN ods.ods_pl_pms_budget_db_budget_instance_sheet_dt instanceSheet
                                      ON instance.id = instanceSheet.budget_instance_id
                                          AND instance.instance_type = 1
                                          AND instanceSheet.budget_sheet_id in (29, 52)
                           inner join ods.ods_pl_pms_budget_db_budget_instance_biz_lease_budget_dt lease
                                      on instanceSheet.id = lease.budget_instance_sheet_id and
                                         lease.is_del = false and lease.store_type = '1'
                  where m.is_del = '0'
                    and m.stat = '2'
                    and lease.curr_cont_type in ('1', '2', '3', '5', '7', '8', '10', '11', '12', '13')
                  group by out_mall_id, lease.annual, lease.charge_type, lease.store_type
              ) t
                  lateral view
                      explode(split(t.res, ",")) t1 as month_budget_money
         where length(month_budget_money) > 0
           and substr(month_budget_money, instr(month_budget_money, '-') + 1, 1) = '2'
     ) t3 on t1.bis_project_id = t3.bis_project_id and t1.year = t3.annual and
             t1.month = t3.budget_month and t1.must_type = t3.fee_type and
             t1.store_type = t3.charge_type
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

union all

select t2.area_name,                   -- 区域名称
       t2.id,                          -- 区域id
       t.bis_project_id,               -- 项目id
       t.short_name,                   -- 项目名称
       t.year,                         -- 年
       t.month,                        -- 月
       t.year_month1,                  -- 年月 2021-12
       t.must_type,                    -- 费项（3：多经）
       t.store_type,                   -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
       t.must_money_qz,                -- 多经权责月应收金额
       t.fact_money_qz,                -- 多经权责月实收金额
       t1.must_money_cont,             -- 多经每月合同口径应收金额
       t1.fact_money_cont,             -- 多经每月合同口径实收金额
       t3.budget_money,                -- 多经 预算金额
       t.total_qz_month_fact_money,    -- 权责月月累计（月递归累计）实收金额
       t.total_qz_year_fact_money,     -- 权责月年累计实收金额
       t.total_qz_month_must_money,    -- 权责月月累计（月递归累计）应收金额
       t.total_qz_year_must_money,     -- 权责月年累计应收金额
       t1.total_cont_month_fact_money, -- 合同口径（应收月）月累计（月递归累计）实收金额
       t1.total_cont_year_fact_money,  -- 合同口径（应收月）年累计实收金额
       t1.total_cont_month_must_money, -- 合同口径（应收月）月累计（月递归累计）应收金额
       t1.total_cont_year_must_money,  -- 合同口径（应收月）年累计应收金额
       CURRENT_TIMESTAMP               -- ETL时间
from (
         -- 多经权责口径--每月应收 和 实收
         select t.bis_project_id,                                                   -- 项目id
                t.short_name,                                                       -- 项目名称
                t.year,                                                             -- 年
                t.month,                                                            -- 月
                t.year_month1,                                                      -- 年月 2021-12
                t.store_type,                                                       -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                t.must_type,                                                        -- 费项（3：多经）
                sum(nvl(t.must_money, 0))                must_money_qz,             -- 多经权责月应收金额
                sum(nvl(t.fact_money, 0))                fact_money_qz,             -- 多经权责月实收金额
                sum(nvl(t.total_qz_month_fact_money, 0)) total_qz_month_fact_money, -- 权责月月累计（月递归累计）实收金额
                sum(nvl(t.total_qz_year_fact_money, 0))  total_qz_year_fact_money,  -- 权责月年累计实收金额
                sum(nvl(t.total_qz_month_must_money, 0)) total_qz_month_must_money, -- 权责月月累计（月递归累计）应收金额
                sum(nvl(t.total_qz_year_must_money, 0))  total_qz_year_must_money   -- -- 权责月年累计应收金额
         from (
                  select t1.bis_project_id,                        -- 项目id
                         t1.short_name,                            -- 项目名称
                         t1.year,                                  -- 年
                         t1.month,                                 -- 月
                         t1.year_month1,                           -- 年月 2021-12
                         '3' as must_type,                         -- 费项（3：多经）
                         t1.store_type,                            -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                         bis_must_basic.must_money,                -- 应收金额
                         bis_must_basic.total_qz_month_must_money, -- 权责月月累计（月递归累计）应收金额
                         bis_must_basic.total_qz_year_must_money,  -- -- 权责月年累计应收金额
                         bis_fact_basic.fact_money,                -- 实收金额
                         bis_fact_basic.total_qz_month_fact_money, -- 权责月月累计（月递归累计）实收金额
                         bis_fact_basic.total_qz_year_fact_money   -- 权责月年累计实收金额


                  from (
                           select distinct bis_project.bis_project_id,
                                           bis_project.short_name,
                                           year,
                                           month,
                                           year_month1,
                                           t.store_type
                           from dim.dim_pl_date,
                                ods.ods_pl_powerdes_bis_project_dt bis_project,
                                (select distinct store_type from ods.ods_pl_powerdes_bis_cont_dt) t
                           where year between '2010' and '2030'
                             and bis_project.is_business_project = '1'
                             and oper_status = '2'
                       ) t1
                           left join
                       (
                           -- 应收
                           select bis_project_id,
                                  qz_year_month,
                                  store_type,
                                  must_type,
                                  round(sum(must_money), 2) must_money,
                                  round(sum(round(sum(must_money), 2))
                                            over (partition by bis_project_id, substr(qz_year_month, 0, 4),store_type, must_type order by bis_project_id,qz_year_month,store_type,must_type),
                                        2)                  total_qz_month_must_money, -- 权责月月累计（月递归累计）应收金额
                                  round(sum(round(sum(must_money), 2))
                                            over (partition by bis_project_id, substr(qz_year_month, 0, 4),store_type, must_type order by bis_project_id, substr(qz_year_month, 0, 4),store_type, must_type),
                                        2)                  total_qz_year_must_money   -- 权责月年累计应收金额
                           from dwd.dwd_bis_must_qz_basic_big_dt bis_must_basic
                           where bis_must_basic.must_type in ('34', '57') -- 34：场地使用费 57 ：展览展示费
                             and bis_must_basic.dt = date_format(current_date, 'yyyyMMdd')
                           group by bis_project_id, qz_year_month, substr(qz_year_month, 0, 4), store_type, must_type
                       ) bis_must_basic
                       on t1.bis_project_id = bis_must_basic.bis_project_id
                           and t1.year_month1 = bis_must_basic.qz_year_month and
                          t1.store_type = bis_must_basic.store_type

                           left join
                       (
                           -- 实收
                           select bis_project_id,
                                  store_type,
                                  qz_year_month,
                                  fact_type,
                                  round(sum(fact_money), 2) fact_money,
                                  null                      total_qz_month_fact_money, -- 权责月月累计（月递归累计）实收金额
                                  round(sum(round(sum(fact_money), 2))
                                            over (partition by bis_project_id, substr(qz_year_month, 0, 4),store_type, fact_type order by bis_project_id, substr(qz_year_month, 0, 4),store_type, fact_type),
                                        2)                  total_qz_year_fact_money   -- 权责月年累计实收金额
                           from dwd.dwd_bis_fact_basic_big_dt bis_fact_basic
                           where bis_fact_basic.fact_type in ('34', '57')                           -- 34：场地使用费 57 ：展览展示费
                             and bis_fact_basic.dt = date_format(current_date, 'yyyyMMdd')
                             and substr(fact_date, 0, 10) <= last_day(concat(qz_year_month, '-01')) -- 权责月最后一天
                           group by bis_project_id, store_type, qz_year_month, substr(qz_year_month, 0, 4), fact_type
                       ) bis_fact_basic
                       on bis_must_basic.bis_project_id = bis_fact_basic.bis_project_id
                           and bis_must_basic.qz_year_month = bis_fact_basic.qz_year_month
                           and bis_must_basic.must_type = bis_fact_basic.fact_type
                           and bis_must_basic.store_type = bis_fact_basic.store_type
              ) t
         group by t.bis_project_id, t.short_name, t.year_month1, t.year, t.month, t.store_type, t.must_type
     ) t
         left join
     (
         -- 多经合同口径应收实收
         select t4.bis_project_id,                                 -- 项目id
                t4.short_name,                                     -- 项目名称
                t4.year,                                           -- 年
                t4.month,                                          -- 月
                t4.year_month1,                                    -- 应收年月
                t6.must_type,                                      -- 费项（3：多经）
                t4.store_type,                                     -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                t6.mustMoney          must_money_cont,             -- 月应收金额
                t6.month_lj_mustMOney total_cont_month_must_money, -- 合同口径（应收月）月累计（月递归累计）应收金额
                t6.year_lj_mustMoney  total_cont_year_must_money,  -- 合同口径（应收月）年累计应收金额
                t6.factMoney          fact_money_cont,             -- 月实收金额
                t6.month_lj_factMoney total_cont_month_fact_money, -- 合同口径（应收月）月累计（月递归累计）实收金额
                t6.year_lf_factMoney  total_cont_year_fact_money   -- 合同口径（应收月）年累计实收金额

         from (
                  select distinct bis_project.bis_project_id,
                                  bis_project.short_name,
                                  year,
                                  month,
                                  year_month1,
                                  t.store_type
                  from dim.dim_pl_date,
                       ods.ods_pl_powerdes_bis_project_dt bis_project,
                       (select distinct store_type from ods.ods_pl_powerdes_bis_cont_dt) t
                  where year between '2010' and '2030'
                    and bis_project.is_business_project = '1'
                    and oper_status = '2'
              ) t4
                  left join
              (
                  select t2.BIS_PROJECT_ID,                                  -- 项目id
                         '3' as                          MUST_TYPE,          -- 费项
                         t3.STORE_TYPE,                                      -- 物业类型
                         t2.must_year_month,                                 -- 应收年月
                         sum(nvl(t2.mustMoney, 0))       mustMoney,          -- 月应收
                         sum(nvl(t2.factMoney, 0))       factMoney,          -- 月实收
                         sum(nvl(month_lj_mustMOney, 0)) month_lj_mustMOney, -- 月累计应收
                         sum(nvl(month_lj_factMoney, 0)) month_lj_factMoney, -- 月累计实收
                         sum(nvl(year_lj_mustMoney, 0))  year_lj_mustMoney,  -- 年累计应收
                         sum(nvl(year_lf_factMoney, 0))  year_lf_factMoney   -- 年累计实收
                  from ods.ods_pl_powerdes_bis_cont_dt t3
                           inner join
                       (
                           select table1.bis_project_id,     -- 项目id
                                  table1.bis_cont_id,        -- 合同id
                                  table1.MUST_TYPE,          -- 费项（'34', '57'）
                                  table1.must_year_month,    -- 应收月
                                  table3.mustMoney,          -- 月应收
                                  table1.factMoney,          -- 月实收
                                  table1.month_lj_factMoney, -- 月累计实收
                                  table1.year_lf_factMoney,  -- 年累计实收
                                  table3.month_lj_mustMOney, -- 月累计应收
                                  table3.year_lj_mustMoney   -- 年累计应收
                           from (
                                    select t.bis_project_id,                                      -- 项目id
                                           t.bis_cont_id,                                         -- 合同id
                                           t.MUST_TYPE,                                           -- 费项（'34', '57'）
                                           substr(t.rent_last_pay_date, 0, 7) must_year_month,    -- 应收月
                                           null                               mustMoney,          -- 月应收
                                           sum(case
                                                   when substr(fact.fact_date, 0, 7) <= substr(t.rent_last_pay_date, 0, 7)
                                                       then nvl(fact.factMoney, 0)
                                                   else 0 end)                factMoney,          -- 月实收
                                           null                               month_lj_factMoney, -- 月累计实收

                                           null                               year_lf_factMoney   -- 年累计实收


                                    from (
                                             -- 应收
                                             SELECT BIS_MUST2.bis_project_id,
                                                    BIS_MUST2.bis_cont_id,
                                                    BIS_MUST2.qz_year_month,
                                                    BIS_MUST2.MUST_TYPE,
                                                    BIS_MUST2.billing_period_end,
                                                    BIS_MUST2.billing_period_begin,
                                                    BIS_MUST2.bis_must_id,
                                                    BIS_MUST2.rent_last_pay_date, -- 应收日期
                                                    sum(nvl(BIS_MUST2.money, 0)) as mustMoney
                                             FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                             where BIS_MUST2.must_type in ('34', '57')
                                               and BIS_MUST2.is_show = 1
                                               and BIS_MUST2.is_delete = 0
                                             group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                                      BIS_MUST2.bis_must_id,
                                                      BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                                                      BIS_MUST2.billing_period_end,
                                                      BIS_MUST2.billing_period_begin, BIS_MUST2.rent_last_pay_date
                                         ) t
                                             left join
                                         (
                                             select must.bis_must_id,
                                                    adj.adjMoney
                                             from (
                                                      -- 应收
                                                      SELECT BIS_MUST2.bis_project_id,
                                                             BIS_MUST2.bis_cont_id,
                                                             BIS_MUST2.qz_year_month,
                                                             BIS_MUST2.MUST_TYPE,
                                                             BIS_MUST2.billing_period_end,
                                                             BIS_MUST2.billing_period_begin,
                                                             min(BIS_MUST2.rent_last_pay_date) rent_last_pay_date, -- 应收日期
                                                             min(BIS_MUST2.bis_must_id)   as   bis_must_id,
                                                             sum(nvl(BIS_MUST2.money, 0)) as   mustMoney
                                                      FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                                      where BIS_MUST2.must_type in ('34', '57')
                                                        and BIS_MUST2.is_show = 1
                                                        and BIS_MUST2.is_delete = 0
                                                      group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                                               BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                                                               BIS_MUST2.billing_period_end,
                                                               BIS_MUST2.billing_period_begin
                                                  ) must
                                                      left join
                                                  (
                                                      -- 减免
                                                      SELECT bis_mf_adjust.bis_cont_id,
                                                             bis_mf_adjust.billing_period_end,
                                                             bis_mf_adjust.billing_period_begin,
                                                             bis_mf_adjust.qz_year_month,
                                                             bis_mf_adjust.FEE_TYPE,
                                                             sum(nvl(bis_mf_adjust.adjust_money, 0)) as adjMoney
                                                      FROM ods.ods_pl_powerdes_bis_mf_adjust_dt bis_mf_adjust
                                                      where bis_mf_adjust.is_del = 1
                                                        and bis_mf_adjust.fee_type in ('34', '57')
                                                      group by bis_mf_adjust.bis_cont_id,
                                                               bis_mf_adjust.qz_year_month,
                                                               bis_mf_adjust.FEE_TYPE,
                                                               bis_mf_adjust.billing_period_end,
                                                               bis_mf_adjust.billing_period_begin
                                                  ) adj
                                                      -- 合同 权责 费项 账期开始  账期结束
                                                  on must.bis_cont_id = adj.bis_cont_id and
                                                     must.qz_year_month = adj.qz_year_month and
                                                     must.MUST_TYPE = adj.FEE_TYPE and
                                                     must.billing_period_end = adj.billing_period_end and
                                                     must.billing_period_begin = adj.billing_period_begin
                                         ) t1 on t.bis_must_id = t1.bis_must_id
                                             left join
                                         (
                                             -- 实收
                                             SELECT bis_fact2.bis_cont_id,
                                                    bis_fact2.bis_must_id,
                                                    bis_fact2.fact_type,
                                                    bis_fact2.fact_date,
                                                    bis_fact2.must_rent_last_pay_date,
                                                    sum(nvl(bis_fact2.money, 0)) as factMoney
                                             FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
                                             where bis_fact2.is_delete = 1
                                               and bis_fact2.fact_type in ('34', '57')
                                               and bis_fact2.bis_must_id is not null
                                             group by bis_fact2.bis_cont_id, bis_fact2.bis_must_id, bis_fact2.fact_type,
                                                      bis_fact2.fact_date,
                                                      bis_fact2.must_rent_last_pay_date
                                         ) fact
                                         on t.bis_cont_id = fact.bis_cont_id and
                                            t.bis_must_id = fact.bis_must_id and
                                            t.MUST_TYPE = fact.FACT_TYPE
                                    group by t.bis_project_id, t.bis_cont_id, substr(t.rent_last_pay_date, 0, 4),
                                             substr(t.rent_last_pay_date, 0, 7), t.MUST_TYPE
                                ) table1
                                    left join
                                (
                                    select t.bis_project_id,                                                  -- 项目id
                                           t.bis_cont_id,                                                     -- 合同id
                                           t.MUST_TYPE,                                                       -- 费项（1：租金 2：物管）
                                           substr(t.rent_last_pay_date, 0, 7)             must_year_month,    -- 应收月
                                           sum(nvl(t.mustMoney, 0) + nvl(t1.adjMoney, 0)) mustMoney,          -- 月应收
                                           null                                           month_lj_mustMOney, -- 月累计应收
                                           null                                           year_lj_mustMoney   -- 年累计应收

                                    from (
                                             -- 应收
                                             SELECT BIS_MUST2.bis_project_id,
                                                    BIS_MUST2.bis_cont_id,
                                                    BIS_MUST2.qz_year_month,
                                                    BIS_MUST2.MUST_TYPE,
                                                    BIS_MUST2.billing_period_end,
                                                    BIS_MUST2.billing_period_begin,
                                                    BIS_MUST2.bis_must_id,
                                                    BIS_MUST2.rent_last_pay_date, -- 应收日期
                                                    sum(nvl(BIS_MUST2.money, 0)) as mustMoney
                                             FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                             where BIS_MUST2.must_type in ('34', '57')
                                               and BIS_MUST2.is_show = 1
                                               and BIS_MUST2.is_delete = 0
                                             group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                                      BIS_MUST2.bis_must_id,
                                                      BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                                                      BIS_MUST2.billing_period_end,
                                                      BIS_MUST2.billing_period_begin,
                                                      BIS_MUST2.rent_last_pay_date
                                         ) t
                                             left join
                                         (
                                             select must.bis_must_id,
                                                    adj.adjMoney
                                             from (
                                                      -- 应收
                                                      SELECT BIS_MUST2.bis_project_id,
                                                             BIS_MUST2.bis_cont_id,
                                                             BIS_MUST2.qz_year_month,
                                                             BIS_MUST2.MUST_TYPE,
                                                             BIS_MUST2.billing_period_end,
                                                             BIS_MUST2.billing_period_begin,
                                                             min(BIS_MUST2.rent_last_pay_date) rent_last_pay_date, -- 应收日期
                                                             min(BIS_MUST2.bis_must_id)   as   bis_must_id,
                                                             sum(nvl(BIS_MUST2.money, 0)) as   mustMoney
                                                      FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                                      where BIS_MUST2.must_type in ('34', '57')
                                                        and BIS_MUST2.is_show = 1
                                                        and BIS_MUST2.is_delete = 0
                                                      group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                                               BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                                                               BIS_MUST2.billing_period_end,
                                                               BIS_MUST2.billing_period_begin
                                                  ) must
                                                      left join
                                                  (
                                                      -- 减免
                                                      SELECT bis_mf_adjust.bis_cont_id,
                                                             bis_mf_adjust.billing_period_end,
                                                             bis_mf_adjust.billing_period_begin,
                                                             bis_mf_adjust.qz_year_month,
                                                             bis_mf_adjust.FEE_TYPE,
                                                             sum(nvl(bis_mf_adjust.adjust_money, 0)) as adjMoney
                                                      FROM ods.ods_pl_powerdes_bis_mf_adjust_dt bis_mf_adjust
                                                      where bis_mf_adjust.is_del = 1
                                                        and bis_mf_adjust.fee_type in ('34', '57')
                                                      group by bis_mf_adjust.bis_cont_id,
                                                               bis_mf_adjust.qz_year_month,
                                                               bis_mf_adjust.FEE_TYPE,
                                                               bis_mf_adjust.billing_period_end,
                                                               bis_mf_adjust.billing_period_begin
                                                  ) adj
                                                      -- 合同 权责 费项 账期开始  账期结束
                                                  on must.bis_cont_id = adj.bis_cont_id and
                                                     must.qz_year_month = adj.qz_year_month and
                                                     must.MUST_TYPE = adj.FEE_TYPE and
                                                     must.billing_period_end = adj.billing_period_end and
                                                     must.billing_period_begin = adj.billing_period_begin
                                         ) t1 on t.bis_must_id = t1.bis_must_id
                                    group by t.bis_project_id, t.bis_cont_id,
                                             substr(t.rent_last_pay_date, 0, 4),
                                             substr(t.rent_last_pay_date, 0, 7), t.MUST_TYPE
                                ) table3
                                on table1.bis_project_id = table3.bis_project_id
                                    and table1.bis_cont_id = table3.bis_cont_id
                                    and table1.must_type = table3.must_type
                                    and table1.must_year_month = table3.must_year_month
                       ) t2 on t3.BIS_PROJECT_ID = t2.BIS_PROJECT_ID and t3.BIS_CONT_ID = t2.BIS_CONT_ID
                  group by t2.BIS_PROJECT_ID,
                           t3.STORE_TYPE,
                           t2.must_year_month
              ) t6
              on t4.bis_project_id = t6.bis_project_id and t4.year_month1 = t6.must_year_month and
                 t4.store_type = t6.store_type
     ) t1 on t.bis_project_id = t1.bis_project_id and t.short_name = t1.short_name
         and t.year_month1 = t1.year_month1 and t.store_type = t1.store_type and t.must_type = t1.must_type
         left join
     (
         -- 多经 物管 租金 预算
         select t.bis_project_id,                                                                  -- 项目id
                t.annual,                                                                          -- 年
                substr(month_budget_money, 0, instr(month_budget_money, '-') - 1) as budget_month, -- 月
                t.charge_type,                                                                     -- 物业类型 （1购物中心2商业街3住宅4住宅底商5写字楼 6住宅公寓7住宅别墅）
                t.store_type,                                                                      -- 1 铺位  2 多径
                substr(month_budget_money, instr(month_budget_money, '-') + 1, 1) as fee_type,     -- 费项 (1:租金 2：物管 3： 多经)
                substr(month_budget_money, instr(month_budget_money, '=') + 1)    as budget_money  -- 租金 物管 多经 每个月的预算金额
         from (
                  SELECT out_mall_id as bis_project_id, -- 项目id
                         lease.annual,                  -- 年
                         lease.charge_type,             -- 物业类型 （1购物中心2商业街3住宅4住宅底商5写字楼 6住宅公寓7住宅别墅）
                         lease.store_type,              -- 1 铺位  2 多径
                         -- if(lease.store_type = '1 ：铺位','1：租金','3：多经')   -- if(lease.store_type = '1 ：铺位','1：物管','无效‘)
                         concat_ws(',',
                                   concat('1', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(jan_zj))),
                                   concat('2', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(feb_zj))),
                                   concat('3', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(march_zj))),
                                   concat('4', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(april_zj))),
                                   concat('5', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(may_zj))),
                                   concat('6', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(june_zj))),
                                   concat('7', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(july_zj))),
                                   concat('8', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(aug_zj))),
                                   concat('9', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(sep_zj))),
                                   concat('10', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(oct_zj))),
                                   concat('11', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(nov_zj))),
                                   concat('12', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(dece_zj))),
                                   if(string(sum(jan_wg)) is null, null,
                                      concat('1', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(jan_wg)))),
                                   if(string(sum(feb_wg)) is null, null,
                                      concat('2', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(feb_wg)))),
                                   if(string(sum(march_wg)) is null, null,
                                      concat('3', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(march_wg)))),
                                   if(string(sum(april_wg)) is null, null,
                                      concat('4', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(april_wg)))),
                                   if(string(sum(may_wg)) is null, null,
                                      concat('5', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(may_wg)))),
                                   if(string(sum(june_wg)) is null, null,
                                      concat('6', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(june_wg)))),
                                   if(string(sum(july_wg)) is null, null,
                                      concat('7', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(july_wg)))),
                                   if(string(sum(aug_wg)) is null, null,
                                      concat('8', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(aug_wg)))),
                                   if(string(sum(sep_wg)) is null, null,
                                      concat('9', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(sep_wg)))),
                                   if(string(sum(oct_wg)) is null, null,
                                      concat('10', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(oct_wg)))),
                                   if(string(sum(nov_wg)) is null, null,
                                      concat('11', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(nov_wg)))),
                                   if(string(sum(dece_wg)) is null, null,
                                      concat('12', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(dece_wg))))
                             )       as res

                  FROM ods.ods_pl_pms_bis_db_bs_mall_dt m
                           inner join ods.ods_pl_pms_budget_db_budget_instance_dt instance
                                      on m.id = instance.project_id
                           INNER JOIN ods.ods_pl_pms_budget_db_budget_instance_sheet_dt instanceSheet
                                      ON instance.id = instanceSheet.budget_instance_id
                                          AND instance.instance_type = 1
                                          AND instanceSheet.budget_sheet_id in (29, 52)
                           inner join ods.ods_pl_pms_budget_db_budget_instance_biz_lease_dt lease
                                      on instanceSheet.id = lease.budget_instance_sheet_id and
                                         lease.is_del = false and lease.store_type = '2'
                  where m.is_del = '0'
                    and m.stat = '2'
                  group by out_mall_id, lease.annual, lease.charge_type, lease.store_type
              ) t
                  lateral view
                      explode(split(t.res, ",")) t1 as month_budget_money
         where length(month_budget_money) > 0
     ) t3 on t1.bis_project_id = t3.bis_project_id and t1.year = t3.annual and
             t1.month = t3.budget_month and t1.must_type = t3.fee_type and
             t1.store_type = t3.charge_type
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
