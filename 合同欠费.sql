select t2.area_name,     -- 区域名称
       t2.id,            -- 区域id
       t.bis_project_id, -- 项目id
       t.short_name,     -- 项目名称
       t.year,           -- 年
       t.month,          -- 月
       t.year_month1,    -- 年月 2021-12
       t.bis_shop_id,    -- 商家id
       t.name_cn,        -- 商家名称
       t.must_type,      -- 费项（1：租金 2：物管）
       t.store_type,     -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
       t.owe_qz,         -- 欠费（权责月）
       t1.owe_cont,      -- 欠费（应收月）
       t.bis_cont_id     -- 合同id
from (
         -- 权责欠费（权责月）
         select t1.bis_project_id, -- 项目id
                t1.short_name,     -- 项目名称
                t1.year,           -- 年
                t1.month,          -- 月
                t1.year_month1,    -- 年月 2021-12
                t1.bis_shop_id,    -- 商家id
                t1.name_cn,        -- 商家名称
                t1.must_type,      -- 费项（1：租金 2：物管）
                t1.store_type,     -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                t2.owe_qz,         -- 欠费（权责月）
                t1.bis_cont_id     -- 合同id

         from (
                  select bis_project_id,
                         short_name,
                         year,
                         month,
                         year_month1,
                         NAME_CN,
                         bis_shop_id,
                         bis_cont_id,
                         store_type,
                         must_type
                  from dwd.dwd_bis_cross_join_result_big_dt
                  where dt = date_format(current_date(), 'yyyyMMdd')
                  --and bis_project_id = '402834702b486353012b4ce1151706291'
                  --and year_month1 like '2021%'
                  --and bis_cont_id = '8a7b868b726ff6490172744a8d217e9f'
              ) t1
                  left join
              (
                  select table1.BIS_PROJECT_ID, -- 项目id
                         t.PROJECT_NAME,        -- 项目名称
                         table1.BIS_CONT_ID,    -- 合同id
                         table1.STORE_TYPE,     -- 物业类型
                         table1.BIS_SHOP_ID,    -- 商家id
                         table1.NAME_CN,        -- 商家名称
                         t.qz_year_month,       -- 权责月
                         t.MUST_TYPE,           -- 费项
                         t.oweMoney owe_qz      -- 欠费
                  from (
                           -- 合同表
                           select BIS_CONT.bis_project_id,
                                  BIS_CONT.bis_cont_id,
                                  BIS_CONT.bis_shop_id,
                                  bis_cont.STORE_TYPE,                           -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                                  case
                                      when bis_cont.bis_shop_name is null then bis_shop.NAME_CN
                                      else bis_cont.bis_shop_name end as NAME_CN -- 结果表取合同表中品牌名称，如合同表中品牌名称为空取品牌表中名称，  如全部为空  及为空
                           from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                                    left join ods.ods_pl_powerdes_bis_shop_dt bis_shop
                                              on BIS_CONT.BIS_SHOP_ID = BIS_SHOP.BIS_SHOP_ID
                       ) table1
                           inner join
                       (
                           select bis_project.project_name, -- 项目名称
                                  d.bis_project_id,         -- 项目id
                                  d.bis_cont_id,            -- 合同id
                                  d.qz_year_month,          -- 权责月
                                  d.MUST_TYPE,              -- 费项
                                  d.oweMoney                -- 欠费
                           from (
                                    select must.bis_project_id,
                                           must.bis_cont_id,
                                           must.qz_year_month,
                                           must.MUST_TYPE,
                                           sum(nvl(mustMoney, 0)) + sum(nvl(adjMoney, 0)) -
                                           sum(nvl(factMoney, 0)) as oweMoney
                                    from (
                                             -- 应收
                                             SELECT BIS_MUST2.bis_project_id,
                                                    BIS_MUST2.bis_cont_id,
                                                    BIS_MUST2.qz_year_month,
                                                    BIS_MUST2.MUST_TYPE,
                                                    sum(nvl(BIS_MUST2.money, 0)) as mustMoney
                                             FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                             where BIS_MUST2.must_type in ('1', '2')
                                               and BIS_MUST2.is_show = 1
                                               and BIS_MUST2.is_delete = 0
                                             group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                                      BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE
                                         ) must
                                             left join
                                         (
                                             -- 减免
                                             SELECT bis_mf_adjust.bis_cont_id,
                                                    bis_mf_adjust.qz_year_month,
                                                    bis_mf_adjust.FEE_TYPE,
                                                    sum(nvl(bis_mf_adjust.adjust_money, 0)) as adjMoney
                                             FROM ods.ods_pl_powerdes_bis_mf_adjust_dt bis_mf_adjust
                                             where bis_mf_adjust.is_del = 1
                                               and bis_mf_adjust.fee_type in ('1', '2')
                                             group by bis_mf_adjust.bis_cont_id, bis_mf_adjust.qz_year_month,
                                                      bis_mf_adjust.FEE_TYPE
                                         ) adj
                                         on must.bis_cont_id = adj.bis_cont_id and
                                            must.qz_year_month = adj.qz_year_month and
                                            must.MUST_TYPE = adj.FEE_TYPE
                                             left join
                                         (
                                             -- 实收
                                             SELECT bis_fact2.bis_cont_id,
                                                    bis_fact2.qz_year_month,
                                                    bis_fact2.FACT_TYPE,
                                                    sum(nvl(bis_fact2.money, 0)) as factMoney
                                             FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
                                             where bis_fact2.is_delete = 1
                                               and bis_fact2.fact_type in ('1', '2')
                                             group by bis_fact2.bis_cont_id, bis_fact2.qz_year_month,
                                                      bis_fact2.FACT_TYPE
                                         ) fact
                                         on must.bis_cont_id = fact.bis_cont_id and
                                            must.qz_year_month = fact.qz_year_month and
                                            must.MUST_TYPE = fact.FACT_TYPE
                                    group by must.bis_project_id, must.bis_cont_id, must.qz_year_month, must.MUST_TYPE
                                ) d
                                    left join ods.ods_pl_powerdes_bis_project_dt bis_project
                                              on d.bis_project_id = bis_project.bis_project_id
                       ) t on table1.BIS_PROJECT_ID = t.BIS_PROJECT_ID and table1.BIS_CONT_ID = t.BIS_CONT_ID
              ) t2 on t1.bis_project_id = t2.bis_project_id and t1.bis_cont_id = t2.bis_cont_id and
                      t1.year_month1 = t2.qz_year_month and t1.must_type = t2.must_type
     ) t
         left join
     (
         -- 合同欠费（应收日期）
         select t1.bis_project_id, -- 项目id
                t1.short_name,     -- 项目名称
                t1.year,           -- 年
                t1.month,          -- 月
                t1.year_month1,    -- 年月 2021-12
                t1.bis_shop_id,    -- 商家id
                t1.name_cn,        -- 商家名称
                t1.must_type,      -- 费项（1：租金 2：物管）
                t1.store_type,     -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                t2.owe_cont,       -- 欠费（应收月）
                t1.bis_cont_id     -- 合同id

         from (
                  select bis_project_id,
                         short_name,
                         year,
                         month,
                         year_month1,
                         NAME_CN,
                         bis_shop_id,
                         bis_cont_id,
                         store_type,
                         must_type
                  from dwd.dwd_bis_cross_join_result_big_dt
                  where dt = date_format(current_date(), 'yyyyMMdd')
                  --and bis_project_id = '402834702b486353012b4ce1151706291'
                  --and year_month1 like '2021%'
                  --and bis_cont_id = '8a7b868b726ff6490172744a8d217e9f'
              ) t1
                  left join
              (
                  select table1.BIS_PROJECT_ID, -- 项目id
                         t.PROJECT_NAME,        -- 项目名称
                         table1.BIS_CONT_ID,    -- 合同id
                         table1.STORE_TYPE,     -- 物业类型
                         table1.BIS_SHOP_ID,    -- 商家id
                         table1.NAME_CN,        -- 商家名称
                         t.must_year_month,     -- 应收月
                         t.MUST_TYPE,           -- 费项
                         t.oweMoney owe_cont    -- 欠费
                  from (
                           -- 合同表
                           select BIS_CONT.bis_project_id,
                                  BIS_CONT.bis_cont_id,
                                  BIS_CONT.bis_shop_id,
                                  bis_cont.STORE_TYPE,                           -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                                  case
                                      when bis_cont.bis_shop_name is null then bis_shop.NAME_CN
                                      else bis_cont.bis_shop_name end as NAME_CN -- 结果表取合同表中品牌名称，如合同表中品牌名称为空取品牌表中名称，  如全部为空  及为空
                           from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                                    left join ods.ods_pl_powerdes_bis_shop_dt bis_shop
                                              on BIS_CONT.BIS_SHOP_ID = BIS_SHOP.BIS_SHOP_ID
                       ) table1
                           inner join
                       (
                           select bis_project.project_name, -- 项目名称
                                  d.bis_project_id,         -- 项目id
                                  d.bis_cont_id,            -- 合同id
                                  d.must_year_month,        -- 应收月
                                  d.MUST_TYPE,              -- 费项
                                  d.oweMoney                -- 欠费
                           from (
                                    select t.bis_project_id,                                   -- 项目id
                                           t.bis_cont_id,                                      -- 合同id
                                           t.MUST_TYPE,                                        -- 费项（1：租金 2：物管）
                                           substr(t.rent_last_pay_date, 0, 7) must_year_month, -- 应收月
                                           sum(nvl(t.mustMoney, 0)) + sum(nvl(t1.adjMoney, 0)) -
                                           sum(nvl(fact.factMoney, 0))        oweMoney         -- 欠费
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
                                                      BIS_MUST2.billing_period_begin, BIS_MUST2.rent_last_pay_date
                                         ) t
                                             left join
                                         (
                                             -- 特殊情况：两条应收除了 应收id和应收日期不一样（权责 ，费项 账期开始、账期结束 都一样），这个时候减免没法知道和哪条应收对应
                                             -- 处理方法：取这两条应收中应收日期最小的一条应收和减免关联
                                             select must.bis_must_id, -- 应收id
                                                    adj.adjMoney      -- 减免
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
                                                      group by BIS_MUST2.bis_project_id,
                                                               BIS_MUST2.bis_cont_id,
                                                               BIS_MUST2.qz_year_month,
                                                               BIS_MUST2.MUST_TYPE,
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
                                                    sum(nvl(bis_fact2.money, 0)) as factMoney
                                             FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
                                             where bis_fact2.is_delete = 1
                                               and bis_fact2.fact_type in ('1', '2')
                                               and bis_fact2.bis_must_id is not null
                                             group by bis_fact2.bis_cont_id, bis_fact2.bis_must_id, bis_fact2.fact_type
                                         ) fact
                                         on t.bis_cont_id = fact.bis_cont_id and
                                            t.bis_must_id = fact.bis_must_id and
                                            t.MUST_TYPE = fact.FACT_TYPE
                                    group by t.bis_project_id, t.bis_cont_id, substr(t.rent_last_pay_date, 0, 7),
                                             t.MUST_TYPE
                                ) d
                                    left join ods.ods_pl_powerdes_bis_project_dt bis_project
                                              on d.bis_project_id = bis_project.bis_project_id
                       ) t on table1.BIS_PROJECT_ID = t.BIS_PROJECT_ID and table1.BIS_CONT_ID = t.BIS_CONT_ID
              ) t2 on t1.bis_project_id = t2.bis_project_id and t1.year_month1 = t2.must_year_month and
                      t1.bis_cont_id = t2.bis_cont_id and t1.must_type = t2.must_type
     ) t1
     on t.bis_project_id = t1.bis_project_id and t.bis_cont_id = t1.bis_cont_id
         and t.must_type = t1.must_type and t.year_month1 = t1.year_month1
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
where (t.owe_qz is not null or t1.owe_cont is not null);



select distinct bis_project.bis_project_id,
                bis_project.short_name,
                year,
                month,
                table1.NAME_CN,
                table1.bis_shop_id,
                year_month1,
                bis_cont_id,
                store_type,
                t.charge_item_code must_type
from dim.dim_pl_date,
     ods.ods_pl_powerdes_bis_project_dt bis_project,
     (
         -- 合同表
         select BIS_CONT.bis_project_id,
                BIS_CONT.bis_cont_id,
                BIS_CONT.bis_shop_id,
                bis_cont.STORE_TYPE,                           -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                case
                    when bis_cont.bis_shop_name is null then bis_shop.NAME_CN
                    else bis_cont.bis_shop_name end as NAME_CN -- 结果表取合同表中品牌名称，如合同表中品牌名称为空取品牌表中名称，  如全部为空  及为空
         from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                  left join ods.ods_pl_powerdes_bis_shop_dt bis_shop
                            on BIS_CONT.BIS_SHOP_ID = BIS_SHOP.BIS_SHOP_ID
     ) table1,
     (select charge_item_code
      from ods.ods_pl_powerdes_pms_fin_charge_item_dict_dt
      where charge_item_code in ('1', '2')) t
where year between '2010' and '2030'
  and bis_project.is_business_project = '1'
  and oper_status = '2';



select t.bis_project_id,
       t.short_name,
       t.year,
       t.month,
       t.year_month1,
       t.must_type,
       t1.NAME_CN,
       t1.bis_shop_id,
       t1.bis_cont_id,
       t1.store_type
from dwd.dwd_bis_cross_join_project_basic01_result_big_dt t
         join
     dwd.dwd_bis_cross_join_cont_basic01_result_big_dt t1
     on t.num_key = t1.num_key and t1.dt = date_format(current_date(), 'yyyyMMdd')
where t.dt = date_format(current_date(), 'yyyyMMdd');


-- 租金  物管
select t.bis_project_id,
       t.query_date,
       t.fee_type,
       t.store_type,
       t1.fact_money

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
         where dt = date_format(current_date, 'YYYYMMdd')
           and fact_type in ('1', '2')
         group by fact_type,
                  fact_date,
                  store_type,
                  bis_project_id,
                  must_year_month
     ) t1 on t1.store_type = t.store_type
         and t1.fact_type = t.fee_type
         and t1.bis_project_id = t.bis_project_id
         and t1.must_year_month = t.query_date
where t.dt = date_format(current_date, 'YYYYMMdd')
  and substr(t1.fact_date, 1, 10) <= last_day(concat(t.query_date, '-01'))
  and t1.must_year_month <= t.query_date
  and t1.must_year_month >= concat(t.query_year, '-01')
  and t1.must_year_month is not null
  and substr(t1.fact_date, 1, 4) < '2021';



select sum(MONEY),
       sum(case
               when substr(fact.fact_date, 0, 7) <= substr(MUST_RENT_LAST_PAY_DATE, 0, 7)
                   then nvl(fact.money, 0)
               else 0 end)
from ods.ods_pl_powerdes_bis_fact2_dt fact
where substr(MUST_RENT_LAST_PAY_DATE, 0, 7) = '2021-01'
  and FACT_TYPE = '1'
  and bis_project_id = '6D7E1C7AFAFB43E986670A81CF444233'
  AND MUST_RENT_LAST_PAY_DATE is not null
  and IS_DELETE = '1'
  and bis_must_id is not null
group by bis_project_id;


-- 实收
select sum(factMoney)
from (
         SELECT bis_fact2.bis_project_id,
                bis_fact2.bis_cont_id,
                bis_fact2.bis_must_id,
                bis_fact2.fact_type,
                bis_fact2.fact_date,
                bis_fact2.must_rent_last_pay_date,
                sum(nvl(bis_fact2.money, 0)) as factMoney
         FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
         where bis_fact2.is_delete = 1
           and bis_fact2.fact_type in ('1', '2')
           and bis_fact2.bis_must_id is not null
            and bis_fact2.bis_cont_id ='8a7b868b6e41067f016e43d76c0e1af1'
         group by bis_fact2.bis_cont_id, bis_fact2.bis_must_id,
                  bis_fact2.fact_type,
                  bis_fact2.fact_date,
                  bis_fact2.must_rent_last_pay_date,
                  bis_fact2.bis_project_id
     )t

where bis_project_id = '6D7E1C7AFAFB43E986670A81CF444233'
  and substr(MUST_RENT_LAST_PAY_DATE, 0, 7) = '2021-01'
  and FACT_TYPE = '1'
group by bis_project_id;




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
where bis_project_id = '6D7E1C7AFAFB43E986670A81CF444233'
  and substr(MUST_RENT_LAST_PAY_DATE, 0, 7) = '2021-01'
  and FACT_TYPE = '1'
group by t.bis_project_id, t.bis_cont_id,
         substr(t.rent_last_pay_date, 0, 4),
         substr(t.rent_last_pay_date, 0, 7), t.MUST_TYPE





















