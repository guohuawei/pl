with tmp1 as (
    select a1.bis_shop_id,
           a1.bis_project_id,
           a2.project_name,
           min(a1.cont_start_date) start_date
    from ods.ods_pl_powerdes_bis_cont_dt a1
             left join ods.ods_pl_powerdes_bis_project_dt a2 on a1.bis_project_id = a2.bis_project_id
    where a1.status_cd <> '0'
      and a1.status_cd is not null
    group by a1.bis_shop_id,
             a1.bis_project_id,
             a2.project_name
),
     tmp2 as (
         select t.BIS_SHOP_ID,
                t.BIS_PROJECT_ID,
                max(t.current_mgr_money)  current_mgr_money,  -- 当物管收入
                max(t.current_rent_money) current_rent_money, -- 当月租金收入
                max(t.last_mgr_money)     last_mgr_money,     -- 上月物管收入
                max(t.last_rent_money)    last_rent_money     -- 上月租金收入
         from (
                  select a4.bis_shop_id,
                         a4.bis_project_id,
                         case
                             when a3.fact_type = '1' and a3.QZ_YEAR_MONTH = substr(current_date(), 1, 7)
                                 then sum(a3.money)
                             else null end as current_rent_money, -- 当月租金收入
                         case
                             when a3.fact_type = '2' and a3.QZ_YEAR_MONTH = substr(current_date(), 1, 7)
                                 then sum(a3.money)
                             else null end as current_mgr_money,  -- 当月物管收入
                         case
                             when a3.fact_type = '1' and a3.QZ_YEAR_MONTH = substr(add_months(current_date(), -1), 0, 7)
                                 then sum(a3.money)
                             else null end as last_rent_money,    -- 上一个月租金收入
                         case
                             when a3.fact_type = '2' and a3.QZ_YEAR_MONTH = substr(add_months(current_date(), -1), 0, 7)
                                 then sum(a3.money)
                             else null end as last_mgr_money      -- 上一个月物管收入
                  from ods.ods_pl_powerdes_bis_fact2_dt a3
                           left join ods.ods_pl_powerdes_bis_cont_dt a4
                                     on a3.bis_cont_id = a4.bis_cont_id and a4.EFFECT_FLG <> 'D'
                  where a3.IS_DELETE <> '0'
                    and a3.FACT_TYPE in ('1', '2')
                    and a3.QZ_YEAR_MONTH in (substr(current_date(), 1, 7), substr(add_months(current_date(), -1), 0, 7))
                  group by a4.bis_shop_id,
                           a4.bis_project_id,
                           a3.fact_type,
                           a3.qz_year_month
              ) t
         group by t.BIS_PROJECT_ID, t.BIS_SHOP_ID
     ),
     tmp3 as (
         select t.BIS_PROJECT_ID,
                t.BIS_SHOP_ID,
                max(current_sales) current_sales, -- 当月销售额
                max(last_sales)    last_sales     -- 上月月销售额
         from (
                  select bis_shop_id,
                         bis_project_id,
                         case
                             when substr(sales_date, 1, 7) = substr(current_date(), 1, 7) then sum(sales_money)
                             else null end as current_sales, -- 当月销售额
                         case
                             when substr(sales_date, 1, 7) = substr(add_months(current_date(), -1), 0, 7)
                                 then sum(sales_money)
                             else null end as last_sales     -- 上月月销售额
                  from dwd.dwd_bis_sales_day_big_dt a5
                  where substr(sales_date, 1, 7) in
                        (substr(current_date(), 1, 7), substr(add_months(current_date(), -1), 0, 7))
                    and dt = date_format(current_date, 'yyyyMMdd')
                  group by bis_shop_id,
                           bis_project_id,
                           substr(sales_date, 1, 7)
              ) t
         group by BIS_PROJECT_ID, BIS_SHOP_ID
     ),
     tmp4 as (
         select bis_shop_id,
                bis_project_id,
                sum(rent_square) rent_square
         from ods.ods_pl_powerdes_bis_cont_dt a6
         where substr(current_date(), 1, 7) between substr(CONT_START_DATE, 1, 7) and substr(nvl(CONT_TO_FAIL_DATE, CONT_END_DATE), 1, 7)
           and a6.EFFECT_FLG <> '0'
         group by bis_shop_id,
                  bis_project_id
     ),
     tmp5 as (
         select a8.bis_shop_id,
                a8.bis_project_id,
                sum(OWN_MONEY) total_owe
         from ods.ods_pl_powerdes_pms_fin_arrearage_dt a7
                  left join ods.ods_pl_powerdes_bis_cont_dt a8
                            on a7.bis_cont_id = a8.bis_cont_id and a8.EFFECT_FLG <> 'D'
         where a7.is_del <> '1'
           and a7.finance_period <= substr(current_date(), 1, 7)
         group by a8.bis_shop_id,
                  a8.bis_project_id
     )

insert
OVERWRITE
table
dws.dws_bis_merchant_cooperation_project_big_dt
partition
(
dt = '${hiveconf:nowdate}'
)
select tmp1.bis_shop_id,                                                                        -- 商家id
       tmp1.project_name,                                                                       -- 项目名称
       tmp1.start_date,                                                                         -- 起始时间
       round(nvl(tmp2.current_rent_money, 0), 2)                            current_rent_money, -- 当月租金收入
       round(nvl(tmp2.current_mgr_money, 0), 2)                             current_mgr_money,  -- 当月物管收入
       round(nvl(tmp2.last_rent_money, 0), 2)                               last_rent_money,    -- 上月租金收入
       round(nvl(tmp2.last_mgr_money, 0), 2)                                last_mgr_money,     -- 上月物管收入
       round(nvl(tmp3.current_sales, 0), 2)                                 current_sales,      -- 当月销售额
       round(nvl(tmp3.last_sales, 0), 2)                                    last_sales,         -- 上月销售额
       tmp4.rent_square,                                                                        -- 面积
       round(case
                 when nvl(tmp4.rent_square, 0) = 0 then 0
                 else nvl(tmp3.current_sales, 0) / tmp4.rent_square end, 2) months_effect,      -- 月坪效
       round(nvl(tmp5.total_owe, 0), 2)                                     total_owe           -- 总欠费
from tmp1
         left join tmp2 on tmp1.bis_shop_id = tmp2.bis_shop_id and tmp1.bis_project_id = tmp2.bis_project_id
         left join tmp3 on tmp1.bis_shop_id = tmp3.bis_shop_id and tmp1.bis_project_id = tmp3.bis_project_id
         left join tmp4 on tmp1.bis_shop_id = tmp4.bis_shop_id and tmp1.bis_project_id = tmp4.bis_project_id
         left join tmp5 on tmp1.bis_shop_id = tmp5.bis_shop_id and tmp1.bis_project_id = tmp5.bis_project_id
;
