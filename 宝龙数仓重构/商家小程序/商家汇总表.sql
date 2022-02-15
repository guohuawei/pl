with tmp1 as (
    select a1.BIS_SHOP_ID,
           a1.NAME_CN merchants_chinese_name,
           a2.query_date
    from ods.ods_pl_powerdes_bis_shop_dt a1,
         (select concat(substr(month_id, 1, 4), '-', substr(month_id, 5, 2)) query_date
          from dim.dim_date
          where month_id >= '2010-01'
          group by concat(substr(month_id, 1, 4), '-', substr(month_id, 5, 2))) a2
    where a1.DELETE_BL <> '1'),
     tmp2 as (
         select a4.bis_shop_id,
                a3.MUST_YEAR_MONTH                                      query_date,
                sum(case when a3.must_type = '1' then MONEY else 0 end) rent_money,
                sum(case when a3.must_type = '2' then MONEY else 0 end) management_money
         from ods.ods_pl_powerdes_bis_must2_dt a3
                  left join ods.ods_pl_powerdes_bis_cont_dt a4
                            on a3.bis_cont_id = a4.bis_cont_id and a4.EFFECT_FLG <> 'D'
         where a3.IS_DELETE <> '1'
         group by a4.bis_shop_id,
                  a3.MUST_YEAR_MONTH
     ),
     tmp3 as (
         select bis_shop_id,
                substr(sales_date, 1, 7) query_date,
                sum(sales_money)         sales
         from dwd.dwd_bis_sales_day_big_dt a5
         where dt = date_format(current_date, 'yyyyMMdd')
         group by bis_shop_id,
                  substr(sales_date, 1, 7)
     ),
     tmp4 as (
         select a7.bis_shop_id,
                a6.finance_period query_date,
                sum(OWN_MONEY)    total_owe
         from ods.ods_pl_powerdes_pms_fin_arrearage_dt a6
                  left join ods.ods_pl_powerdes_bis_cont_dt a7
                            on a6.bis_cont_id = a7.bis_cont_id and a7.EFFECT_FLG <> 'D'
         where trim(a6.is_del) <> '1'
         group by a7.bis_shop_id,
                  a6.finance_period
     ),
     tmp5 as (
         select a8.bis_shop_id,
                a8.query_date,
                sum(RENT_SQUARE) RENT_SQUARE
         from tmp1 a8
                  left join ods.ods_pl_powerdes_bis_cont_dt a9
                            on a8.bis_shop_id = a9.bis_shop_id and a9.EFFECT_FLG <> 'D'
         where (a8.query_date between substr(a9.CONT_START_DATE, 1, 7)
             and substr(nvl(a9.CONT_TO_FAIL_DATE, a9.CONT_END_DATE), 1, 7))
            or a9.bis_shop_id is not null
         group by a8.bis_shop_id,
                  a8.query_date
     ),
     tmp6 as (
         select bis_shop_id,
                substr(sales_date, 1, 7) query_date,
                sum(sales_money)         sales
         from dwd.dwd_bis_sales_day_big_dt a5
         where dt = date_format(current_date, 'yyyyMMdd')
         group by bis_shop_id,
                  substr(sales_date, 1, 7)
     )
insert
OVERWRITE
table
dws.dws_bis_merchant_summary_big_dt
partition
(
dt = '${hiveconf:nowdate}'
)
select tmp1.bis_shop_id,                                                 -- 品牌id
       tmp1.merchants_chinese_name,                                      -- 商家中文名
       tmp1.query_date,                                                  -- 月份
       round(nvl(tmp2.rent_money, 0), 2)               rent_money,       -- 月租金
       round(nvl(tmp2.management_money, 0), 2)         management_money, -- 月物管
       round(nvl(tmp6.sales, 0), 2)                    sales,            -- 上月销售额
       round(nvl(tmp3.sales / tmp5.RENT_SQUARE, 0), 2) months_effect,    -- 月坪效
       round(nvl(tmp4.total_owe, 0), 2)                total_owe,        -- 总欠费
       round(nvl(tmp2.rent_money / tmp3.sales, 0), 2)  rent_ratio        -- 租售比
from tmp1
         left join tmp2 on tmp1.BIS_SHOP_ID = tmp2.BIS_SHOP_ID and tmp1.query_date = tmp2.query_date
         left join tmp3 on tmp1.BIS_SHOP_ID = tmp3.BIS_SHOP_ID and tmp1.query_date = tmp3.query_date
         left join tmp4 on tmp1.BIS_SHOP_ID = tmp4.BIS_SHOP_ID and tmp1.query_date = tmp4.query_date
         left join tmp5 on tmp1.BIS_SHOP_ID = tmp5.BIS_SHOP_ID and tmp1.query_date = tmp5.query_date
         left join tmp6 on tmp1.BIS_SHOP_ID = tmp6.BIS_SHOP_ID and tmp1.query_date = substr(
        add_months(from_unixtime(unix_timestamp(tmp6.query_date, 'yyyy-MM'), 'yyyy-MM-dd'), 1), 1, 7)
;

