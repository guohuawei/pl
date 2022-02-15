select table1.bis_shop_id,                                            -- 商家id
       table1.primary_forms,                                          -- 一级业态
       table1.cooperative_brand,                                      -- 品牌名称
       BIS_PROJECT.short_name,                                        -- 项目名称
       BIS_CONT.BIS_STORE_NOS,                                        -- 铺位号
       BIS_CONT.RENT_SQUARE,                                          -- 租赁面积
       BIS_CONT.SIGN_DATE,                                            -- 签约日
       BIS_CONT.cont_end_date,                                        -- 到期日
       table3.free_rent_period,                                       -- 免租期
       table7.rent_price,                                             -- 租金单价
       table8.mgr_price,                                              -- 物管费单价
       table4.hap_2018,                                               -- 当前年往前二年的销售额
       table5.hap_2019,                                               -- 当前年往前一年的销售额
       table6.accumulated_turnover_2020,                              -- 当前年销售额
       table9.owe_qz,                                                 -- 权责欠费
       table9.owe_cont,                                               -- 合同欠费
       case when table9.owe_qz > 0 then '1' else '2' end as owe_type, -- 欠费情况(1: 欠费 2：不欠费)
       table10.hap_2017                                               -- 当前年往前三年的销售额
from (
         -- 新业态
         select BIS_SHOP.BIS_SHOP_ID,                       -- 商家id
                max(case
                        when BIS_SHOP_SORT_NEW.SORT_TYPE = 1 then BIS_SHOP_SORT_NEW.SORT_NAME
                        else null end) as primary_forms,    -- 一级业态
                max(BIS_SHOP.NAME_CN)     cooperative_brand -- 品牌名称
         from ods.ods_pl_powerdes_bis_shop_dt BIS_SHOP
                  left join ods.ods_pl_powerdes_bis_shop_sort_new_rel_dt BIS_SHOP_SORT_NEW_REL
                            on BIS_SHOP.BIS_SHOP_ID = BIS_SHOP_SORT_NEW_REL.BIS_SHOP_ID
                  left join ods.ods_pl_powerdes_bis_shop_sort_new_dt BIS_SHOP_SORT_NEW
                            on BIS_SHOP_SORT_NEW_REL.BIS_SHOP_SORT_ID = BIS_SHOP_SORT_NEW.BIS_SHOP_SORT_ID
         where BIS_SHOP.delete_bl <> '1'
           and BIS_SHOP.IS_NEW = '1'
           and BIS_SHOP.IS_NEW_SHOP = '1'
         group by BIS_SHOP.BIS_SHOP_ID
     ) table1
         LEFT JOIN ods.ods_pl_powerdes_bis_cont_dt BIS_CONT
                   ON table1.BIS_SHOP_ID = BIS_CONT.BIS_SHOP_ID and BIS_CONT.EFFECT_FLG = 'Y'
         left join
     (
         select bis_cont.bis_cont_id, -- 合同id
                t.rent_price          -- 租金单价
         from (
                  -- 固定租金
                  select t1.res_approve_info_id,
                         concat_ws(",", collect_list(cast(split(RENT_TYPE_1, '-')[0] as string))) rent_price -- 每期单价
                  from ods.ods_pl_powerdes_res_approve_rent_info_dt t1
                           LATERAL VIEW explode(split(t1.PRICE_ACTUAL_INFO, '\\|\\|')) cfi as RENT_TYPE_1
                  where RENT_TYPE = 1
                  group by t1.res_approve_info_id

                  union all

                  -- 抽成：固定抽成
                  select t1.res_approve_info_id,
                         concat_ws(",", collect_list(cast(split(RENT_TYPE_1, '-')[1] as string))) rent_price -- 每期扣率
                  from ods.ods_pl_powerdes_res_approve_rent_info_dt t1
                           LATERAL VIEW explode(split(t1.PRICE_ACTUAL_INFO, '\\|\\|')) cfi as RENT_TYPE_1
                  where t1.RENT_TYPE = 2
                    and nvl(t1.pumping_type, 1000) = 1001
                  group by t1.res_approve_info_id


                  union all
                  -- 抽成：阶梯抽成


                  -- 两者取高 （旧数据）
                  select t1.res_approve_info_id,
                         concat_ws(",", collect_list(cast(CONCAT_WS('/', split(RENT_TYPE_1, '-')[0],
                                                                    concat(split(RENT_TYPE_1, '-')[1], '%')) as string))) rent_price --每期 单价/扣率
                  from ods.ods_pl_powerdes_res_approve_rent_info_dt t1
                           LATERAL VIEW explode(split(t1.PRICE_ACTUAL_INFO, '\\|\\|')) cfi as RENT_TYPE_1
                  where t1.RENT_TYPE = 3
                  group by t1.res_approve_info_id

                  -- 两者取高 （新数据）
              ) t
                  LEFT JOIN ods.ods_pl_powerdes_bis_cont_dt bis_cont
                            ON bis_cont.res_approve_id1 = t.res_approve_info_id and bis_cont.effect_flg = 'Y'
     ) table7 on BIS_CONT.bis_cont_id = table7.bis_cont_id
         left join
     (
         select bis_cont.bis_cont_id, -- 合同id
                t.mgr_price           -- 物管费单价
         FROM (
                  select t1.res_approve_info_id,
                         concat_ws(",", collect_list(cast(split(RENT_TYPE_1, '-')[0] as string))) mgr_price -- 每期扣率
                  from ods.ods_pl_powerdes_res_approve_rent_info_dt t1
                           LATERAL VIEW explode(split(t1.PROP_PRICE_ACTUAL, '\\|\\|')) cfi as RENT_TYPE_1
                  group by t1.res_approve_info_id
              ) t
                  LEFT JOIN ods.ods_pl_powerdes_bis_cont_dt bis_cont
                            ON bis_cont.res_approve_id1 = t.res_approve_info_id and bis_cont.effect_flg = 'Y'
     ) table8 on BIS_CONT.bis_cont_id = table8.bis_cont_id
         left join ods.ods_pl_powerdes_bis_project_dt bis_project
                   on BIS_CONT.BIS_PROJECT_ID = BIS_PROJECT.BIS_PROJECT_ID
         left join
     (
         select BIS_CONT_ID,
                sum(datediff(end_date, start_date)) free_rent_period
         from ods.ods_pl_powerdes_bis_cont_free_day_dt BIS_CONT_FREE_DAY
         where (START_DATE is not null and END_DATE is not null)
         group by BIS_CONT_FREE_DAY.bis_cont_id
     ) table3 on BIS_CONT.BIS_CONT_ID = table3.BIS_CONT_ID
         LEFT JOIN
     (
         select SUM(SALES_MONEY) hap_2017,
                BIS_CONT_ID
         from dwd.dwd_bis_sales_day_big_dt BIS_SALES_DAY
         where substr(SALES_DATE, 0, 4) = year(add_months(current_date, -12 * 3))
           and dt = date_format(current_date, 'yyyyMMdd')
           and effect_flg = 'Y'
         GROUP BY BIS_CONT_ID, substr(SALES_DATE, 0, 4)
     ) table10 on BIS_CONT.BIS_CONT_ID = table10.BIS_CONT_ID
         LEFT JOIN
     (
         select SUM(SALES_MONEY) hap_2018,
                BIS_CONT_ID
         from dwd.dwd_bis_sales_day_big_dt BIS_SALES_DAY
         where substr(SALES_DATE, 0, 4) = year(add_months(current_date, -12 * 2))
           and dt = date_format(current_date, 'yyyyMMdd')
           and effect_flg = 'Y'
         GROUP BY BIS_CONT_ID, substr(SALES_DATE, 0, 4)
     ) table4 on BIS_CONT.BIS_CONT_ID = table4.BIS_CONT_ID
         LEFT JOIN
     (
         select SUM(SALES_MONEY) hap_2019,
                BIS_CONT_ID
         from dwd.dwd_bis_sales_day_big_dt BIS_SALES_DAY
         where substr(SALES_DATE, 0, 4) = year(add_months(current_date, -12 * 1))
           and dt = date_format(current_date, 'yyyyMMdd')
           and effect_flg = 'Y'
         GROUP BY BIS_CONT_ID, substr(SALES_DATE, 0, 4)
     ) table5 on BIS_CONT.BIS_CONT_ID = table5.BIS_CONT_ID
         LEFT JOIN
     (
         select SUM(SALES_MONEY) accumulated_turnover_2020,
                BIS_CONT_ID
         from dwd.dwd_bis_sales_day_big_dt BIS_SALES_DAY
         where substr(SALES_DATE, 0, 4) = year(current_date)
           and dt = date_format(current_date, 'yyyyMMdd')
           and effect_flg = 'Y'
         GROUP BY BIS_CONT_ID, substr(SALES_DATE, 0, 4)
     ) table6 on BIS_CONT.BIS_CONT_ID = table6.BIS_CONT_ID
         left join
     (
         select bis_project_id,                                                                               -- 项目id
                bis_cont_id,                                                                                  -- 合同id
                round(sum(nvl(owe_qz, 0)), 2)                                                        owe_qz,  -- 权责欠费
                sum(case when query_date >= '2021-01' then round(nvl(owe_cont, 0), 2) else 0 end) as owe_cont -- 合同欠费
         from dws.dws_bis_rent_mgr_arrearage_big_dt
         where dt = date_format(current_date, 'yyyyMMdd')
           and query_date <= substr(current_date, 0, 7)
           AND fee_type IN ('1', '2')
           AND store_type IN ('1', '2')
           AND effect_flg = 'Y'
         group by bis_project_id, bis_cont_id
     ) table9 on bis_project.bis_project_id = table9.bis_project_id and BIS_CONT.bis_cont_id = table9.bis_cont_id;













