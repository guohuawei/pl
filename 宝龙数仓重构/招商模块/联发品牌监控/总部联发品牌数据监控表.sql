-- 商家-新业态
select table1.BIS_SHOP_ID,                                            -- 商家id
       table1.primary_forms,                                          -- 一级业态
       table1.company_name,                                           -- 品牌集团名称
       table1.cooperative_brand_CN,                                   -- 合作品牌中文
       table1.cooperative_brand_EN,                                   -- 合作品牌英文
       table1.business_level,                                         -- 商家级别(0：S；1：A；2：B；3：C；4：D；5：L)
       table1.BRAND_OWNERSHIP,                                        -- 品牌归属（1总部、2区域 3:其他品牌 4:黑名单）
       table1.area_type,                                              -- 品牌所属区域
       table2.project_number,                                         -- 项目合作数量
       table2.PROJECT_NAME,                                           -- 合作项目
       table2.total_rent_area,                                        -- 合作租赁面积
       table4.hap_2018,                                               -- 当前年往前二年的销售额
       table5.hap_2019,                                               -- 当前年往前一年的销售额
       table6.accumulated_turnover_2020,                              -- 当前年销售额
       table9.owe_qz,                                                 -- 权责欠费
       table9.owe_cont,                                               -- 合同欠费
       case when table9.owe_qz > 0 then '1' else '2' end as owe_type, -- 欠费情况(1: 欠费 2：不欠费)
       table12.bis_cont_count,                                        -- 当月新增合同数
       table13.three_month_expire_cont_number,                        -- 三个月内到期合同数（铺位考核口径)
       table14.hap_2017                                               -- 当前年往前三年的销售额

from (
         select BIS_SHOP.BIS_SHOP_ID,                               --商家id
                max(case
                        when BIS_SHOP_SORT_NEW.SORT_TYPE = 1 then BIS_SHOP_SORT_NEW.SORT_NAME
                        else null end) as     primary_forms,        -- 一级业态
                max(BIS_SHOP.COMPANY_NAME)    company_name,         -- 品牌集团名称
                max(BIS_SHOP.NAME_CN)         cooperative_brand_CN, -- 合作品牌中文
                max(BIS_SHOP.name_en)         cooperative_brand_EN, -- 合作品牌英文
                max(BIS_SHOP.SHOP_LEVEL)      business_level,       -- 商家级别
                max(BIS_SHOP.BRAND_OWNERSHIP) BRAND_OWNERSHIP,      -- 品牌归属（1:总部、2区域 3:其他品牌 4:黑名单）
                max(area_type)                area_type             -- 品牌所属区域

         from ods.ods_pl_powerdes_bis_shop_dt BIS_SHOP
                  left join ods.ods_pl_powerdes_bis_shop_sort_new_rel_dt BIS_SHOP_SORT_NEW_REL
                            on BIS_SHOP.BIS_SHOP_ID = BIS_SHOP_SORT_NEW_REL.BIS_SHOP_ID
                  left join ods.ods_pl_powerdes_bis_shop_sort_new_dt BIS_SHOP_SORT_NEW
                            on BIS_SHOP_SORT_NEW_REL.BIS_SHOP_SORT_ID = BIS_SHOP_SORT_NEW.BIS_SHOP_SORT_ID
         where BIS_SHOP.delete_bl <> '1'
           and IS_NEW = '1'
           and IS_NEW_SHOP = '1'
           and BIS_SHOP.BRAND_OWNERSHIP in ('1', '2')
         group by BIS_SHOP.BIS_SHOP_ID
     ) table1
         left join
     (
         SELECT COUNT(distinct BIS_CONT.BIS_PROJECT_ID)             project_number,
                BIS_SHOP.BIS_SHOP_ID,
                concat_ws(",", collect_set(bis_project.short_name)) PROJECT_NAME,
                sum(BIS_CONT.RENT_SQUARE) as                        total_rent_area
         FROM ods.ods_pl_powerdes_bis_shop_dt BIS_SHOP
                  LEFT JOIN ods.ods_pl_powerdes_bis_cont_dt BIS_CONT
                            ON BIS_SHOP.BIS_SHOP_ID = BIS_CONT.BIS_SHOP_ID AND bis_cont.effect_flg = 'Y'
                  left join ods.ods_pl_powerdes_bis_project_dt bis_project
                            on BIS_CONT.BIS_PROJECT_ID = BIS_PROJECT.BIS_PROJECT_ID
         where bis_shop.delete_bl <> '1'
           and IS_NEW = '1'
           and BIS_SHOP.IS_NEW_SHOP = '1'
         GROUP BY BIS_SHOP.BIS_SHOP_ID
     ) table2 on table1.BIS_SHOP_ID = table2.BIS_SHOP_ID
         LEFT JOIN
     (
         select SUM(SALES_MONEY) hap_2017,
                bis_shop_id
         from dwd.dwd_bis_sales_day_big_dt BIS_SALES_DAY
         where substr(SALES_DATE, 0, 4) = year(add_months(current_date, -12 * 3))
           and dt = date_format(current_date, 'yyyyMMdd')
           and effect_flg = 'Y'
         GROUP BY bis_shop_id, substr(SALES_DATE, 0, 4)
     ) table14 on table1.bis_shop_id = table14.bis_shop_id
         LEFT JOIN
     (
         select SUM(SALES_MONEY) hap_2018,
                bis_shop_id
         from dwd.dwd_bis_sales_day_big_dt BIS_SALES_DAY
         where substr(SALES_DATE, 0, 4) = year(add_months(current_date, -12 * 2))
           and dt = date_format(current_date, 'yyyyMMdd')
           and effect_flg = 'Y'
         GROUP BY bis_shop_id, substr(SALES_DATE, 0, 4)
     ) table4 on table1.bis_shop_id = table4.bis_shop_id
         LEFT JOIN
     (
         select SUM(SALES_MONEY) hap_2019,
                bis_shop_id
         from dwd.dwd_bis_sales_day_big_dt BIS_SALES_DAY
         where substr(SALES_DATE, 0, 4) = year(add_months(current_date, -12 * 1))
           and dt = date_format(current_date, 'yyyyMMdd')
           and effect_flg = 'Y'
         GROUP BY bis_shop_id, substr(SALES_DATE, 0, 4)
     ) table5 on table1.bis_shop_id = table5.bis_shop_id
         LEFT JOIN
     (
         select SUM(SALES_MONEY) accumulated_turnover_2020,
                bis_shop_id
         from dwd.dwd_bis_sales_day_big_dt BIS_SALES_DAY
         where substr(SALES_DATE, 0, 4) = year(current_date)
           and dt = date_format(current_date, 'yyyyMMdd')
           and effect_flg = 'Y'
         GROUP BY bis_shop_id, substr(SALES_DATE, 0, 4)
     ) table6 on table1.bis_shop_id = table6.bis_shop_id
         left join
     (
         select bis_shop_id,                                                                               -- 品牌id
                round(sum(nvl(owe_qz, 0)), 2)                                                     owe_qz,  -- 权责欠费
                round(sum(case when query_date >= '2021-01' then nvl(owe_cont, 0) else 0 end), 2) owe_cont -- 合同欠费
         from dws.dws_bis_rent_mgr_arrearage_big_dt
         where dt = date_format(current_date, 'yyyyMMdd')
           and query_date <= substr(current_date, 0, 7)
           AND fee_type IN ('1', '2')
           AND store_type IN ('1', '2')
           AND effect_flg = 'Y'
         group by bis_shop_id
     ) table9 on table1.bis_shop_id = table9.bis_shop_id
         left join
     (
         select count(bis_cont_id) bis_cont_count, -- 当月新增合同数
                bis_shop_id
         from ods.ods_pl_powerdes_bis_cont_dt bis_cont
         where effect_flg = 'Y'
           AND substr(cont_start_date, 0, 7) = date_format(current_date(), 'yyyy-MM')
         group by bis_shop_id
     ) table12 on table1.bis_shop_id = table12.bis_shop_id
         left join
     (
         -- 考核铺位
         SELECT count(BIS_CONT.BIS_CONT_ID) three_month_expire_cont_number, -- 三个月内到期合同数（铺位考核口径)
                bis_cont.bis_shop_id
         FROM ods.ods_pl_powerdes_bis_store_dt bis_store
                  inner join
              (
                  -- 铺位打散
                  SELECT tmp.bis_cont_id,
                         bis_store_id,
                         bis_project_id,
                         bis_shop_id
                  FROM (
                           SELECT bis_cont_id,
                                  bis_store_ids,
                                  bis_project_id,
                                  bis_shop_id
                           FROM ods.ods_pl_powerdes_bis_cont_dt
                           where effect_flg = 'Y'
                             and substr(cont_end_date, 0, 10) BETWEEN current_date AND add_months(current_date, 3)
                       ) tmp LATERAL VIEW explode(split(bis_store_ids, ',')) tmp1 AS bis_store_id
              ) bis_cont on BIS_STORE.BIS_STORE_ID = BIS_CONT.BIS_STORE_ID
         where bis_store.is_delete = 'N'
           and bis_store.IS_ASSESS = 'Y' -- 考核商铺
           and bis_store.status_cd = '1' -- 有效铺位,
         group by bis_cont.bis_shop_id
     ) table13 on table1.bis_shop_id = table13.bis_shop_id;










