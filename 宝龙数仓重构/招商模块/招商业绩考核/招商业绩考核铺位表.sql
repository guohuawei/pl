select BIS_PROJECT.BIS_PROJECT_ID, -- 项目id
       BIS_PROJECT.PROJECT_NAME,   -- 项目名称
       table1.CHIEF_NAME,          -- 主谈人
       table1.STORE_NO,            -- 铺位号
       table2.primary_forms,       -- 一级液态
       table1.RENT_SQUARE          -- 铺位计租面积

from ods.ods_pl_powerdes_bis_project_dt BIS_PROJECT
         left join
     (
         select t.BIS_PROJECT_ID,
                BIS_STORE.STORE_NO,
                COMMERCE_INVESTMENT.CHIEF_NAME,
                bis_store.RENT_SQUARE,
                t.BIS_CONT_ID,
                t.BIS_SHOP_ID
         from (
                  -- 铺位打散
                  SELECT tmp.bis_cont_id,
                         tmp.bis_store_ids,
                         tmp.bis_project_id,
                         tmp.bis_shop_id,
                         bis_store_id,
                         tmp.cont_no
                  FROM (
                           SELECT BIS_PROJECT_ID,
                                  BIS_CONT_ID,
                                  CONT_NO,
                                  bis_store_ids,
                                  bis_shop_id
                           FROM ods.ods_pl_powerdes_bis_cont_dt
                           where effect_flg = 'Y'
                       ) tmp LATERAL VIEW explode(split(bis_store_ids, ',')) tmp1 AS bis_store_id
              ) t
                  inner join ods.ods_pl_powerdes_commerce_investment_dt COMMERCE_INVESTMENT
                             on t.CONT_NO = COMMERCE_INVESTMENT.CONTRACT_NO
                  left join ods.ods_pl_powerdes_bis_store_dt bis_store on t.bis_store_id = BIS_STORE.BIS_STORE_ID
     ) table1 on BIS_PROJECT.BIS_PROJECT_ID = table1.BIS_PROJECT_ID
         left join
     (
         -- 新业态
         select BIS_SHOP.BIS_SHOP_ID,                   -- 商家id
                max(case
                        when BIS_SHOP_SORT_NEW.SORT_TYPE = 1 then BIS_SHOP_SORT_NEW.SORT_NAME
                        else null end) as primary_forms -- 一级业态
         from ods.ods_pl_powerdes_bis_shop_dt BIS_SHOP
                  left join ods.ods_pl_powerdes_bis_shop_sort_rel_dt BIS_SHOP_SORT_NEW_REL
                            on BIS_SHOP.BIS_SHOP_ID = BIS_SHOP_SORT_NEW_REL.BIS_SHOP_ID
                  left join ods.ods_pl_powerdes_bis_shop_sort_new_dt BIS_SHOP_SORT_NEW
                            on BIS_SHOP_SORT_NEW_REL.BIS_SHOP_SORT_ID = BIS_SHOP_SORT_NEW.BIS_SHOP_SORT_ID
         where BIS_SHOP.delete_bl <> '1'
           and BIS_SHOP.IS_NEW = '1'
         group by BIS_SHOP.BIS_SHOP_ID

         union all
         -- 老业态
         select BIS_SHOP.BIS_SHOP_ID,                   --商家id
                max(case
                        when BIS_SHOP_SORT.SORT_TYPE = 1 then BIS_SHOP_SORT.SORT_NAME
                        else null end) as primary_forms -- 一级业态

         from ods.ods_pl_powerdes_bis_shop_dt BIS_SHOP
                  left join ods.ods_pl_powerdes_bis_shop_sort_rel_dt BIS_SHOP_SORT_REL
                            on BIS_SHOP.BIS_SHOP_ID = BIS_SHOP_SORT_REL.BIS_SHOP_ID
                  left join ods.ods_pl_powerdes_bis_shop_sort_dt BIS_SHOP_SORT
                            on BIS_SHOP_SORT.BIS_SHOP_SORT_ID = BIS_SHOP_SORT_REL.BIS_SHOP_SORT_ID
         where BIS_SHOP.delete_bl <> '1'
           and (BIS_SHOP.IS_NEW is null or BIS_SHOP.IS_NEW = '0')
           and BIS_SHOP_SORT.SORT_TYPE <> '0'
           and BIS_SHOP_SORT.SORT_TYPE is not null
         group by BIS_SHOP.BIS_SHOP_ID
     ) table2 ON table2.BIS_SHOP_ID = table1.BIS_SHOP_ID;
