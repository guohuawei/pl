select BIS_PROJECT.BIS_PROJECT_ID,                                                                              -- 项目id
       BIS_PROJECT.PROJECT_NAME,                                                                                -- 项目名称
       substr(BIS_PROJECT.OPEN_DATE, 0, 4),                                                                     -- 开业时间
       table1.y_rent_area,                                                                                      --计租面积(实际）
       case when table2.t_rent_area = 0 then 0 else round(table1.y_rent_area / table2.t_rent_area, 2) end,      -- 面积占比
       table1.y_brand_count,                                                                                    -- 品牌数（实际）
       case when table2.t_brand_count = 0 then 0 else round(table1.y_brand_count / table2.t_brand_count, 2) end -- 品牌占比
from ods.ods_pl_powerdes_bis_project_dt BIS_PROJECT
         left join
     (
         select BIS_PROJECT_ID,
                sum(RENT_SQUARE)                     as y_rent_area,  -- 当前项目有效合同计租面积
                count(distinct BIS_SHOP.BIS_SHOP_ID) as y_brand_count -- 当前有效合同品牌数
         from ods.ods_pl_powerdes_bis_cont_dt BIS_CONT
                  left join ods.ods_pl_powerdes_bis_shop_dt BIS_SHOP on BIS_CONT.BIS_SHOP_ID = BIS_SHOP.BIS_SHOP_ID
         where BIS_CONT.EFFECT_FLG = 'Y'
         group by BIS_PROJECT_ID
     ) table1 on BIS_PROJECT.BIS_PROJECT_ID = table1.BIS_PROJECT_ID
         left join
     (
         select BIS_PROJECT_ID,
                sum(RENT_SQUARE)                     as t_rent_area,  -- 项目总计租面积
                count(distinct BIS_SHOP.BIS_SHOP_ID) as t_brand_count -- 系统所有品牌数
         from ods.ods_pl_powerdes_bis_cont_dt BIS_CONT
                  left join ods.ods_pl_powerdes_bis_shop_dt BIS_SHOP on BIS_CONT.BIS_SHOP_ID = BIS_SHOP.BIS_SHOP_ID
         where BIS_CONT.EFFECT_FLG <> 'D'
         group by BIS_PROJECT_ID
     ) table2 on BIS_PROJECT.BIS_PROJECT_ID = table2.BIS_PROJECT_ID;






