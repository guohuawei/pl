select BIS_PROJECT.BIS_PROJECT_ID, -- 项目id
       BIS_PROJECT.PROJECT_NAME,   -- 项目名称
       table1.CHIEF_NAME,          -- 主谈人
       null as rent_area_t,        -- 计租面积（目标）
       table1.y_rent_area,         -- 计租面积(实际）
       null as percentage_complete -- 完成率
from ods.ods_pl_powerdes_bis_project_dt BIS_PROJECT
         left join
     (
         select BIS_CONT.BIS_PROJECT_ID,
                sum(BIS_CONT.RENT_SQUARE) as y_rent_area, -- 当前项目有效合同计租面积
                CHIEF_NAME                               -- 主谈人
         from ods.ods_pl_powerdes_bis_cont_dt BIS_CONT
                  inner join ods.ods_pl_powerdes_commerce_investment_dt COMMERCE_INVESTMENT
                             on BIS_CONT.CONT_NO = COMMERCE_INVESTMENT.CONTRACT_NO
         where BIS_CONT.EFFECT_FLG = 'Y'
         group by BIS_PROJECT_ID, COMMERCE_INVESTMENT.CHIEF_NAME
     ) table1 on BIS_PROJECT.BIS_PROJECT_ID = table1.BIS_PROJECT_ID;