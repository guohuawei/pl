select *
from (
         select max(bis_shop.bis_shop_id),                                                                     --  bis_shop_id,
                max(bis_shop.name_cn),                                                                         --  '商家中文名称',
                max(bis_shop.name_en),                                                                         --  '商家英文名称',
                max(case
                        when bis_shop_sort_new.sort_type = 3 then bis_shop_sort_new.bis_shop_sort_id
                        else null end),                                                                        -- '三级业态id',
                max(case when bis_shop_sort_new.sort_type = 1 then bis_shop_sort_new.sort_name else null end), -- 一级业态,
                max(case when bis_shop_sort_new.sort_type = 2 then bis_shop_sort_new.sort_name else null end), -- 二级业态,
                max(case when bis_shop_sort_new.sort_type = 3 then bis_shop_sort_new.sort_name else null end), -- 三级业态,
                max(case
                        when BIS_SHOP.SHOP_TYPE_CD = '1' and (IS_NEW = '0' or IS_NEW is null) then '主力店-百货'
                        when BIS_SHOP.SHOP_TYPE_CD = '2' and (IS_NEW = '0' or IS_NEW is null) then '主力店-超市'
                        when BIS_SHOP.SHOP_TYPE_CD = '3' and (IS_NEW = '0' or IS_NEW is null) then '主力店-影院'
                        when BIS_SHOP.SHOP_TYPE_CD = '4' and (IS_NEW = '0' or IS_NEW is null) then '次主力店'
                        when BIS_SHOP.SHOP_TYPE_CD = '5' and (IS_NEW = '0' or IS_NEW is null) then '大型店'
                        when BIS_SHOP.SHOP_TYPE_CD = '6' and (IS_NEW = '0' or IS_NEW is null) then '中型店'
                        when BIS_SHOP.SHOP_TYPE_CD = '7' and (IS_NEW = '0' or IS_NEW is null) then '小型店'
                        when BIS_SHOP.SHOP_TYPE_CD = '8' and (IS_NEW = '0' or IS_NEW is null) then '商业街'
                        when BIS_SHOP.SHOP_TYPE_CD = '9' and (IS_NEW = '0' or IS_NEW is null) then '住宅底商'
                        when BIS_SHOP.SHOP_TYPE_CD = '1' and BIS_SHOP.IS_NEW = '1' then '主力店'
                        when BIS_SHOP.SHOP_TYPE_CD = '2' and BIS_SHOP.IS_NEW = '1' then '次主力店'
                        when BIS_SHOP.SHOP_TYPE_CD = '3' and BIS_SHOP.IS_NEW = '1' then '大商铺'
                        when BIS_SHOP.SHOP_TYPE_CD = '4' and BIS_SHOP.IS_NEW = '1' then '小商铺'
                        when BIS_SHOP.SHOP_TYPE_CD = '5' and BIS_SHOP.IS_NEW = '1' then '一般商铺'
                        when BIS_SHOP.SHOP_TYPE_CD = '6' and BIS_SHOP.IS_NEW = '1' then '中型商铺'
                        else null end),                                                                        -- 店型,
                max(case
                        when bis_shop.is_new_shop = '1' then '有效'
                        when bis_shop.is_new_shop = '0' then '无效'
                        else null end),                                                                        -- 商家状态,
                max(table1.project_number),                                                                    -- 合作项目数,
                max(table1.project_name) as project_name,                                                      -- 合作项目,
                max(table2.own_total_money)                                                                    -- 欠费
         from ods.ods_pl_powerdes_bis_shop_dt bis_shop
                  left join ods.ods_pl_powerdes_bis_shop_sort_new_rel_dt bis_shop_sort_new_rel
                            on bis_shop.bis_shop_id = bis_shop_sort_new_rel.bis_shop_id
                  left join ods.ods_pl_powerdes_bis_shop_sort_new_dt bis_shop_sort_new
                            on bis_shop_sort_new_rel.bis_shop_sort_id = bis_shop_sort_new.bis_shop_sort_id
                  left join
              (
                  select COUNT(t.BIS_PROJECT_ID)                      project_number,
                         t.BIS_SHOP_ID,
                         concat_ws(",", collect_list(t.project_name)) project_name
                  from (
                           SELECT BIS_CONT.BIS_PROJECT_ID,
                                  BIS_SHOP.BIS_SHOP_ID,
                                  BIS_PROJECT.PROJECT_NAME,
                                  row_number() over (partition by BIS_PROJECT.PROJECT_NAME,BIS_SHOP.BIS_SHOP_ID order by BIS_PROJECT.PROJECT_NAME,BIS_SHOP.BIS_SHOP_ID) rank
                           FROM ods.ods_pl_powerdes_bis_shop_dt BIS_SHOP
                                    LEFT JOIN ods.ods_pl_powerdes_bis_cont_dt BIS_CONT ON BIS_SHOP.BIS_SHOP_ID = BIS_CONT.BIS_SHOP_ID
                                    left join ods.ods_pl_powerdes_bis_project_dt bis_project
                                              on BIS_CONT.BIS_PROJECT_ID = BIS_PROJECT.BIS_PROJECT_ID
                           where bis_shop.delete_bl <> '1'
                             AND bis_cont.effect_flg <> 'D'
                           -- AND BIS_SHOP.BIS_SHOP_ID = '402834704f6e9fda014f721de9172523'
                       ) t
                  where t.rank = 1
                  GROUP BY t.BIS_SHOP_ID
              ) table1 on bis_shop.bis_shop_id = table1.bis_shop_id
                  left join
              (
                  select bis_cont.bis_shop_id,
                         sum(pms_fin_arrearage.own_money) own_total_money-- 欠费
                  from ods.ods_pl_powerdes_pms_fin_arrearage_dt pms_fin_arrearage
                           left join ods.ods_pl_powerdes_bis_cont_dt bis_cont
                                     on pms_fin_arrearage.bis_cont_id = bis_cont.bis_cont_id and
                                        pms_fin_arrearage.project_id = bis_cont.bis_project_id
                  where trim(pms_fin_arrearage.is_del) = '0'
                    and bis_cont.effect_flg <> 'D'
                  group by bis_cont.bis_shop_id
              ) table2 on bis_shop.bis_shop_id = table2.bis_shop_id
         where bis_shop.delete_bl <> '1'
         group by bis_shop.bis_shop_id
     ) t
where t.project_name is not null;




