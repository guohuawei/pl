insert OVERWRITE table dws.dws_bis_tobeopen_project_merchantstep_big_dt partition (dt = '${hiveconf:nowdate}')
select bis_project_id, -- 项目id
       project_name,   -- 项目名称
       query_date,     -- 月份
       primary_forms,  -- 一级业态
       steptype,       -- 招商进度
       targetnum,      -- 目标数量
       actualnum       -- 实际数量
from (
         select a1.bis_project_id,
                a1.project_name,
                a1.query_date,
                case
                    when (a4.SHOP_TYPE_CD in ('1', '2', '3') and nvl(a4.is_new, 0) = '0')
                        or (a4.SHOP_TYPE_CD = '1' and nvl(a4.is_new, 0) = '1') then '主力店'
                    when (a4.SHOP_TYPE_CD = '4' and nvl(a4.is_new, 0) = '0')
                        or (a4.SHOP_TYPE_CD = '2' and nvl(a4.is_new, 0) = '1') then '次主力店'
                    else a5.primary_forms end primary_forms,
                '1'                           steptype,
                count(a2.bis_shop_id)         targetnum,
                count(case
                          when a1.query_date between substr(a3.CREATED_DATE, 1, 7) and nvl(substr(a3.REAL_COMPLETED_DATE, 1, 7), '2099-12')
                              then a2.BIS_SHOP_ID
                          else null end)      actualnum
         from (
                  select t1.bis_project_id,
                         t1.project_name,
                         t2.query_date
                  from ods.ods_pl_powerdes_bis_project_dt t1,
                       (select concat(substr(month_id, 1, 4), '-', substr(month_id, 5, 2)) query_date
                        from dim.dim_date
                        where month_id >= '2015-01'
                        group by concat(substr(month_id, 1, 4), '-', substr(month_id, 5, 2))) t2
                  where t1.oper_status = '1'
                    and t1.is_business_project = '1'
              ) a1
                  left join ods.ods_pl_powerdes_zs_plan_dt a2 on a1.bis_project_id = a2.PROJECT_ID
                  left join ods.ods_pl_powerdes_zs_plan_step_dt a3 on a2.id = a3.ZS_PLAN_ID and a3.STEP_NO = '4'
                  left join ods.ods_pl_powerdes_bis_shop_dt a4 on a2.bis_shop_id = a4.bis_shop_id
                  left join
              (
                  select BIS_SHOP_ID,              -- 商家id
                         primary_forms,            -- 一级业态
                         name_cn cooperative_brand -- 品牌名称
                  from dwd.dwd_bis_shop_primary_forms_big_dt
                  where dt = date_format(current_date, 'yyyyMMdd')
              ) a5 on a4.bis_shop_id = a5.bis_shop_id
         group by a1.bis_project_id,
                  a1.project_name,
                  a1.query_date,
                  case
                      when (a4.SHOP_TYPE_CD in ('1', '2', '3') and nvl(a4.is_new, 0) = '0')
                          or (a4.SHOP_TYPE_CD = '1' and nvl(a4.is_new, 0) = '1') then '主力店'
                      when (a4.SHOP_TYPE_CD = '4' and nvl(a4.is_new, 0) = '0')
                          or (a4.SHOP_TYPE_CD = '2' and nvl(a4.is_new, 0) = '1') then '次主力店'
                      else a5.primary_forms end
         union all
         select a1.bis_project_id,
                a1.project_name,
                a1.query_date,
                case
                    when (a4.SHOP_TYPE_CD in ('1', '2', '3') and nvl(a4.is_new, 0) = '0')
                        or (a4.SHOP_TYPE_CD = '1' and nvl(a4.is_new, 0) = '1') then '主力店'
                    when (a4.SHOP_TYPE_CD = '4' and nvl(a4.is_new, 0) = '0')
                        or (a4.SHOP_TYPE_CD = '2' and nvl(a4.is_new, 0) = '1') then '次主力店'
                    else a5.primary_forms end primary_forms,
                '2'                           steptype,
                count(a2.bis_shop_id)         targetnum,
                count(case
                          when a1.query_date between substr(a3.CREATED_DATE, 1, 7) and nvl(substr(a3.REAL_COMPLETED_DATE, 1, 7), '2099-12')
                              then a2.BIS_SHOP_ID
                          else null end)      actualnum
         from (
                  select t1.bis_project_id,
                         t1.project_name,
                         t2.query_date
                  from ods.ods_pl_powerdes_bis_project_dt t1,
                       (select concat(substr(month_id, 1, 4), '-', substr(month_id, 5, 2)) query_date
                        from dim.dim_date
                        where month_id >= '2015-01'
                        group by concat(substr(month_id, 1, 4), '-', substr(month_id, 5, 2))) t2
                  where t1.oper_status = '1'
                    and t1.is_business_project = '1'
              ) a1
                  left join ods.ods_pl_powerdes_zs_plan_dt a2 on a1.bis_project_id = a2.PROJECT_ID
                  left join ods.ods_pl_powerdes_zs_plan_step_dt a3 on a2.id = a3.ZS_PLAN_ID and a3.STEP_NO = '5'
                  left join ods.ods_pl_powerdes_bis_shop_dt a4 on a2.bis_shop_id = a4.bis_shop_id
                  left join
              (
                  select BIS_SHOP_ID,              -- 商家id
                         primary_forms,            -- 一级业态
                         name_cn cooperative_brand -- 品牌名称
                  from dwd.dwd_bis_shop_primary_forms_big_dt
                  where dt = date_format(current_date, 'yyyyMMdd')
              ) a5 on a4.bis_shop_id = a5.bis_shop_id
         group by a1.bis_project_id,
                  a1.project_name,
                  a1.query_date,
                  case
                      when (a4.SHOP_TYPE_CD in ('1', '2', '3') and nvl(a4.is_new, 0) = '0')
                          or (a4.SHOP_TYPE_CD = '1' and nvl(a4.is_new, 0) = '1') then '主力店'
                      when (a4.SHOP_TYPE_CD = '4' and nvl(a4.is_new, 0) = '0')
                          or (a4.SHOP_TYPE_CD = '2' and nvl(a4.is_new, 0) = '1') then '次主力店'
                      else a5.primary_forms end
         union all
         select a1.bis_project_id,
                a1.project_name,
                a1.query_date,
                case
                    when (a4.SHOP_TYPE_CD in ('1', '2', '3') and nvl(a4.is_new, 0) = '0')
                        or (a4.SHOP_TYPE_CD = '1' and nvl(a4.is_new, 0) = '1') then '主力店'
                    when (a4.SHOP_TYPE_CD = '4' and nvl(a4.is_new, 0) = '0')
                        or (a4.SHOP_TYPE_CD = '2' and nvl(a4.is_new, 0) = '1') then '次主力店'
                    else a5.primary_forms end primary_forms,
                '3'                           steptype,
                count(a2.bis_shop_id)         targetnum,
                count(case
                          when a1.query_date between substr(a3.CREATED_DATE, 1, 7) and nvl(substr(a3.REAL_COMPLETED_DATE, 1, 7), '2099-12')
                              then a2.BIS_SHOP_ID
                          else null end)      actualnum
         from (
                  select t1.bis_project_id,
                         t1.project_name,
                         t2.query_date
                  from ods.ods_pl_powerdes_bis_project_dt t1,
                       (select concat(substr(month_id, 1, 4), '-', substr(month_id, 5, 2)) query_date
                        from dim.dim_date
                        where month_id >= '2015-01'
                        group by concat(substr(month_id, 1, 4), '-', substr(month_id, 5, 2))) t2
                  where t1.oper_status = '1'
                    and t1.is_business_project = '1'
              ) a1
                  left join ods.ods_pl_powerdes_zs_plan_dt a2 on a1.bis_project_id = a2.PROJECT_ID
                  left join ods.ods_pl_powerdes_zs_plan_step_dt a3 on a2.id = a3.ZS_PLAN_ID and a3.STEP_NO = '6'
                  left join ods.ods_pl_powerdes_bis_shop_dt a4 on a2.bis_shop_id = a4.bis_shop_id
                  left join
              (
                  select BIS_SHOP_ID,              -- 商家id
                         primary_forms,            -- 一级业态
                         name_cn cooperative_brand -- 品牌名称
                  from dwd.dwd_bis_shop_primary_forms_big_dt
                  where dt = date_format(current_date, 'yyyyMMdd')
              ) a5 on a4.bis_shop_id = a5.bis_shop_id
         group by a1.bis_project_id,
                  a1.project_name,
                  a1.query_date,
                  case
                      when (a4.SHOP_TYPE_CD in ('1', '2', '3') and nvl(a4.is_new, 0) = '0')
                          or (a4.SHOP_TYPE_CD = '1' and nvl(a4.is_new, 0) = '1') then '主力店'
                      when (a4.SHOP_TYPE_CD = '4' and nvl(a4.is_new, 0) = '0')
                          or (a4.SHOP_TYPE_CD = '2' and nvl(a4.is_new, 0) = '1') then '次主力店'
                      else a5.primary_forms end
         union all
         select a1.bis_project_id,
                a1.project_name,
                a1.query_date,
                case
                    when (a4.SHOP_TYPE_CD in ('1', '2', '3') and nvl(a4.is_new, 0) = '0')
                        or (a4.SHOP_TYPE_CD = '1' and nvl(a4.is_new, 0) = '1') then '主力店'
                    when (a4.SHOP_TYPE_CD = '4' and nvl(a4.is_new, 0) = '0')
                        or (a4.SHOP_TYPE_CD = '2' and nvl(a4.is_new, 0) = '1') then '次主力店'
                    else a5.primary_forms end primary_forms,
                '4'                           steptype,
                count(a2.bis_shop_id)         targetnum,
                count(case
                          when a1.query_date between substr(a3.CREATED_DATE, 1, 7) and nvl(substr(a3.REAL_COMPLETED_DATE, 1, 7), '2099-12')
                              then a2.BIS_SHOP_ID
                          else null end)      actualnum
         from (
                  select t1.bis_project_id,
                         t1.project_name,
                         t2.query_date
                  from ods.ods_pl_powerdes_bis_project_dt t1,
                       (select concat(substr(month_id, 1, 4), '-', substr(month_id, 5, 2)) query_date
                        from dim.dim_date
                        where month_id >= '2015-01'
                        group by concat(substr(month_id, 1, 4), '-', substr(month_id, 5, 2))) t2
                  where t1.oper_status = '1'
                    and t1.is_business_project = '1'
              ) a1
                  left join ods.ods_pl_powerdes_zs_plan_dt a2 on a1.bis_project_id = a2.PROJECT_ID
                  left join ods.ods_pl_powerdes_zs_plan_step_dt a3 on a2.id = a3.ZS_PLAN_ID and a3.STEP_NO = '7'
                  left join ods.ods_pl_powerdes_bis_shop_dt a4 on a2.bis_shop_id = a4.bis_shop_id
                  left join
              (
                  select BIS_SHOP_ID,              -- 商家id
                         primary_forms,            -- 一级业态
                         name_cn cooperative_brand -- 品牌名称
                  from dwd.dwd_bis_shop_primary_forms_big_dt
                  where dt = date_format(current_date, 'yyyyMMdd')
              ) a5 on a4.bis_shop_id = a5.bis_shop_id
         group by a1.bis_project_id,
                  a1.project_name,
                  a1.query_date,
                  case
                      when (a4.SHOP_TYPE_CD in ('1', '2', '3') and nvl(a4.is_new, 0) = '0')
                          or (a4.SHOP_TYPE_CD = '1' and nvl(a4.is_new, 0) = '1') then '主力店'
                      when (a4.SHOP_TYPE_CD = '4' and nvl(a4.is_new, 0) = '0')
                          or (a4.SHOP_TYPE_CD = '2' and nvl(a4.is_new, 0) = '1') then '次主力店'
                      else a5.primary_forms end
     ) temp;