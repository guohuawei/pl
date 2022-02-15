select temp1.bis_project_id,                                -- 项目id
       temp1.project_name       bis_project_name,           --项目名称
       temp1.primary_forms,                                 -- 一级业态
       sum(temp1.rent_amount)   rent_opening3_year,         --从项目开业时间前三年租金应收
       sum(temp2.rent_standard) rent_standard_opening3_year --从项目开业时间前三年标准租金应收
from (
         select t1.bis_project_id,
                t1.project_name,
                t3.bis_cont_id,
                t3.BIS_STORE_IDS                                        bis_store_id,
                case
                    when (t4.SHOP_TYPE_CD in ('1', '2', '3') and nvl(t4.is_new, 0) = '0')
                        or (t4.SHOP_TYPE_CD = '1' and nvl(t4.is_new, 0) = '1') then '主力店'
                    when (t4.SHOP_TYPE_CD = '4' and nvl(t4.is_new, 0) = '0')
                        or (t4.SHOP_TYPE_CD = '2' and nvl(t4.is_new, 0) = '1') then '次主力店'
                    else t6.primary_forms end                           primary_forms,
                sum(case when t2.must_type = '1' then MONEY else 0 end) rent_amount
         from ods.ods_pl_powerdes_bis_project_dt t1
                  left join ods.ods_pl_powerdes_bis_must2_dt t2 on t1.bis_project_id = t2.bis_project_id
                  left join ods.ods_pl_powerdes_bis_cont_dt t3 on t2.BIS_CONT_ID = t3.bis_cont_id
                  left join ods.ods_pl_powerdes_bis_shop_dt t4 on t3.bis_shop_id = t4.bis_shop_id
                  left join
              (
                  select BIS_SHOP_ID,              -- 商家id
                         primary_forms,            -- 一级业态
                         name_cn cooperative_brand -- 品牌名称
                  from dwd.dwd_bis_shop_primary_forms_big_dt
                  where dt = date_format(current_date, 'yyyyMMdd')
              ) t6 on t4.bis_shop_id = t6.bis_shop_id
         where t2.MUST_YEAR_MONTH between substr(t1.open_date, 1, 7) and substr(add_months(t1.open_date, 36), 1, 7)
           and t1.oper_status = '1'
           and is_business_project = '1'
         group by t1.bis_project_id,
                  t1.project_name,
                  t3.bis_cont_id,
                  t3.BIS_STORE_IDS,
                  case
                      when (t4.SHOP_TYPE_CD in ('1', '2', '3') and nvl(t4.is_new, 0) = '0')
                          or (t4.SHOP_TYPE_CD = '1' and nvl(t4.is_new, 0) = '1') then '主力店'
                      when (t4.SHOP_TYPE_CD = '4' and nvl(t4.is_new, 0) = '0')
                          or (t4.SHOP_TYPE_CD = '2' and nvl(t4.is_new, 0) = '1') then '次主力店'
                      else t6.primary_forms end) temp1
         left join
     (
         select a2.bis_project_id,
                a2.project_name,
                a1.bis_store_id,
                sum((12 - substr(a2.OPEN_DATE, 6, 2)) * a3.rent_year1 * a1.rent_square +
                    12 * a3.rent_year2 * a1.rent_square
                    + 12 * a3.rent_year3 * a1.rent_square +
                    substr(a2.OPEN_DATE, 6, 2) * a3.rent_year4 * a1.rent_square)   rent_standard,
                sum((12 - substr(a2.OPEN_DATE, 6, 2)) * a3.manage_year1 * a1.rent_square +
                    12 * a3.manage_year2 * a1.rent_square
                    + 12 * a3.manage_year3 * a1.rent_square +
                    substr(a2.OPEN_DATE, 6, 2) * a3.manage_year4 * a1.rent_square) manage_standard
         from ods.ods_pl_powerdes_bis_store_dt a1
                  left join ods.ods_pl_powerdes_bis_project_dt a2 on a1.bis_project_id = a2.bis_project_id
                  left join
              (
                  select bis_store_id,
                         max(case when seq_year = '1' then rent_price else 0 end) rent_year1,
                         max(case when seq_year = '2' then rent_price else 0 end) rent_year2,
                         max(case when seq_year = '3' then rent_price else 0 end) rent_year3,
                         max(case when seq_year = '4' then rent_price else 0 end) rent_year4,
                         max(case when seq_year = '1' then MGR_PRICE else 0 end)  manage_year1,
                         max(case when seq_year = '2' then MGR_PRICE else 0 end)  manage_year2,
                         max(case when seq_year = '3' then MGR_PRICE else 0 end)  manage_year3,
                         max(case when seq_year = '4' then MGR_PRICE else 0 end)  manage_year4
                  from ods.ods_pl_powerdes_BIS_STORE_PRICE_dt
                  group by bis_store_id
              ) a3 on a1.bis_store_id = a3.bis_store_id
         where 1 = 1
           and a1.IS_DELETE <> 'Y'
           and a2.oper_status = '1'
           and is_business_project = '1'
         group by a2.bis_project_id,
                  a2.project_name,
                  a1.bis_store_id
     ) temp2 on temp1.bis_project_id = temp2.bis_project_id and temp1.bis_store_id = temp2.bis_store_id
where temp1.primary_forms is not null
group by temp1.bis_project_id,
         temp1.project_name,
         temp1.primary_forms;

