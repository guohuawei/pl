select table3.area_name                                                               `区域名称`,
       BIS_PROJECT.SHORT_NAME                                                         `项目`,
       BIS_PROJECT.OPEN_DATE                                                          `开业日期`,
       case
           when t2.multi_charge_type = 1 then '固定点位'
           when t2.multi_charge_type = 2 then '临时点位'
           when t2.multi_charge_type = 3 then '宣传点位'
           when t2.multi_charge_type = 4 then 'ATM'
           when t2.multi_charge_type = 5 then '其他'
           when t2.multi_charge_type = 6 then 'DP点位'
           when t2.multi_charge_type = 7 then '外摆点位'
           when t2.multi_charge_type = 8 then '仓库点位'
           when t2.multi_charge_type = 9 then '车展点位'
           when t2.multi_charge_type = 10 then '房展点位'
           when t2.multi_charge_type = 11 then '营销点位'
           else null end    as                                                        `点位类型`,
       nvl(t2.multi_num, 0) as                                                        `规划数量`,
       nvl(t3.multi_num, 0)                                                           `(已开业) 开业当天出租数量`,
       nvl(concat(cast(t3.multi_num / t2.multi_num as decimal(10, 4)) * 100, '%'), 0) `(已开业) 开业当天出租率`,
       nvl(round(t5.rent_price_count, 4), 0)                                          `(已开业) 已出租平均价（数量）`,
       nvl(round(t6.rent_price_area, 4), 0)                                           `(已开业) 已出租平均价（面积）`,
       nvl(t7.multi_num, 0)                                                           `(已签约) 开业当天出租数量`,
       nvl(concat(cast(t7.multi_num / t2.multi_num as decimal(19, 4)) * 100, '%'), 0) `(已签约) 开业当天出租率`,
       nvl(round(t9.rent_price_count, 4), 0)                                          `(已签约) 已出租平均价（数量）`,
       nvl(round(t10.rent_price_area, 4), 0)                                          `(已签约) 已出租平均价（面积）`
from ods.ods_pl_powerdes_bis_project_dt BIS_PROJECT
         left join
     (
         select bs_area.id,
                bs_area.area_name,  -- 区域名称
                bs_mall.area,
                bs_mall.out_mall_id -- 项目id
         from ods.ods_pl_pms_bis_db_bs_area_dt bs_area
                  left join ods.ods_pl_pms_bis_db_bs_mall_dt bs_mall
                            on bs_area.id = bs_mall.area
         where bs_mall.is_del = '0'
           and stat = '2'
     ) table3
     on BIS_PROJECT.bis_project_id = table3.out_mall_id
         left join
     (
         select bis_project_id,
                multi_charge_type,
                count(*) multi_num -- 规划数量
         from ods.ods_pl_powerdes_bis_multi_dt
         where is_delete = '0'
         group by bis_project_id, multi_charge_type
     ) t2 on BIS_PROJECT.bis_project_id = t2.bis_project_id
         left join
     (
         select trim(t1.bis_project_id)    bis_project_id,
                trim(t1.multi_charge_type) multi_charge_type,
                count(*)                   multi_num -- 开业当天出租数量
         from (
                  select t1.bis_project_id,
                         t1.multi_charge_type,
                         t1.bis_multi_id
                  from ods.ods_pl_powerdes_bis_multi_dt t1
                           left join ods.ods_pl_powerdes_bis_cont_dt t2
                                     on t1.bis_project_id = t2.bis_project_id and t1.bis_multi_id = t2.bis_store_ids
                           left join ods.ods_pl_powerdes_bis_project_dt t3 on t1.bis_project_id = t3.bis_project_id
                  where t3.open_date like '2022%'
                    and t2.cont_start_date <= t3.open_date
                    and t1.is_delete = '0'
                    -- and t1.bis_project_id = '2b2f909171884d40846ebcfdd38ef544'
                  group by t1.bis_project_id, t1.multi_charge_type, t1.bis_multi_id
              ) t1
         group by trim(t1.bis_project_id), trim(t1.multi_charge_type)
     ) t3 on t2.bis_project_id = t3.bis_project_id and t2.multi_charge_type = t3.multi_charge_type
         left join
     (
         select t1.bis_project_id,
                t1.multi_charge_type,
                t3.price / t1.multi_num as rent_price_count -- 已出租平均价（数量）
         from (
                  select t1.bis_project_id,
                         t1.multi_charge_type,
                         count(*) multi_num --点位数
                  from (
                           -- 点位去重
                           select t1.bis_project_id,
                                  t1.multi_charge_type,
                                  t1.bis_multi_id
                           from ods.ods_pl_powerdes_bis_multi_dt t1
                                    left join ods.ods_pl_powerdes_bis_project_dt t2
                                              on t1.bis_project_id = t2.bis_project_id and open_date like '2022%'
                                    left join ods.ods_pl_powerdes_bis_cont_dt t3
                                              on t1.bis_project_id = t3.bis_project_id and
                                                 t1.bis_multi_id = t3.bis_store_ids
                           where t3.cont_start_date <= t2.open_date
                             and t1.is_delete = '0'
                           group by t1.bis_project_id, t1.multi_charge_type, t1.bis_multi_id
                       ) t1
                  group by t1.bis_project_id, t1.multi_charge_type
              ) t1
                  left join
              (
                  select t1.bis_project_id,
                         t1.multi_charge_type,
                         sum(t3.RENT_PROFIT_TOTAL / round(months_between(cont_end_date, cont_start_date), 2)) as price

                  from ods.ods_pl_powerdes_bis_multi_dt t1
                           left join ods.ods_pl_powerdes_bis_project_dt t2
                                     on t1.bis_project_id = t2.bis_project_id and open_date like '2022%'
                           left join ods.ods_pl_powerdes_bis_cont_dt t3
                                     on t1.bis_project_id = t3.bis_project_id and
                                        t1.bis_multi_id = t3.bis_store_ids
                  where t3.cont_start_date <= t2.open_date
                    and t1.is_delete = '0'
                  group by t1.bis_project_id, t1.multi_charge_type
              ) t3
              on t1.bis_project_id = t3.bis_project_id and t1.multi_charge_type = t3.multi_charge_type
     ) t5 on t2.bis_project_id = t5.bis_project_id and t2.multi_charge_type = t5.multi_charge_type
         left join
     (
         select t1.bis_project_id,
                t1.multi_charge_type,
                t3.price / t1.rent_square as rent_price_area -- 已出租平均价（面积）
         from (
                  select t1.bis_project_id,
                         t1.multi_charge_type,
                         sum(t3.rent_square) rent_square -- 总面积
                  from ods.ods_pl_powerdes_bis_multi_dt t1
                           left join ods.ods_pl_powerdes_bis_project_dt t2
                                     on t1.bis_project_id = t2.bis_project_id and open_date like '2022%'
                           left join ods.ods_pl_powerdes_bis_cont_dt t3
                                     on t1.bis_project_id = t3.bis_project_id and
                                        t1.bis_multi_id = t3.bis_store_ids
                  where t3.cont_start_date <= t2.open_date
                    and t1.is_delete = '0'
                  group by t1.bis_project_id, t1.multi_charge_type
              ) t1
                  left join
              (
                  select t1.bis_project_id,
                         t1.multi_charge_type,
                         sum(t3.RENT_PROFIT_TOTAL /
                             round(months_between(cont_end_date, cont_start_date), 2)) price -- 总面积
                  from ods.ods_pl_powerdes_bis_multi_dt t1
                           left join ods.ods_pl_powerdes_bis_project_dt t2
                                     on t1.bis_project_id = t2.bis_project_id and open_date like '2022%'
                           left join ods.ods_pl_powerdes_bis_cont_dt t3
                                     on t1.bis_project_id = t3.bis_project_id and
                                        t1.bis_multi_id = t3.bis_store_ids
                  where t3.cont_start_date <= t2.open_date
                    and t1.is_delete = '0'
                  group by t1.bis_project_id, t1.multi_charge_type
              ) t3
              on t1.bis_project_id = t3.bis_project_id and t1.multi_charge_type = t3.multi_charge_type
     ) t6 on t2.bis_project_id = t6.bis_project_id and t2.multi_charge_type = t6.multi_charge_type
         left join
     (
         select trim(t1.bis_project_id)    bis_project_id,
                trim(t1.multi_charge_type) multi_charge_type,
                count(*)                   multi_num -- 开业当天出租数量
         from (
                  select trim(t1.bis_project_id)    bis_project_id,
                         trim(t1.multi_charge_type) multi_charge_type,
                         trim(t1.bis_multi_id)      bis_multi_id
                  from ods.ods_pl_powerdes_bis_multi_dt t1
                           left join ods.ods_pl_powerdes_bis_cont_dt t2
                                     on t1.bis_project_id = t2.bis_project_id and t1.bis_multi_id = t2.bis_store_ids
                           left join ods.ods_pl_powerdes_bis_project_dt t3 on t1.bis_project_id = t3.bis_project_id
                  where t3.open_date like '2022%'
                    and t2.status_cd <> '2'
                    and t1.is_delete = '0'
                  group by trim(t1.bis_project_id), trim(t1.multi_charge_type), trim(t1.bis_multi_id)
              ) t1
         group by trim(t1.bis_project_id), trim(t1.multi_charge_type)
     ) t7 on t2.bis_project_id = t7.bis_project_id and t2.multi_charge_type = t7.multi_charge_type
         left join
     (
         select t1.bis_project_id,
                t1.multi_charge_type,
                t3.total_price / t1.multi_num  as rent_price_count -- 已出租平均价（数量）
         from (
                  select t1.bis_project_id,
                         t1.multi_charge_type,
                         count(*) multi_num
                  from (
                           select t1.bis_project_id,
                                  t1.multi_charge_type,
                                  t1.bis_multi_id
                           from ods.ods_pl_powerdes_bis_multi_dt t1
                                    left join ods.ods_pl_powerdes_bis_project_dt t2
                                              on t1.bis_project_id = t2.bis_project_id and open_date like '2022%'
                                    left join ods.ods_pl_powerdes_bis_cont_dt t3
                                              on t1.bis_project_id = t3.bis_project_id and
                                                 t1.bis_multi_id = t3.bis_store_ids
                           where t3.status_cd <> '2'
                             and t1.is_delete = '0'
                           group by t1.bis_project_id, t1.multi_charge_type, t1.bis_multi_id
                       ) t1
                  group by t1.bis_project_id, t1.multi_charge_type
              ) t1
                  left join
              (
                  select bis_project_id,
                         multi_charge_type,
                         sum(price) total_price
                  from (
                           select t1.bis_project_id,
                                  t1.multi_charge_type,
                                  t3.RENT_PROFIT_TOTAL /
                                  round(months_between(cont_end_date, cont_start_date), 2) as price -- 每个点位的单价
                           from ods.ods_pl_powerdes_bis_multi_dt t1
                                    left join ods.ods_pl_powerdes_bis_project_dt t2
                                              on t1.bis_project_id = t2.bis_project_id and open_date like '2022%'
                                    left join ods.ods_pl_powerdes_bis_cont_dt t3
                                              on t1.bis_project_id = t3.bis_project_id and
                                                 t1.bis_multi_id = t3.bis_store_ids
                           where t3.status_cd <> '2'
                             and t1.is_delete = '0'
                       ) t1
                  group by bis_project_id, multi_charge_type
              ) t3
              on t1.bis_project_id = t3.bis_project_id and t1.multi_charge_type = t3.multi_charge_type
     ) t9 on t2.bis_project_id = t9.bis_project_id and t2.multi_charge_type = t9.multi_charge_type
         left join
     (
         select t1.bis_project_id,
                t1.multi_charge_type,
                t3.total_price / t1.rent_square as rent_price_area -- 已出租平均价（面积）
         from (
                  select t1.bis_project_id,
                         t1.multi_charge_type,
                         sum(t3.rent_square) rent_square -- 总面积
                  from ods.ods_pl_powerdes_bis_multi_dt t1
                           left join ods.ods_pl_powerdes_bis_project_dt t2
                                     on t1.bis_project_id = t2.bis_project_id and open_date like '2022%'
                           left join ods.ods_pl_powerdes_bis_cont_dt t3
                                     on t1.bis_project_id = t3.bis_project_id and t1.bis_multi_id = t3.bis_store_ids
                  where t3.status_cd <> '2'
                    and t1.is_delete = '0'
                  group by t1.bis_project_id, t1.multi_charge_type
              ) t1
                  left join
              (
                  select bis_project_id,
                         multi_charge_type,
                         sum(price) total_price
                  from (

                           select t1.bis_project_id,
                                  t1.multi_charge_type,
                                  t3.RENT_PROFIT_TOTAL /
                                  round(months_between(cont_end_date, cont_start_date), 2)  as price -- 每个点位的单价
                           from ods.ods_pl_powerdes_bis_multi_dt t1
                                    left join ods.ods_pl_powerdes_bis_project_dt t2
                                              on t1.bis_project_id = t2.bis_project_id and open_date like '2022%'
                                    left join ods.ods_pl_powerdes_bis_cont_dt t3
                                              on t1.bis_project_id = t3.bis_project_id and
                                                 t1.bis_multi_id = t3.bis_store_ids
                           where t3.status_cd <> '2'
                             and t1.is_delete = '0'
                       ) t1
                  group by bis_project_id, multi_charge_type
              ) t3
              on t1.bis_project_id = t3.bis_project_id and t1.multi_charge_type = t3.multi_charge_type
     ) t10 on t2.bis_project_id = t10.bis_project_id and t2.multi_charge_type = t10.multi_charge_type
where BIS_PROJECT.open_date like '2022%'
  and BIS_PROJECT.bis_project_id not in ('86DF8DD7649E414BE0530D14FB0AB5A0', 'D1FE66A7CCC2258AE0530B03FB0ADB4E')




