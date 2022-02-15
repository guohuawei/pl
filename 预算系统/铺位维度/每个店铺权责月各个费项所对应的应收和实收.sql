/**
  C:铺位维度--每个店铺权责月各个费项所对应的应收和实收
  dws_basic_data_store_rent_big
 */

select table2.bis_project_id,                                              -- 项目id
       bis_cont_bis_store_ids.bis_cont_id,                                 -- 合同id
       bis_cont_bis_store_ids.bis_store_id,                                -- 店铺id
       table2.discount_rate,                                               -- 扣率
       table2.qz_year_month,                                               -- 全责月
       table2.fee_type,                                                    -- 费项
       bis_store_merge.rent_square / table1.totalArea * table2.must_money, --每月应收
       bis_store_merge.rent_square / table1.totalArea * table2.fact_money  -- 每月实收

from (
         select t.bis_store_id,
                T.bis_cont_id
         from (
                  -- 铺位打散
                  SELECT tmp.bis_cont_id,
                         tmp.bis_store_ids,
                         bis_store_id,
                         bis_project_id
                  FROM (
                           SELECT bis_cont_id,
                                  bis_store_ids,
                                  bis_project_id
                           FROM ods.ods_pl_powerdes_bis_cont_dt
                           where effect_flg <> 'D' -- 有效合同，对应的铺位不是空铺
                       ) tmp LATERAL VIEW explode(split(bis_store_ids, ',')) tmp1 AS bis_store_id
              ) t
     ) bis_cont_bis_store_ids
         left join
     (
         select bis_store.bis_store_id,
                '1' as store_type,
                bis_store.rent_square
         from ods.ods_pl_powerdes_bis_store_dt bis_store
         where bis_store.is_delete = 'N'
           and bis_store.status_cd = '1'
         union
         select bis_multi.bis_multi_id,
                '2'                                as store_type,
                cast(bis_multi.max_area as double) as rent_square
         from ods.ods_pl_powerdes_bis_multi_dt bis_multi
     ) bis_store_merge
     on bis_cont_bis_store_ids.bis_store_id = bis_store_merge.bis_store_id
         left join
     (
         select bis_cont_bis_store_ids.bis_cont_id,  -- 合同号
                sum(bis_store.rent_square) totalArea -- 一个合同下所有店铺面积总和
         from (
                  select t.bis_store_id,
                         T.bis_cont_id
                  from (
                           -- 铺位打散
                           SELECT tmp.bis_cont_id,
                                  tmp.bis_store_ids,
                                  bis_store_id,
                                  bis_project_id
                           FROM (
                                    SELECT bis_cont_id,
                                           bis_store_ids,
                                           bis_project_id
                                    FROM ods.ods_pl_powerdes_bis_cont_dt
                                    where effect_flg <> 'D' -- 有效合同，对应的铺位不是空铺
                                ) tmp LATERAL VIEW explode(split(bis_store_ids, ',')) tmp1 AS bis_store_id
                       ) t
              ) bis_cont_bis_store_ids
                  left join ods.ods_pl_powerdes_bis_store_dt bis_store
                            on bis_store.bis_store_id = bis_cont_bis_store_ids.bis_store_id
         group by bis_cont_bis_store_ids.bis_cont_id
     ) table1 on table1.bis_cont_id = bis_cont_bis_store_ids.bis_cont_id
         left join
     (
         select cont.bis_project_id,
                cont.bis_cont_id,
                cont.rent_square,
                cont.status_cd,
                cont.cont_type_cd,
                cont.store_type,
                cont.effect_flg,
                cont.is_create_rent,
                cont.management_forms,
                coalesce(mst.qz_year_month, adj.qz_year_month)     as qz_year_month,
                coalesce(mst.must_type, adj.fee_type)              as fee_type,
                mst.must_base,
                mst.must_date,
                mst.discount_rate,
                fct.fact_base,
                fct.fact_hexiao,
                fct.fact_date,
                fct02.fact_base02,
                fct02.fact_hexiao02,
                fct02.fact_date02,
                adj.must_adj,
                adj.fact_adj,
                (nvl(mst.must_base, 0) + nvl(adj.must_adj, 0))     as must_money,
                (nvl(fct.fact_base, 0) + nvl(adj.fact_adj, 0))     as fact_money,
                (nvl(fct02.fact_base02, 0) + nvl(adj.fact_adj, 0)) as fact_money02
         from
             -- 合同表
             (select bis_project_id,
                     bis_cont_id,
                     rent_square,
                     status_cd,
                     cont_type_cd,
                     store_type,
                     effect_flg,
                     is_create_rent,
                     management_forms
              from ods.ods_pl_powerdes_bis_cont_dt c
             ) cont
                 inner join
             -- 应收费用表
                 (select bis_cont_id,
                         qz_year_month,
                         must_type,
                         max(discount_rate) as   discount_rate,
                         sum(money)              must_base,
                         max(rent_last_pay_date) must_date
                  from ods.ods_pl_powerdes_bis_must2_dt
                  where (is_delete = '0' or is_delete is null)
                    --and must_type in ('1', '2', '34') -- 34为多经场地使用费
                    and must_type is not null
                  group by bis_cont_id, qz_year_month, must_type
                 ) mst
             on cont.bis_cont_id = mst.bis_cont_id
                 left outer join
             -- 实收费用表
                 (select bis_cont_id,
                         qz_year_month,
                         fact_type,
                         sum(money)                                                fact_base,
                         sum(case when fact_method = '8' then money else 0 end) as fact_hexiao, -- 核销收入
                         max(fact_date)                                            fact_date
                  from ods.ods_pl_powerdes_bis_fact2_dt
                  where is_delete = '1' -- 实收表is_delete = '1'为有效
                    --and fact_type in ('1', '2', '34')
                    and fact_type is not null
                  group by bis_cont_id, qz_year_month, fact_type
                 ) fct
             on mst.bis_cont_id = fct.bis_cont_id and mst.qz_year_month = fct.qz_year_month and
                mst.must_type = fct.fact_type
                 left outer join
             -- 实收费用表(财务实收当月口径：只计实收月<=权责月的数据)
                 (select bis_cont_id,
                         qz_year_month,
                         fact_type,
                         sum(money)                                                fact_base02,
                         sum(case when fact_method = '8' then money else 0 end) as fact_hexiao02, -- 核销收入
                         max(fact_date)                                            fact_date02
                  from ods.ods_pl_powerdes_bis_fact2_dt
                  where is_delete = '1' -- 实收表is_delete = '1'为有效
                    --and fact_type in ('1', '2', '34')
                    and fact_type is not null
                    and date_format(fact_date, 'yyyy-MM') <= qz_year_month
                  group by bis_cont_id, qz_year_month, fact_type
                 ) fct02
             on mst.bis_cont_id = fct02.bis_cont_id and mst.qz_year_month = fct02.qz_year_month and
                mst.must_type = fct02.fact_type
                 left outer join
             -- 调整费用表
                 (select bis_cont_id,
                         qz_year_month,
                         fee_type,
                         sum(case when adjust_type = '1' then adjust_money else 0 end) as must_adj, -- 调整类型（1：应收，2：实收）
                         sum(case when adjust_type = '2' then adjust_money else 0 end) as fact_adj
                  from ods.ods_pl_powerdes_bis_mf_adjust_dt
                  where (is_del = '1' or is_del is null) -- 是否删除（0：是，1：否）
                    --and fee_type in ('1', '2', '34')
                    and fee_type is not null
                  group by bis_cont_id, qz_year_month, fee_type
                 ) adj
             on mst.bis_cont_id = adj.bis_cont_id and mst.qz_year_month = adj.qz_year_month and
                mst.must_type = adj.fee_type
     ) table2 on table2.bis_cont_id = bis_cont_bis_store_ids.bis_cont_id;