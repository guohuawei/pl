/**
  PMS-预算编制-业务表-租赁大表设计
 */

select bis_project.mall_id,                                                                                  -- 项目id
       bis_store.bis_store_id,                                                                               -- 铺位id
       null                                                                        as unit_number,           -- 单元编号
       bis_store.store_no,                                                                                   -- 铺位编号
       bis_floor.building_num,                                                                               -- 楼栋编号
       bis_floor.floor_num,                                                                                  -- 楼层编号
       bis_floor.charge_type,                                                                                -- 物业类型
       null                                                                        as multi_type,            -- 多经类型
       '1'                                                                         as store_type,            -- 铺位类型（1：铺位 2：多经）
       table1.cont_count,                                                                                    -- 期间合同数量
       bis_store_price.rent_price,                                                                           -- 标准租金
       null                                                                        as price_holidays,        -- 节假日租金
       null                                                                        as price_weekend,         -- 周末租金
       bis_store_price.mgr_price,                                                                            -- 标准管理费
       table7.unit_money                                                           as unit_money1,           -- 租金单价
       table8.unit_money                                                           as history_unit_money,    -- 历史租金单价
       (table7.unit_money / bis_store_price.rent_price)                            as achieving_rate,        -- 达成率
       bis_store.rent_square,                                                                                -- 面积
       null                                                                        as rank,                  -- 等级
       if(table2.bis_cont_id is null, table2.proportion_names, bis_store.issuing_layout_cd),                 -- 铺位业态
       table2.PROPORTION_NAMES,                                                                              -- 合同业态
       table2.PROPORTION_IDS,                                                                                -- 合同业态ids
       case
           when bis_store.EQUITY_NATURE = '1' then '1'
           when bis_store.EQUITY_NATURE = '2' and bis_store.MANAGEMENT_STATUS = '1' then '2'
           when bis_store.EQUITY_NATURE = '2' and bis_store.MANAGEMENT_STATUS = '2' then '3'
           else null
           end                                                                     as property_right,        -- 资产属性
       bis_store.LAST_DATE,                                                                                  -- 返租到期日
       case
           when '2021-01-01' between bis_shop_open_info.open_date and if(table2.status_cd = '2',
                                                                         table2.cont_to_fail_date, table2.cont_end_date)
               then '2'
           when '2021-01-01' between table2.cont_start_date and if(table2.status_cd = '2', table2.cont_to_fail_date,
                                                                   table2.cont_end_date)
               then '3'
           when table2.bis_cont_id is null
               then '1' end                                                        as Initial_state,         -- 期初状态
       table2.bis_cont_id,                                                                                   -- 合同id
       null                                                                        as is_adjust,             -- 是否调铺（1：是 0：否）
       null                                                                        as cont_type,             -- 合同类型
       table2.cont_no,                                                                                       -- 合同号
       table2.cont_start_date,                                                                               -- 合同开始日期
       table2.cont_end_date,                                                                                 -- 合同结束日期
       CASE
           WHEN table2.cont_type_cd = 3 THEN bis_more_manage_leasee.leasee_name
           WHEN table2.cont_type_cd = 2 AND bis_shop_conn.part_name IS NULL THEN bis_more_manage_leasee.leasee_name
           WHEN table2.cont_type_cd = 2 AND bis_more_manage_leasee.leasee_name IS NULL THEN bis_shop_conn.part_name
           ELSE bis_shop_conn.part_name END                                        AS companyName,           -- 商戶
       CASE
           WHEN table2.cont_type_cd = 3 THEN bis_more_manage_leasee.more_manage_leasee_id
           WHEN table2.cont_type_cd = 2 AND bis_shop_conn.part_name IS NULL
               THEN bis_more_manage_leasee.more_manage_leasee_id
           WHEN table2.cont_type_cd = 2 AND bis_more_manage_leasee.leasee_name IS NULL
               THEN bis_shop_conn.bis_shop_conn_id
           ELSE bis_shop_conn.bis_shop_conn_id END                                 AS companyNameID,         -- 商戶id

       table2.pay_way,                                                                                       -- 計租方式
       table2.pay_cycle_cd                                                         as pay_cycle_cd1,         -- 固定支付週期
       table2.pay_cycle_cd                                                         as pay_cycle_cd2,         -- 扣點支付週期
       table7.ROYALTY_RATIO                                                        as buckle_point,          -- 扣點
       null                                                                        as estimated_sales,       -- 预估销售额
       null                                                                        as sales_type,            -- 销售额方式（1含税2不含税）
       round((table7.unit_money - table8.unit_money) / table8.unit_money, 2) * 100 as increasing_rate,       -- 租金递增率
       null                                                                        as future_income,         -- 未来两年的收入
       table2.pay_way_prop,                                                                                  -- 物管支付方式
       table6.unit_money                                                           as unit_price_wg,         -- 物管单价
       table9.unit_money                                                           as history_unit_price_wg, -- 历史物管单价
       round((table6.unit_money - table9.unit_money) / table9.unit_money, 2) * 100 as increasing_rate_wg,    -- 物管递增率
       null                                                                        as future_income_wg,      -- 未来2年物管费收入
       table7.free_rent_period,                                                                              -- 经营免租期
       table3.store_own_money,                                                                               -- 欠费
       datediff(if(table2.cont_end_date >= '2021-12-31', '2021-12-31', table2.cont_end_date),
                if(table2.cont_start_date <= '2021-01-01', '2021-01-01', table2.cont_start_date)) +
       1                                                                           as lease_term,            -- 有效租期
       table4.rent_total_money                                                     as zj_income,             -- 租金收入
       table4.mgr_total_money                                                      as wg_income,             -- 物管收入
       null                                                                        as total_income,          -- 综合收入
       table4.rent_money_01                                                        as jan_zj,                -- 一月份租金
       table4.rent_money_2                                                         as feb_zj,                -- 二月份租金
       table4.rent_money_3                                                         as march_zj,              -- 三月份租金
       table4.rent_money_4                                                         as april_zj,              -- 四月份租金
       table4.rent_money_5                                                         as may_zj,                -- 五月份租金
       table4.rent_money_6                                                         as june_zj,               -- 六月份租金
       table4.rent_money_7                                                         as july_zj,               -- 七月份租金
       table4.rent_money_8                                                         as aug_zj,                -- 八月份租金
       table4.rent_money_9                                                         as sep_zj,                -- 九月份租金
       table4.rent_money_10                                                        as oct_zj,                -- 十月份租金
       table4.rent_money_11                                                        as nov_zj,                -- 十一月份租金
       table4.rent_money_12                                                        as dece_zj,               -- 十二月份租金
       table4.mgr_money_01                                                         as jan_wg,                -- 一月份物管
       table4.mgr_money_2                                                          as feb_wg,                -- 二月份物管
       table4.mgr_money_3                                                          as march_wg,              -- 三月份物管
       table4.mgr_money_4                                                          as april_wg,              -- 四月份物管
       table4.mgr_money_5                                                          as may_wg,                -- 五月份物管
       table4.mgr_money_6                                                          as june_wg,               -- 六月份物管
       table4.mgr_money_7                                                          as july_wg,               -- 七月份物管
       table4.mgr_money_8                                                          as aug_wg,                -- 八月份物管
       table4.mgr_money_9                                                          as sep_wg,                -- 九月份物管
       table4.mgr_money_10                                                         as oct_wg,                -- 十月份物管
       table4.mgr_money_11                                                         as nov_wg,                -- 十一月份物管
       table4.mgr_money_12                                                         as dece_wg,               -- 十二月份物管
       bis_floor.bis_floor_id,                                                                               -- 楼层id
       table2.bis_shop_id,                                                                                   -- 品牌id
       table2.bis_shop_name,                                                                                 -- 品牌名称
       bis_store.is_delete,                                                                                  -- 铺位是否删除(0:否 1：是)
       bis_store.updated_date,                                                                               -- 铺位更新时间
       bis_store.updator,                                                                                    -- 铺位更新人员
       bis_store.created_date,                                                                               -- 铺位创建时间
       bis_store.creator,                                                                                    -- 铺位创建人
       "oracle"                                                                    as source                 -- 数据来源

from ods.ods_pl_powerdes_bis_store_dt bis_store

         LEFT JOIN
     (
         SELECT max(bis_store_id) as                         store_id,     -- 铺位id
                concat_ws(',', collect_set(tmp.bis_cont_id)) bis_cont_ids, -- 铺位对应的多个合同id
                count(tmp.bis_cont_id)                       cont_count    -- 铺位所对应的合同数量
         FROM (
                  SELECT bis_cont_id,
                         bis_store_ids
                  FROM ods.ods_pl_powerdes_bis_cont_dt
                       -- 2021 需要动态传值
                  where '2021' between year(cont_start_date) and year(if(status_cd = '2', cont_to_fail_date, cont_end_date))
                    and cont_type_cd in ('1', '2')
                    and effect_flg <> 'D'
                    and CONT_NO is not null
              ) tmp LATERAL VIEW explode(split(bis_store_ids, ',')) tmp1 AS bis_store_id
         group by bis_store_id
     ) table1
     on bis_store.bis_store_id = table1.store_id
         left join ods.ods_pl_powerdes_bis_floor_dt bis_floor
                   on bis_store.bis_floor_id = bis_floor.bis_floor_id
         left join ods.ods_pl_powerdes_bis_store_price_dt bis_store_price
                   on bis_store_price.bis_store_id = bis_store.bis_store_id
         left join
     (
         -- 店铺打散
         SELECT tmp.bis_cont_id,
                tmp.bis_store_ids,
                bis_store_id,
                status_cd,
                IF(status_cd = '2', cont_to_fail_date, cont_end_date) AS cont_end_date,
                cont_start_date,
                cont_to_fail_date,
                cont_no,
                cont_type_cd,
                bis_shop_conn_id,
                pay_way,
                pay_cycle_cd,
                pay_way_prop,
                PROPORTION_NAMES,
                PROPORTION_IDS,
                bis_shop_id,
                bis_shop_name


         FROM (
                  SELECT bis_cont_id,
                         bis_store_ids,
                         status_cd,
                         cont_end_date,
                         cont_start_date,
                         cont_to_fail_date,
                         cont_no,
                         cont_type_cd,
                         bis_shop_conn_id,
                         pay_way,
                         pay_cycle_cd,
                         pay_way_prop,
                         PROPORTION_NAMES,
                         PROPORTION_IDS,
                         bis_shop_id,
                         bis_shop_name

                  FROM ods.ods_pl_powerdes_bis_cont_dt
                       -- 2021 需要动态传值
                  where '2021' between year(cont_start_date) and year(if(status_cd = '2', cont_to_fail_date, cont_end_date))
                    and cont_type_cd in ('1', '2')
                    and effect_flg <> 'D'
                    and CONT_NO is not null
              ) tmp LATERAL VIEW explode(split(bis_store_ids, ',')) tmp1 AS bis_store_id
     ) table2 on table2.bis_store_id = bis_store.bis_store_id
         left join
     (
         select bis_must_rent.unit_money, -- 租金单价
                bis_must_rent.bis_cont_id,
                bis_must_rent.free_rent_period,
                bis_must_rent.ROYALTY_RATIO
         from (
                  select bis_cont_id,
                         cont_start_date,
                         cont_end_date
                  from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                  where '2021' between year(bis_cont.cont_start_date) and year(
                          if(bis_cont.status_cd = '2', bis_cont.cont_to_fail_date, bis_cont.cont_end_date))
                    and bis_cont.cont_type_cd in ('1', '2')
                    and bis_cont.effect_flg <> 'D'
                    and bis_cont.CONT_NO is not null
              ) table5
                  left join ods.ods_pl_powerdes_bis_must_rent_dt bis_must_rent
                            on table5.bis_cont_id = bis_must_rent.bis_cont_id
         where (year(table5.cont_start_date) - 1 + cast(bis_must_rent.YEAR as int)) = '2021'
     ) table7
     on table2.bis_cont_id = table7.bis_cont_id
         left join
     (
         select t2.BIS_CONT_ID,
                t2.MYyear,    -- 年份
                t2.UNIT_MONEY -- 历史租金单价
         from (
                  select row_number() over (partition by t1.BIS_CONT_ID order by t1.MYyear desc ) rn,
                         t1.*
                  from (
                           select bis_must_rent.bis_cont_id,
                                  (year(table5.cont_start_date) - 1 + cast(bis_must_rent.YEAR as int)) as MYyear,
                                  bis_must_rent.unit_money -- 历史租金单价
                           from (
                                    select bis_cont_id,
                                           cont_start_date,
                                           cont_end_date
                                    from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                                    where '2021' between year(bis_cont.cont_start_date) and year(
                                            if(bis_cont.status_cd = '2', bis_cont.cont_to_fail_date,
                                               bis_cont.cont_end_date))
                                      and bis_cont.cont_type_cd in ('1', '2')
                                      and bis_cont.effect_flg <> 'D'
                                      and bis_cont.CONT_NO is not null
                                ) table5
                                    left join ods.ods_pl_powerdes_bis_must_rent_dt bis_must_rent
                                              on table5.bis_cont_id = bis_must_rent.bis_cont_id and
                                                 bis_must_rent.unit_money is not null
                           where (year(table5.cont_start_date) - 1 + cast(bis_must_rent.YEAR as int)) < '2021'
                       ) t1) t2
         where t2.rn = 1
     ) table8 on table2.bis_cont_id = table8.bis_cont_id
         left join ods.ods_pl_powerdes_bis_shop_open_info_dt bis_shop_open_info
                   on table2.bis_cont_id = bis_shop_open_info.bis_cont_id
         LEFT JOIN ods.ods_pl_powerdes_bis_more_manage_leasee_dt bis_more_manage_leasee
                   ON table2.bis_cont_id = bis_more_manage_leasee.bis_cont_id
         LEFT JOIN ods.ods_pl_powerdes_bis_shop_conn_dt bis_shop_conn
                   ON table2.bis_shop_conn_id = bis_shop_conn.bis_shop_conn_id
         left join
     (
         select bis_must2_prop.unit_money, -- 物管单价
                bis_must2_prop.bis_cont_id
         from (
                  select bis_cont_id,
                         cont_start_date,
                         cont_end_date
                  from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                  where '2021' between year(bis_cont.cont_start_date) and year(
                          if(bis_cont.status_cd = '2', bis_cont.cont_to_fail_date, bis_cont.cont_end_date))
                    and bis_cont.cont_type_cd in ('1', '2')
                    and bis_cont.effect_flg <> 'D'
                    and bis_cont.CONT_NO is not null
              ) table5
                  left join ods.ods_pl_powerdes_bis_must2_prop_dt bis_must2_prop
                            on table5.bis_cont_id = bis_must2_prop.bis_cont_id
         where (year(table5.cont_start_date) - 1 + cast(bis_must2_prop.YEAR as int)) = '2021'
     ) table6
     on table2.bis_cont_id = table6.bis_cont_id
         left join
     (
         select t2.BIS_CONT_ID,
                t2.MYyear,    -- 年份
                t2.UNIT_MONEY -- 历史物管单价
         from (
                  select row_number() over (partition by t1.BIS_CONT_ID order by t1.MYyear desc ) rn,
                         t1.*
                  from (
                           select bis_must2_prop.bis_cont_id,
                                  (year(table5.cont_start_date) - 1 + cast(bis_must2_prop.YEAR as int)) as MYyear,
                                  bis_must2_prop.unit_money -- 历史物管单价
                           from (
                                    select bis_cont_id,
                                           cont_start_date,
                                           cont_end_date
                                    from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                                    where '2021' between year(bis_cont.cont_start_date) and year(
                                            if(bis_cont.status_cd = '2', bis_cont.cont_to_fail_date,
                                               bis_cont.cont_end_date))
                                      and bis_cont.cont_type_cd in ('1', '2')
                                      and bis_cont.effect_flg <> 'D'
                                      and bis_cont.CONT_NO is not null
                                ) table5
                                    left join ods.ods_pl_powerdes_bis_must2_prop_dt bis_must2_prop
                                              on table5.bis_cont_id = bis_must2_prop.bis_cont_id and
                                                 bis_must2_prop.unit_money is not null
                           where (year(table5.cont_start_date) - 1 + cast(bis_must2_prop.YEAR as int)) < '2021'
                       ) t1) t2
         where t2.rn = 1
     ) table9 on table2.bis_cont_id = table9.bis_cont_id

         left join
     (
         select t4.bis_store_id,                               -- 铺位id
                sum(t4.bis_store_owm_money) as store_own_money -- 铺位欠费
         from (
                  -- 铺位欠费
                  select t2.bis_cont_id,
                         t2.bis_store_id,
                         t1.total_own_money,                                                                   -- 合同欠费总金额
                         t3.total_area,                                                                        -- 合同所对应的所有铺位面积之和
                         bis_store.rent_square,                                                                -- 单个店铺面积
                         ((bis_store.rent_square / t3.total_area) * t1.total_own_money) as bis_store_owm_money -- 按照店铺面积拆分后的店铺欠费金额
                  from (
                           select bis_cont_id,
                                  sum(own_money) as total_own_money
                           from ods.ods_pl_powerdes_pms_fin_arrearage_dt pms_fin_arrearage
                           where FEE_TYPE in ('1', '2', '6', '76', '63', '71', '7', '8', '62', '72', '79', '88')
                           group by bis_cont_id
                       ) t1
                           left join
                       (
                           -- 铺位打散
                           SELECT tmp.bis_cont_id,
                                  tmp.bis_store_ids,
                                  bis_store_id
                           FROM (
                                    SELECT bis_cont_id,
                                           bis_store_ids
                                    FROM ods.ods_pl_powerdes_bis_cont_dt
                                ) tmp LATERAL VIEW explode(split(bis_store_ids, ',')) tmp1 AS bis_store_id
                       ) t2
                       on t1.bis_cont_id = t2.bis_cont_id
                           left join ods.ods_pl_powerdes_bis_store_dt bis_store
                                     on bis_store.bis_store_id = t2.bis_store_id
                           left join
                       (
                           select t4.bis_cont_id,
                                  sum(bis_store.rent_square) total_area -- 合同下所有铺位面积之和
                           from (
                                    -- 铺位打散
                                    SELECT tmp.bis_cont_id,
                                           bis_store_id
                                    FROM (
                                             SELECT bis_cont_id,
                                                    bis_store_ids
                                             FROM ods.ods_pl_powerdes_bis_cont_dt
                                         ) tmp LATERAL VIEW explode(split(bis_store_ids, ',')) tmp1 AS bis_store_id
                                ) t4
                                    left join ods.ods_pl_powerdes_bis_store_dt bis_store
                                              on bis_store.bis_store_id = t4.bis_store_id
                           group by t4.bis_cont_id
                       ) t3 on t1.bis_cont_id = t3.bis_cont_id
              ) t4
         group by t4.bis_store_id
     ) as table3
     on bis_store.bis_store_id = table3.bis_store_id

         LEFT JOIN ods.ods_pl_powerdes_bis_project_dt bis_project
                   on bis_store.bis_project_id = bis_project.bis_project_id
         left join
     (
         select table1.bis_cont_id,
                -- table1.must_type, -- 1:租金  2 物管
                MAX(table1.rent_money_01)   AS rent_money_01,
                MAX(table1.rent_money_2)    AS rent_money_2,
                MAX(table1.rent_money_3)    AS rent_money_3,
                MAX(table1.rent_money_4)    AS rent_money_4,
                MAX(table1.rent_money_5)    AS rent_money_5,
                MAX(table1.rent_money_6)    AS rent_money_6,
                MAX(table1.rent_money_7)    AS rent_money_7,
                MAX(table1.rent_money_8)    AS rent_money_8,
                MAX(table1.rent_money_9)    AS rent_money_9,
                MAX(table1.rent_money_10)   AS rent_money_10,
                MAX(table1.rent_money_11)   AS rent_money_11,
                MAX(table1.rent_money_12)   AS rent_money_12,
                (MAX(table1.rent_money_01) + MAX(table1.rent_money_2) + MAX(table1.rent_money_3) +
                 MAX(table1.rent_money_4) +
                 MAX(table1.rent_money_5)
                    + MAX(table1.rent_money_6) + MAX(table1.rent_money_7) + MAX(table1.rent_money_8) +
                 MAX(table1.rent_money_9)
                    + MAX(table1.rent_money_10) + MAX(table1.rent_money_11) +
                 MAX(table1.rent_money_12)) as rent_total_money,

                MAX(table1.mgr_money_01)    AS mgr_money_01,
                MAX(table1.mgr_money_02)    AS mgr_money_2,
                MAX(table1.mgr_money_03)    AS mgr_money_3,
                MAX(table1.mgr_money_04)    AS mgr_money_4,
                MAX(table1.mgr_money_05)    AS mgr_money_5,
                MAX(table1.mgr_money_06)    AS mgr_money_6,
                MAX(table1.mgr_money_07)    AS mgr_money_7,
                MAX(table1.mgr_money_08)    AS mgr_money_8,
                MAX(table1.mgr_money_09)    AS mgr_money_9,
                MAX(table1.mgr_money_10)    AS mgr_money_10,
                MAX(table1.mgr_money_11)    AS mgr_money_11,
                MAX(table1.mgr_money_12)    AS mgr_money_12,
                (MAX(table1.mgr_money_01) + MAX(table1.mgr_money_02) + MAX(table1.mgr_money_03) +
                 MAX(table1.mgr_money_04) +
                 MAX(table1.mgr_money_05)
                    + MAX(table1.mgr_money_06) + MAX(table1.mgr_money_07) + MAX(table1.mgr_money_08) +
                 MAX(table1.mgr_money_09)
                    + MAX(table1.mgr_money_10) + MAX(table1.mgr_money_11) +
                 MAX(table1.mgr_money_12))  as mgr_total_money

         from (
                  select BIS_CONT_ID,
                         must_type,
                         case
                             when MUST_TYPE = '1' and QZ_YEAR_MONTH = '2021-01' then MONEY
                             else null end as rent_money_01,
                         case
                             when MUST_TYPE = '1' and QZ_YEAR_MONTH = '2021-02' then MONEY
                             else null end as rent_money_2,
                         case
                             when MUST_TYPE = '1' and QZ_YEAR_MONTH = '2021-03' then MONEY
                             else null end as rent_money_3,
                         case
                             when MUST_TYPE = '1' and QZ_YEAR_MONTH = '2021-04' then MONEY
                             else null end as rent_money_4,
                         case
                             when MUST_TYPE = '1' and QZ_YEAR_MONTH = '2021-05' then MONEY
                             else null end as rent_money_5,
                         case
                             when MUST_TYPE = '1' and QZ_YEAR_MONTH = '2021-06' then MONEY
                             else null end as rent_money_6,
                         case
                             when MUST_TYPE = '1' and QZ_YEAR_MONTH = '2021-07' then MONEY
                             else null end as rent_money_7,
                         case
                             when MUST_TYPE = '1' and QZ_YEAR_MONTH = '2021-08' then MONEY
                             else null end as rent_money_8,
                         case
                             when MUST_TYPE = '1' and QZ_YEAR_MONTH = '2021-09' then MONEY
                             else null end as rent_money_9,
                         case
                             when MUST_TYPE = '1' and QZ_YEAR_MONTH = '2021-10' then MONEY
                             else null end as rent_money_10,
                         case
                             when MUST_TYPE = '1' and QZ_YEAR_MONTH = '2021-11' then MONEY
                             else null end as rent_money_11,
                         case
                             when MUST_TYPE = '1' and QZ_YEAR_MONTH = '2021-12' then MONEY
                             else null end as rent_money_12,
                         case
                             when MUST_TYPE = '2' and QZ_YEAR_MONTH = '2021-01' then MONEY
                             else null end as mgr_money_01,
                         case
                             when MUST_TYPE = '2' and QZ_YEAR_MONTH = '2021-02' then MONEY
                             else null end as mgr_money_02,
                         case
                             when MUST_TYPE = '2' and QZ_YEAR_MONTH = '2021-03' then MONEY
                             else null end as mgr_money_03,
                         case
                             when MUST_TYPE = '2' and QZ_YEAR_MONTH = '2021-04' then MONEY
                             else null end as mgr_money_04,
                         case
                             when MUST_TYPE = '2' and QZ_YEAR_MONTH = '2021-05' then MONEY
                             else null end as mgr_money_05,
                         case
                             when MUST_TYPE = '2' and QZ_YEAR_MONTH = '2021-06' then MONEY
                             else null end as mgr_money_06,
                         case
                             when MUST_TYPE = '2' and QZ_YEAR_MONTH = '2021-07' then MONEY
                             else null end as mgr_money_07,
                         case
                             when MUST_TYPE = '2' and QZ_YEAR_MONTH = '2021-08' then MONEY
                             else null end as mgr_money_08,
                         case
                             when MUST_TYPE = '2' and QZ_YEAR_MONTH = '2021-09' then MONEY
                             else null end as mgr_money_09,
                         case
                             when MUST_TYPE = '2' and QZ_YEAR_MONTH = '2021-10' then MONEY
                             else null end as mgr_money_10,
                         case
                             when MUST_TYPE = '2' and QZ_YEAR_MONTH = '2021-11' then MONEY
                             else null end as mgr_money_11,
                         case
                             when MUST_TYPE = '2' and QZ_YEAR_MONTH = '2021-12' then MONEY
                             else null end as mgr_money_12
                  from ods.ods_pl_powerdes_bis_must2_dt bis_must2
                  where substr(bis_must2.QZ_YEAR_MONTH, 0, 4) = '2021'
                    and MUST_TYPE in ('1', '2')
              ) table1
         group by table1.bis_cont_id
     ) table4 on table2.bis_cont_id = table4.bis_cont_id
where bis_store.is_delete = 'N'
  and bis_store.status_cd = '1'
  and (year(bis_store.effect_date) - 1 + cast(bis_store_price.seq_year as int)) = '2021'


union all

select bis_project.mall_id,                                                                                  -- 项目id
       bis_multi.bis_multi_id,                                                                               -- 多经id
       null                                                                        as unit_number,           -- 单元编号
       bis_multi.multi_name,                                                                                 -- 多经编号
       bis_floor.building_num,                                                                               -- 楼栋编号
       bis_floor.floor_num,                                                                                  -- 楼层编号
       bis_floor.charge_type,                                                                                -- 物业类型
       bis_multi.multi_charge_type                                                 as multi_type,            -- 多经类型
       '2'                                                                         as store_type,            -- 铺位类型（1：铺位 2：多经）
       table1.cont_count,                                                                                    -- 期间合同数量
       CAST(bis_multi.multi_price AS DOUBLE),                                                                -- 标准租金
       bis_multi.multi_price_holidays                                              as price_holidays,        -- 节假日租金
       bis_multi.multi_price_weekend                                               as price_weekend,         -- 周末租金
       null                                                                        as mgr_price,             -- 标准管理费
       cast(table2.multi_rent as double)                                           as rent_price,            -- 租金单价
       null                                                                        as history_unit_money,    -- 历史租金单价
       (cast(table2.multi_rent as double) / bis_multi.multi_price)                 as achieving_rate,        -- 达成率
       bis_multi.SQUARE,                                                                                     -- 面积
       null                                                                        as rank,                  -- 等级
       if(table2.bis_cont_id is null, table2.proportion_names, bis_multi.charge_type),                       -- 铺位业态
       table2.PROPORTION_NAMES,                                                                              -- 合同业态
       table2.PROPORTION_IDS,                                                                                -- 合同业态ids
       null                                                                        as property_right,        -- 资产属性
       null                                                                        as LAST_DATE,             -- 返租到期日
       case

           when '2021-01-01' between table2.cont_start_date and if(table2.status_cd = '2', table2.cont_to_fail_date,
                                                                   table2.cont_end_date)
               then '2'
           when table2.bis_cont_id is null then '1' end                            as Initial_state,         -- 期初状态
       table2.bis_cont_id,                                                                                   -- 合同id
       null                                                                        as is_adjust,             -- 是否调铺（1：是 0：否）
       null                                                                        as cont_type,             -- 合同类型
       table2.cont_no,                                                                                       -- 合同号
       table2.cont_start_date,                                                                               -- 合同开始日期
       table2.cont_end_date,                                                                                 -- 合同结束日期
       CASE
           WHEN table2.cont_type_cd = 3 THEN bis_more_manage_leasee.leasee_name
           WHEN table2.cont_type_cd = 2 AND bis_shop_conn.part_name IS NULL THEN bis_more_manage_leasee.leasee_name
           WHEN table2.cont_type_cd = 2 AND bis_more_manage_leasee.leasee_name IS NULL THEN bis_shop_conn.part_name
           ELSE bis_shop_conn.part_name END                                        AS companyName,           -- 商戶
       CASE
           WHEN table2.cont_type_cd = 3 THEN bis_more_manage_leasee.more_manage_leasee_id
           WHEN table2.cont_type_cd = 2 AND bis_shop_conn.part_name IS NULL
               THEN bis_more_manage_leasee.more_manage_leasee_id
           WHEN table2.cont_type_cd = 2 AND bis_more_manage_leasee.leasee_name IS NULL
               THEN bis_shop_conn.bis_shop_conn_id
           ELSE bis_shop_conn.bis_shop_conn_id END                                 AS companyNameID,         -- 商戶id
       table2.pay_way,                                                                                       -- 計租方式
       '0'                                                                         as pay_cycle_cd1,         -- 固定支付週期
       '0'                                                                         as pay_cycle_cd2,         -- 扣點支付週期
       table7.ROYALTY_RATIO                                                        as buckle_point,          -- 扣点
       null                                                                        as estimated_sales,       -- 预估销售额
       null                                                                        as sales_type,            -- 销售额方式（1含税2不含税）
       null                                                                        as increasing_rate,       -- 租金递增率
       null                                                                        as future_income,         -- 未来两年的收入
       table2.pay_way_prop,                                                                                  -- 物管支付方式
       table6.unit_money                                                           as unit_price_wg,         -- 物管单价
       table9.unit_money                                                           as history_unit_price_wg, -- 历史物管单价
       round((table6.unit_money - table9.unit_money) / table9.unit_money, 2) * 100 as increasing_rate_wg,    -- 物管递增率
       null                                                                        as future_income_wg,      -- 未来2年物管费收入
       table7.free_rent_period,                                                                              -- 经营免租期
       table3.total_own_money,                                                                               -- 欠费
       datediff(if(table2.cont_end_date >= '2021-12-31', '2021-12-31', table2.cont_end_date),
                if(table2.cont_start_date <= '2021-01-01', '2021-01-01', table2.cont_start_date)) +
       1                                                                           as lease_term,            -- 有效租期
       table4.rent_total_money                                                     as zj_income,             -- 租金收入
       table4.mgr_total_money                                                      as wg_income,             -- 物管收入
       null                                                                        as total_income,          -- 综合收入
       table4.rent_money_01                                                        as jan_zj,                -- 一月份租金
       table4.rent_money_2                                                         as feb_zj,                -- 二月份租金
       table4.rent_money_3                                                         as march_zj,              -- 三月份租金
       table4.rent_money_4                                                         as april_zj,              -- 四月份租金
       table4.rent_money_5                                                         as may_zj,                -- 五月份租金
       table4.rent_money_6                                                         as june_zj,               -- 六月份租金
       table4.rent_money_7                                                         as july_zj,               -- 七月份租金
       table4.rent_money_8                                                         as aug_zj,                -- 八月份租金
       table4.rent_money_9                                                         as sep_zj,                -- 九月份租金
       table4.rent_money_10                                                        as oct_zj,                -- 十月份租金
       table4.rent_money_11                                                        as nov_zj,                -- 十一月份租金
       table4.rent_money_12                                                        as dece_zj,               -- 十二月份租金
       table4.mgr_money_01                                                         as jan_wg,                -- 一月份物管
       table4.mgr_money_2                                                          as feb_wg,                -- 二月份物管
       table4.mgr_money_3                                                          as march_wg,              -- 三月份物管
       table4.mgr_money_4                                                          as april_wg,              -- 四月份物管
       table4.mgr_money_5                                                          as may_wg,                -- 五月份物管
       table4.mgr_money_6                                                          as june_wg,               -- 六月份物管
       table4.mgr_money_7                                                          as july_wg,               -- 七月份物管
       table4.mgr_money_8                                                          as aug_wg,                -- 八月份物管
       table4.mgr_money_9                                                          as sep_wg,                -- 九月份物管
       table4.mgr_money_10                                                         as oct_wg,                -- 十月份物管
       table4.mgr_money_11                                                         as nov_wg,                -- 十一月份物管
       table4.mgr_money_12                                                         as dece_wg,               -- 十二月份物管
       bis_floor.bis_floor_id,                                                                               -- 楼层id
       table2.bis_shop_id,                                                                                   -- 品牌id
       table2.bis_shop_name,                                                                                 -- 品牌名称
       '0'                                                                         as is_delete,             -- 多经是否删除(0:否 1：是)
       bis_multi.updated_date,                                                                               -- 多径更新时间
       bis_multi.updator,                                                                                    -- 多径更新人员
       bis_multi.created_date,                                                                               -- 多径创建时间
       bis_multi.creator,                                                                                    -- 多径创建人
       "oracle"                                                                    as source                 -- 数据来源

from ods.ods_pl_powerdes_bis_multi_dt bis_multi

         LEFT JOIN
     (
         SELECT max(bis_store_id) as                         store_id,     -- 多经id
                concat_ws(',', collect_set(tmp.bis_cont_id)) bis_cont_ids, -- 多经对应的多个合同id
                count(tmp.bis_cont_id)                       cont_count    -- 多经所对应的合同数量
         FROM (
                  SELECT bis_cont_id,
                         bis_store_ids
                  FROM ods.ods_pl_powerdes_bis_cont_dt
                       -- 2021 需要动态传值
                  where '2021' between year(cont_start_date) and year(if(status_cd = '2', cont_to_fail_date, cont_end_date))
                    and cont_type_cd in ('3', '4')
                    and effect_flg <> 'D'
                    and CONT_NO is not null
              ) tmp LATERAL VIEW explode(split(bis_store_ids, ',')) tmp1 AS bis_store_id
         group by bis_store_id
     ) table1
     on bis_multi.bis_multi_id = table1.store_id
         left join ods.ods_pl_powerdes_bis_floor_dt bis_floor
                   on bis_multi.bis_floor_id = bis_floor.bis_floor_id
         left join
     (
         -- 店铺打散
         SELECT tmp.bis_cont_id,
                tmp.bis_store_ids,
                bis_store_id,
                status_cd,
                cont_end_date,
                cont_start_date,
                cont_to_fail_date,
                cont_no,
                cont_type_cd,
                bis_shop_conn_id,
                pay_way,
                pay_cycle_cd,
                pay_way_prop,
                PROPORTION_NAMES,
                PROPORTION_IDS,
                bis_shop_id,
                bis_shop_name,
                multi_rent


         FROM (
                  SELECT bis_cont_id,
                         bis_store_ids,
                         status_cd,
                         cont_end_date,
                         cont_start_date,
                         cont_to_fail_date,
                         cont_no,
                         cont_type_cd,
                         bis_shop_conn_id,
                         pay_way,
                         pay_cycle_cd,
                         pay_way_prop,
                         PROPORTION_NAMES,
                         PROPORTION_IDS,
                         bis_shop_id,
                         bis_shop_name,
                         multi_rent


                  FROM ods.ods_pl_powerdes_bis_cont_dt
                  where '2021' between year(cont_start_date) and year(if(status_cd = '2', cont_to_fail_date, cont_end_date))
                    and cont_type_cd in ('3', '4')
                    and effect_flg <> 'D'
                    and CONT_NO is not null
              ) tmp LATERAL VIEW explode(split(bis_store_ids, ',')) tmp1 AS bis_store_id
     ) table2 on table2.bis_store_id = bis_multi.bis_multi_id
         left join
     (
         select bis_must_rent.unit_money,
                bis_must_rent.bis_cont_id,
                bis_must_rent.free_rent_period,
                bis_must_rent.ROYALTY_RATIO
         from (
                  select bis_cont_id,
                         cont_start_date,
                         cont_end_date
                  from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                  where '2021' between year(bis_cont.cont_start_date) and year(
                          if(bis_cont.status_cd = '2', bis_cont.cont_to_fail_date, bis_cont.cont_end_date))
                    and bis_cont.cont_type_cd in ('3', '4')
                    and bis_cont.effect_flg <> 'D'
                    and bis_cont.CONT_NO is not null
              ) table5
                  left join ods.ods_pl_powerdes_bis_must_rent_dt bis_must_rent
                            on table5.bis_cont_id = bis_must_rent.bis_cont_id
         where (year(table5.cont_start_date) - 1 + cast(bis_must_rent.YEAR as int)) = '2021'
     ) table7
     on table2.bis_cont_id = table7.bis_cont_id


         left join ods.ods_pl_powerdes_bis_shop_open_info_dt bis_shop_open_info
                   on table2.bis_cont_id = bis_shop_open_info.bis_cont_id
         LEFT JOIN ods.ods_pl_powerdes_bis_more_manage_leasee_dt bis_more_manage_leasee
                   ON table2.bis_cont_id = bis_more_manage_leasee.bis_cont_id
         LEFT JOIN ods.ods_pl_powerdes_bis_shop_conn_dt bis_shop_conn
                   ON table2.bis_shop_conn_id = bis_shop_conn.bis_shop_conn_id
         left join
     (
         select bis_must2_prop.unit_money,
                bis_must2_prop.bis_cont_id
         from (
                  select bis_cont_id,
                         cont_start_date,
                         cont_end_date
                  from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                  where '2021' between year(bis_cont.cont_start_date) and year(
                          if(bis_cont.status_cd = '2', bis_cont.cont_to_fail_date, bis_cont.cont_end_date))
                    and bis_cont.cont_type_cd in ('3', '4')
                    and bis_cont.effect_flg <> 'D'
                    and bis_cont.CONT_NO is not null
              ) table5
                  left join ods.ods_pl_powerdes_bis_must2_prop_dt bis_must2_prop
                            on table5.bis_cont_id = bis_must2_prop.bis_cont_id
         where (year(table5.cont_start_date) - 1 + cast(bis_must2_prop.YEAR as int)) = '2021'
     ) table6
     on table2.bis_cont_id = table6.bis_cont_id
         left join
     (
         select t2.BIS_CONT_ID,
                t2.MYyear,    -- 年份
                t2.UNIT_MONEY -- 历史物管单价
         from (
                  select row_number() over (partition by t1.BIS_CONT_ID order by t1.MYyear desc ) rn,
                         t1.*
                  from (
                           select bis_must2_prop.bis_cont_id,
                                  (year(table5.cont_start_date) - 1 + cast(bis_must2_prop.YEAR as int)) as MYyear,
                                  bis_must2_prop.unit_money -- 历史物管单价
                           from (
                                    select bis_cont_id,
                                           cont_start_date,
                                           cont_end_date
                                    from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                                    where '2021' between year(bis_cont.cont_start_date) and year(
                                            if(bis_cont.status_cd = '2', bis_cont.cont_to_fail_date,
                                               bis_cont.cont_end_date))
                                      and bis_cont.cont_type_cd in ('3', '4')
                                      and bis_cont.effect_flg <> 'D'
                                      and bis_cont.CONT_NO is not null
                                ) table5
                                    left join ods.ods_pl_powerdes_bis_must2_prop_dt bis_must2_prop
                                              on table5.bis_cont_id = bis_must2_prop.bis_cont_id and
                                                 bis_must2_prop.unit_money is not null
                           where (year(table5.cont_start_date) - 1 + cast(bis_must2_prop.YEAR as int)) < '2021'
                       ) t1) t2
         where t2.rn = 1
     ) table9 on table2.bis_cont_id = table9.bis_cont_id
         left join
     (
         select bis_cont_id,
                sum(own_money) as total_own_money
         from ods.ods_pl_powerdes_pms_fin_arrearage_dt pms_fin_arrearage
         group by bis_cont_id
     ) as table3
     on table2.bis_cont_id = table3.bis_cont_id
         LEFT JOIN ods.ods_pl_powerdes_bis_project_dt bis_project
                   on bis_multi.bis_project_id = bis_project.bis_project_id
         left join
     (
         select table1.bis_cont_id,
                max(table1.rent_money_01)   AS rent_money_01,
                max(table1.rent_money_2)    AS rent_money_2,
                max(table1.rent_money_3)    AS rent_money_3,
                max(table1.rent_money_4)    AS rent_money_4,
                max(table1.rent_money_5)    AS rent_money_5,
                max(table1.rent_money_6)    AS rent_money_6,
                max(table1.rent_money_7)    AS rent_money_7,
                max(table1.rent_money_8)    AS rent_money_8,
                max(table1.rent_money_9)    AS rent_money_9,
                max(table1.rent_money_10)   AS rent_money_10,
                max(table1.rent_money_11)   AS rent_money_11,
                max(table1.rent_money_12)   AS rent_money_12,
                (max(table1.rent_money_01) +
                 max(table1.rent_money_2) +
                 max(table1.rent_money_3) +
                 max(table1.rent_money_4) +
                 max(table1.rent_money_5) +
                 max(table1.rent_money_6) +
                 max(table1.rent_money_7) +
                 max(table1.rent_money_8) +
                 max(table1.rent_money_9) +
                 max(table1.rent_money_10) +
                 max(table1.rent_money_11) +
                 max(table1.rent_money_12)) as rent_total_money,

                null                        AS mgr_money_01,
                null                        AS mgr_money_2,
                null                        AS mgr_money_3,
                null                        AS mgr_money_4,
                null                        AS mgr_money_5,
                null                        AS mgr_money_6,
                null                        AS mgr_money_7,
                null                        AS mgr_money_8,
                null                        AS mgr_money_9,
                null                        AS mgr_money_10,
                null                        AS mgr_money_11,
                null                        AS mgr_money_12,
                null                        as mgr_total_money

         from (
                  select BIS_CONT_ID,
                         case
                             when QZ_YEAR_MONTH = '2021-01' then sum(MONEY)
                             else null end as rent_money_01,
                         case
                             when QZ_YEAR_MONTH = '2021-02' then sum(MONEY)
                             else null end as rent_money_2,
                         case
                             when QZ_YEAR_MONTH = '2021-03' then sum(MONEY)
                             else null end as rent_money_3,
                         case
                             when QZ_YEAR_MONTH = '2021-04' then sum(MONEY)
                             else null end as rent_money_4,
                         case
                             when QZ_YEAR_MONTH = '2021-05' then sum(MONEY)
                             else null end as rent_money_5,
                         case
                             when QZ_YEAR_MONTH = '2021-06' then sum(MONEY)
                             else null end as rent_money_6,
                         case
                             when QZ_YEAR_MONTH = '2021-07' then sum(MONEY)
                             else null end as rent_money_7,
                         case
                             when QZ_YEAR_MONTH = '2021-08' then sum(MONEY)
                             else null end as rent_money_8,
                         case
                             when QZ_YEAR_MONTH = '2021-09' then sum(MONEY)
                             else null end as rent_money_9,
                         case
                             when QZ_YEAR_MONTH = '2021-10' then sum(MONEY)
                             else null end as rent_money_10,
                         case
                             when QZ_YEAR_MONTH = '2021-11' then sum(MONEY)
                             else null end as rent_money_11,
                         case
                             when QZ_YEAR_MONTH = '2021-12' then sum(MONEY)
                             else null end as rent_money_12
                  from ods.ods_pl_powerdes_bis_must2_dt bis_must2
                  where substr(bis_must2.QZ_YEAR_MONTH, 0, 4) = '2021'
                    and MUST_TYPE in ('34', '57')
                  group by bis_cont_id, QZ_YEAR_MONTH
              ) table1
         group by table1.bis_cont_id
     ) table4 on table2.bis_cont_id = table4.bis_cont_id
where bis_multi.is_delete <> '1'
  and bis_multi.multi_name not like 'ZZ%';
