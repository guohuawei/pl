/*
    合同租费规则:
    dwd_bis_rent_rules_big
*/
SELECT bis_must_rent.BIS_MUST_RENT_ID,                                                                       -- 租金应收id
       bis_cont.BIS_CONT_ID,                                                                                 -- 合同code
       bis_cont.CONT_NO,                                                                                     -- 合同NO
       bis_must_rent.RENT_type,                                                                              -- 租金方式
       COUNT_MONEY                                      as                              MIN_SALE,            -- 最低销售额
       bis_must_rent.ROYALTY_RATIO,                                                                          -- 扣率
       (table1.total_rent_price / bis_cont.rent_square) as                              STANDARD_UNIT_MONEY, -- 标准单价 -- 通过一铺一价取,多个铺位取平均
       bis_must_rent.UNIT_MONEY,                                                                             -- 合同租金单价
       add_months(bis_cont.rent_start_date, 12 * (cast(bis_must_rent.YEAR as int) - 1)),                     -- 计租开始时间
       case
           WHEN
                       add_months(bis_cont.rent_start_date, 12 * (cast(bis_must_rent.YEAR as int))) - 1 <
                       bis_cont.CONT_END_DATE
               THEN add_months(bis_cont.rent_start_date, 12 * (cast(bis_must_rent.YEAR as int))) - 1
           else bis_cont.CONT_END_DATE END              as                              RENT_END_DATE,       -- 计租结束时间
       null                                             as                              IS_SUB_SHOP,         -- 是否分铺
       null                                             as                              Tax_amount,          -- 税额
       null                                             as                              tax_rate,            -- 税率
       '1'                                                                              FEE_TYPE,            -- 费项 1租金2物管
       NULL                                             as                              is_First_issue,      -- 是否首期
       null                                             as                              Latest_Date,         -- 最迟交费日
       null                                             as                              payment_type,        -- 支付类型
       bis_cont.pay_Cycle_Cd,                                                                                -- 支付周期
       null                                             as                              Points_Type,         -- 扣点类型 1,品类, 2阶梯
       add_months(bis_cont.rent_start_date, 12 * (cast(bis_must_rent.YEAR as int) - 1)) start_rent_free,     -- 免租期开始
       add_months(bis_cont.rent_start_date,
                  cast(nvl(REGEXP_REPLACE(free_rent_period, '[^0-9]', ''), 0) as int))  end_rent_free,       -- 免租期结束
       case
           when bis_cont.EFFECT_FLG = 'D' THEN 'Y'
           ELSE 'N' END                                 AS                              IS_DEL,              -- 是否删除
       bis_must_rent.updated_date,                                                                           -- 更新时间
       bis_must_rent.updator,                                                                                -- 更新人员
       bis_must_rent.created_date,                                                                           -- 创建时间
       bis_must_rent.creator,                                                                                -- 创建人
       'oracle'                                         as                              source               -- 数据来源（oracle,mysql,http）
FROM ods.ods_pl_powerdes_bis_must_rent_dt bis_must_rent
         LEFT JOIN ods.ods_pl_powerdes_bis_cont_dt bis_cont
                   ON bis_cont.BIS_CONT_ID = bis_must_rent.BIS_CONT_ID
         left join
     (
         select bis_cont.bis_cont_id,
                sum(bis_store_price.rent_price * bis_store.rent_square) as total_rent_price
         from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                  left join
              (
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
              on bis_cont.bis_cont_id = bis_cont_bis_store_ids.bis_cont_id
                  left join ods.ods_pl_powerdes_bis_store_price_dt bis_store_price
                            on bis_store_price.bis_store_id = bis_cont_bis_store_ids.bis_store_id
                  left join ods.ods_pl_powerdes_bis_store_dt bis_store
                            on bis_store.bis_store_id = bis_store_price.bis_store_id
         where bis_cont.bis_cont_id = '402834e533ac88bd0133af81621a0758'
         group by bis_cont.bis_cont_id
     ) table1
     on table1.bis_cont_id = bis_cont.bis_cont_id

union all

SELECT bis_must2_prop.BIS_MUST_PROP_ID                                                 BIS_MUST_RENT_ID,
       bis_cont.BIS_CONT_ID,                                                                                -- 合同code
       bis_cont.CONT_NO,                                                                                    -- 合同NO
       bis_must2_prop.PROP_TYPE                                                        RENT_type,           -- 租金方式
       bis_must2_prop.COUNT_MONEY                                                      MIN_SALE,            -- 最低销售额
       null                                                                            ROYALTY_RATIO,       -- 扣率
       (table1.total_mgr_price / bis_cont.rent_square) as                              STANDARD_UNIT_MONEY, -- 标准单价 -- 通过一铺一价取,多个铺位取平均
       bis_must2_prop.UNIT_MONEY,                                                                           -- 申请租金单价
       add_months(bis_cont.PRMGR_DATE, 12 * (cast(bis_must2_prop.YEAR as int) - 1)),                        -- 计租开始时间
       case
           WHEN add_months(bis_cont.PRMGR_DATE, 12 * (cast(bis_must2_prop.YEAR as int))) - 1 < bis_cont.CONT_END_DATE
               THEN add_months(bis_cont.PRMGR_DATE, 12 * (cast(bis_must2_prop.YEAR as int))) - 1
           else bis_cont.CONT_END_DATE END             as                              RENT_END_DATE
        ,                                                                                                   -- 计租结束时间
       null                                                                            IS_SUB_SHOP,         -- 是否分铺
       null                                                                            Tax_amount,          -- 税额
       null                                                                            tax_rate,            -- 税率
       '2'                                                                             FEE_TYPE,            -- 费项 1租金2物管
       NULL                                                                            is_First_issue,      -- 是否首期
       null                                                                            Latest_Date,         -- 最迟交费日
       null                                                                            payment_type,        -- 支付类型
       bis_cont.pay_Cycle_Cd,                                                                               -- 支付周期
       null                                                                            Points_Type,         -- 扣点类型 1,品类, 2阶梯
       add_months(bis_cont.PRMGR_DATE, 12 * (cast(bis_must2_prop.YEAR as int) - 1))    start_rent_free,     -- 免租期开始
       add_months(bis_cont.PRMGR_DATE,
                  cast(nvl(REGEXP_REPLACE(free_rent_period, '[^0-9]', ''), 0) as int)) end_rent_free,       -- 免租期结束
       case
           when bis_cont.EFFECT_FLG = 'D' THEN 'Y'
           ELSE 'N' END                                AS                              IS_DEL,              -- 是否删除
       bis_must2_prop.updated_date,                                                                         -- 更新时间
       bis_must2_prop.updator,                                                                              -- 更新人员
       bis_must2_prop.created_date,                                                                         -- 创建时间
       bis_must2_prop.creator,                                                                              -- 创建人
       'oracle'                                        as                              source               -- 数据来源（oracle,mysql,http）
FROM ods.ods_pl_powerdes_bis_must2_prop_dt bis_must2_prop
         LEFT JOIN ods.ods_pl_powerdes_bis_cont_dt bis_cont ON bis_cont.BIS_CONT_ID = bis_must2_prop.BIS_CONT_ID
         left join
     (
         select bis_cont.bis_cont_id,
                sum(bis_store_price.mgr_price * bis_store.rent_square) as total_mgr_price
         from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                  left join
              (
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
              on bis_cont.bis_cont_id = bis_cont_bis_store_ids.bis_cont_id
                  left join ods.ods_pl_powerdes_bis_store_price_dt bis_store_price
                            on bis_store_price.bis_store_id = bis_cont_bis_store_ids.bis_store_id
                  left join ods.ods_pl_powerdes_bis_store_dt bis_store
                            on bis_store.bis_store_id = bis_store_price.bis_store_id
         group by bis_cont.bis_cont_id
     ) table1
     on table1.bis_cont_id = bis_cont.bis_cont_id;



