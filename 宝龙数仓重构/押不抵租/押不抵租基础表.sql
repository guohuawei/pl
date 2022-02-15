insert OVERWRITE table dwd.dwd_bis_cover_rent_big_dt partition (dt = '${hiveconf:nowdate}')
select table3.area_name,      -- 区域名称
       table3.id,             -- 区域id
       table1.bis_project_id, -- 项目id
       table1.short_name,     -- 项目名称
       table1.bis_cont_id,    -- 合同id
       table1.BIS_SHOP_ID,    -- 商家id(品牌)
       table1.NAME_CN,        -- 商家名称
       table1.store_type,     -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
       table1.rent_square,    -- 计租面积
       table2.blance,         -- 余额(租金履约保证金余额 + 综合管理履约保证金余额)
       table5.owe_qz,         -- 权责欠费（租金+物管）
       table5.owe_cont,       -- 合同欠费（租金+物管)
       table2.zj_bz_money,    -- 租金履约保证金余额
       table2.zh_bz_money,    -- 综合管理履约保证金余额
       table5.rent_owe_qz,    -- 租金权责欠费
       table5.mgr_owe_qz,     -- 物管权责欠费
       table5.rent_owe_cont,  -- 租金应收月(合同)欠费
       table5.mgr_owe_cont,   -- 物管应收月（合同）欠费
       table1.effect_flg,     -- 合同状态
       table1.IS_GUARANTEE,   -- 合同是否保函 0不是 1是
       table1.pay_way,        -- 租金方式(1、固定租金；2、提成租金；3、两者取高；4、其他)
       table1.year_month1,    -- 年月
       table1.cont_no         -- 合同号
from
    -- 合同表
    (
        select t.year_month1,
               t2.*
        from (
                 select distinct bis_project_id,
                                 short_name,
                                 year,
                                 month,
                                 year_month1,
                                 NAME_CN,
                                 bis_shop_id,
                                 bis_cont_id,
                                 store_type

                 from dwd.dwd_bis_cross_join_result_big_dt
                 where dt = date_format(current_date(), 'yyyyMMdd')
                   and year between '2019' and year(current_date())
                 --and bis_project_id = '16084DA3912DDAE4E050007F01005603'
                 --and year_month1 like '2021%'
                 --and bis_cont_id = '8a7b868b726ff6490172744a8d217e9f'
             ) t
                 left join
             (
                 select BIS_CONT.bis_project_id,
                        BIS_CONT.bis_cont_id,
                        bis_cont.IS_GUARANTEE,                             -- 是否保函 0不是 1是
                        bis_cont.pay_way,                                  -- 租金方式(1、固定租金；2、提成租金；3、两者取高；4、其他)
                        bis_cont.cont_no,
                        bis_cont.effect_flg,
                        BIS_CONT.bis_shop_id,
                        bis_cont.rent_square                   rent_square,
                        bis_cont.STORE_TYPE                    STORE_TYPE, -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                        case
                            when bis_cont.bis_shop_name is null then bis_shop.NAME_CN
                            else bis_cont.bis_shop_name end as NAME_CN,    -- 结果表取合同表中品牌名称，如合同表中品牌名称为空取品牌表中名称，  如全部为空  及为空
                        bis_project.short_name
                 from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                          left join ods.ods_pl_powerdes_bis_project_dt bis_project
                                    on BIS_CONT.BIS_PROJECT_ID = BIS_PROJECT.BIS_PROJECT_ID and
                                       bis_project.is_business_project = '1'
                                        and bis_project.oper_status = '2'
                          left join ods.ods_pl_powerdes_bis_shop_dt bis_shop
                                    on BIS_CONT.BIS_SHOP_ID = BIS_SHOP.BIS_SHOP_ID
             ) t2 on t.bis_project_id = t2.bis_project_id and t.bis_cont_id = t2.bis_cont_id and
                     t.bis_shop_id = t2.bis_shop_id
        where t2.bis_project_id is not null
    ) table1
        left join
    (
        SELECT b.out_mall_id,                                                                 -- 项目id
               A.cont_no,                                                                     -- 合同号
               substr(A.created_date, 0, 7)                                      year_month,  -- 年月
               SUM(CASE WHEN A.SOURCE_TYPE = 7 THEN A.LEFT_AMOUNT ELSE 0 END) +
               SUM(CASE WHEN A.SOURCE_TYPE = 0 OR A.SOURCE_TYPE = 5 THEN A.LEFT_AMOUNT ELSE 0 END) -
               SUM(CASE WHEN A.SOURCE_TYPE = 1 THEN A.LEFT_AMOUNT ELSE 0 END) -
               SUM(CASE WHEN A.SOURCE_TYPE = 3 THEN A.LEFT_AMOUNT ELSE 0 END) -
               SUM(CASE WHEN A.SOURCE_TYPE = 4 THEN A.LEFT_AMOUNT ELSE 0 END) +
               SUM(CASE WHEN A.SOURCE_TYPE = 6 THEN A.LEFT_AMOUNT ELSE 0 END) as blance,      -- 余额(租金履约保证金余额 + 综合管理履约保证金余额)

               SUM(CASE WHEN a.FEE_TYPE = '13' and A.SOURCE_TYPE = 7 THEN A.LEFT_AMOUNT ELSE 0 END) +
               SUM(CASE
                       WHEN a.FEE_TYPE = '13' and (A.SOURCE_TYPE = 0 OR A.SOURCE_TYPE = 5) THEN A.LEFT_AMOUNT
                       ELSE 0 END) -
               SUM(CASE WHEN a.FEE_TYPE = '13' and A.SOURCE_TYPE = 1 THEN A.LEFT_AMOUNT ELSE 0 END) -
               SUM(CASE WHEN a.FEE_TYPE = '13' and A.SOURCE_TYPE = 3 THEN A.LEFT_AMOUNT ELSE 0 END) -
               SUM(CASE WHEN a.FEE_TYPE = '13' and A.SOURCE_TYPE = 4 THEN A.LEFT_AMOUNT ELSE 0 END) +
               SUM(CASE
                       WHEN a.FEE_TYPE = '13' and A.SOURCE_TYPE = 6 THEN A.LEFT_AMOUNT
                       ELSE 0 END)                                            as zj_bz_money, -- 余额(租金履约保证金余额)
               SUM(CASE WHEN a.FEE_TYPE = '14' and A.SOURCE_TYPE = 7 THEN A.LEFT_AMOUNT ELSE 0 END) +
               SUM(CASE
                       WHEN a.FEE_TYPE = '14' and (A.SOURCE_TYPE = 0 OR A.SOURCE_TYPE = 5) THEN A.LEFT_AMOUNT
                       ELSE 0 END) -
               SUM(CASE WHEN a.FEE_TYPE = '14' and A.SOURCE_TYPE = 1 THEN A.LEFT_AMOUNT ELSE 0 END) -
               SUM(CASE WHEN a.FEE_TYPE = '14' and A.SOURCE_TYPE = 3 THEN A.LEFT_AMOUNT ELSE 0 END) -
               SUM(CASE WHEN a.FEE_TYPE = '14' and A.SOURCE_TYPE = 4 THEN A.LEFT_AMOUNT ELSE 0 END) +
               SUM(CASE
                       WHEN a.FEE_TYPE = '14' and A.SOURCE_TYPE = 6 THEN A.LEFT_AMOUNT
                       ELSE 0 END)                                            as zh_bz_money  -- 余额(综合管理履约保证金余额)
        FROM ods.ods_pl_powerdes_pms_ib_company_keep_dt A
                 left join ods.ods_pl_powerdes_pms_bs_mall_dt b on A.MALL_ID = b.id
        WHERE A.IS_DEL = 0
          and b.is_del = 0
          and A.Fee_Type in (13, 14)
        group by b.out_mall_id, A.cont_no, substr(A.created_date, 0, 7)
    ) table2 on table1.bis_project_id = table2.out_mall_id and table1.cont_no = table2.cont_no and
                table1.year_month1 = table2.year_month

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
    ) table3 on table1.bis_project_id = table3.out_mall_id
        left join
    (
        select table4.bis_project_id,                                              -- 项目id
               table4.bis_shop_id,                                                 -- 商家id
               table4.bis_cont_id,                                                 -- 合同id
               table4.year_month1,                                                 -- 年月
               sum(table4.rent_owe_qz) + sum(table4.mgr_owe_qz)     owe_qz,        -- 权责欠费（租金+物管）
               sum(table4.rent_owe_cont) + sum(table4.mgr_owe_cont) owe_cont,      -- 合同欠费（租金+物管)
               sum(table4.rent_owe_qz)                              rent_owe_qz,   -- 租金权责欠费
               sum(table4.mgr_owe_qz)                               mgr_owe_qz,    -- 物管权责欠费
               sum(table4.rent_owe_cont)                            rent_owe_cont, -- 租金应收月欠费
               sum(table4.mgr_owe_cont)                             mgr_owe_cont   -- 物管应收月欠费
        from (
                 select t2.area_name,                                                             -- 区域名称
                        t2.id,                                                                    -- 区域id
                        t.bis_project_id,                                                         -- 项目id
                        t.short_name,                                                             -- 项目名称
                        t.year,                                                                   -- 年
                        t.month,                                                                  -- 月
                        t.year_month1,                                                            -- 年月 2021-12
                        t.bis_shop_id,                                                            -- 商家id
                        t.bis_cont_id,                                                            -- 合同id
                        t.name_cn,                                                                -- 商家名称
                        t.must_type,                                                              -- 费项（1：租金 2：物管）
                        t.store_type,                                                             -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                        case when t.must_type = '1' then t.owe_qz else 0 end    as rent_owe_qz,   -- 租金权责欠费
                        case when t.must_type = '1' then t1.owe_cont else 0 end as rent_owe_cont, -- 租金应收月欠费
                        case when t.must_type = '2' then t.owe_qz else 0 end    as mgr_owe_qz,    -- 物管权责欠费
                        case when t.must_type = '2' then t1.owe_cont else 0 end as mgr_owe_cont   -- 物管应收月欠费

                 from (
                          -- 权责欠费（权责月）
                          select t1.bis_project_id, -- 项目id
                                 t1.short_name,     -- 项目名称
                                 t1.year,           -- 年
                                 t1.month,          -- 月
                                 t1.year_month1,    -- 年月 2021-12
                                 t2.bis_shop_id,    -- 商家id
                                 t2.name_cn,        -- 商家名称
                                 t2.must_type,      -- 费项（1：租金 2：物管）
                                 t2.store_type,     -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                                 t2.owe_qz,         -- 欠费（权责月）
                                 t2.bis_cont_id     -- 合同id

                          from (
                                   select distinct bis_project.bis_project_id,
                                                   bis_project.short_name,
                                                   year,
                                                   month,
                                                   year_month1
                                   from dim.dim_pl_date,
                                        ods.ods_pl_powerdes_bis_project_dt bis_project
                                   where year between '2010' and '2030'
                                     and bis_project.is_business_project = '1'
                                     and oper_status = '2'
                               ) t1
                                   left join
                               (
                                   select table1.BIS_PROJECT_ID, -- 项目id
                                          t.PROJECT_NAME,        -- 项目名称
                                          table1.BIS_CONT_ID,    -- 合同id
                                          table1.STORE_TYPE,     -- 物业类型
                                          table1.BIS_SHOP_ID,    -- 商家id
                                          table1.NAME_CN,        -- 商家名称
                                          t.qz_year_month,       -- 权责月
                                          t.MUST_TYPE,           -- 费项
                                          t.oweMoney owe_qz      -- 欠费
                                   from (
                                            -- 合同表
                                            select BIS_CONT.bis_project_id,
                                                   BIS_CONT.bis_cont_id,
                                                   BIS_CONT.bis_shop_id,
                                                   bis_cont.STORE_TYPE,                           -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                                                   case
                                                       when bis_cont.bis_shop_name is null then bis_shop.NAME_CN
                                                       else bis_cont.bis_shop_name end as NAME_CN -- 结果表取合同表中品牌名称，如合同表中品牌名称为空取品牌表中名称，  如全部为空  及为空
                                            from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                                                     left join ods.ods_pl_powerdes_bis_shop_dt bis_shop
                                                               on BIS_CONT.BIS_SHOP_ID = BIS_SHOP.BIS_SHOP_ID
                                        ) table1
                                            inner join
                                        (
                                            select bis_project.project_name, -- 项目名称
                                                   d.bis_project_id,         -- 项目id
                                                   d.bis_cont_id,            -- 合同id
                                                   d.qz_year_month,          -- 权责月
                                                   d.MUST_TYPE,              -- 费项
                                                   d.oweMoney                -- 欠费
                                            from (
                                                     select must.bis_project_id,
                                                            must.bis_cont_id,
                                                            must.qz_year_month,
                                                            must.MUST_TYPE,
                                                            sum(nvl(mustMoney, 0)) + sum(nvl(adjMoney, 0)) -
                                                            sum(nvl(factMoney, 0)) as oweMoney
                                                     from (
                                                              -- 应收
                                                              SELECT BIS_MUST2.bis_project_id,
                                                                     BIS_MUST2.bis_cont_id,
                                                                     BIS_MUST2.qz_year_month,
                                                                     BIS_MUST2.MUST_TYPE,
                                                                     sum(nvl(BIS_MUST2.money, 0)) as mustMoney
                                                              FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                                              where BIS_MUST2.must_type in ('1', '2')
                                                                and BIS_MUST2.is_show = 1
                                                                and BIS_MUST2.is_delete = 0
                                                                and BIS_MUST2.qz_year_month <= substr(CURRENT_DATE, 0, 7)
                                                              group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                                                       BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE
                                                          ) must
                                                              left join
                                                          (
                                                              -- 减免
                                                              SELECT bis_mf_adjust.bis_cont_id,
                                                                     bis_mf_adjust.qz_year_month,
                                                                     bis_mf_adjust.FEE_TYPE,
                                                                     sum(nvl(bis_mf_adjust.adjust_money, 0)) as adjMoney
                                                              FROM ods.ods_pl_powerdes_bis_mf_adjust_dt bis_mf_adjust
                                                              where bis_mf_adjust.is_del = 1
                                                                and bis_mf_adjust.fee_type in ('1', '2')
                                                              group by bis_mf_adjust.bis_cont_id,
                                                                       bis_mf_adjust.qz_year_month,
                                                                       bis_mf_adjust.FEE_TYPE
                                                          ) adj
                                                          on must.bis_cont_id = adj.bis_cont_id and
                                                             must.qz_year_month = adj.qz_year_month and
                                                             must.MUST_TYPE = adj.FEE_TYPE
                                                              left join
                                                          (
                                                              -- 实收
                                                              SELECT bis_fact2.bis_cont_id,
                                                                     bis_fact2.qz_year_month,
                                                                     bis_fact2.FACT_TYPE,
                                                                     sum(nvl(bis_fact2.money, 0)) as factMoney
                                                              FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
                                                              where bis_fact2.is_delete = 1
                                                                and bis_fact2.fact_type in ('1', '2')
                                                              group by bis_fact2.bis_cont_id, bis_fact2.qz_year_month,
                                                                       bis_fact2.FACT_TYPE
                                                          ) fact
                                                          on must.bis_cont_id = fact.bis_cont_id and
                                                             must.qz_year_month = fact.qz_year_month and
                                                             must.MUST_TYPE = fact.FACT_TYPE
                                                     group by must.bis_project_id, must.bis_cont_id, must.qz_year_month,
                                                              must.MUST_TYPE
                                                 ) d
                                                     left join ods.ods_pl_powerdes_bis_project_dt bis_project
                                                               on d.bis_project_id = bis_project.bis_project_id
                                        ) t on table1.BIS_PROJECT_ID = t.BIS_PROJECT_ID and
                                               table1.BIS_CONT_ID = t.BIS_CONT_ID
                               ) t2 on t1.bis_project_id = t2.bis_project_id and t1.year_month1 = t2.qz_year_month
                      ) t
                          left join
                      (
                          -- 合同欠费（应收日期）
                          select t1.bis_project_id, -- 项目id
                                 t1.short_name,     -- 项目名称
                                 t1.year,           -- 年
                                 t1.month,          -- 月
                                 t1.year_month1,    -- 年月 2021-12
                                 t2.bis_shop_id,    -- 商家id
                                 t2.name_cn,        -- 商家名称
                                 t2.must_type,      -- 费项（1：租金 2：物管）
                                 t2.store_type,     -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                                 t2.owe_cont,       -- 欠费（应收月）
                                 t2.bis_cont_id     -- 合同id

                          from (
                                   select distinct bis_project.bis_project_id,
                                                   bis_project.short_name,
                                                   year,
                                                   month,
                                                   year_month1
                                   from dim.dim_pl_date,
                                        ods.ods_pl_powerdes_bis_project_dt bis_project
                                   where year between '2010' and '2030'
                                     and bis_project.is_business_project = '1'
                                     and oper_status = '2'
                               ) t1
                                   left join
                               (
                                   select table1.BIS_PROJECT_ID, -- 项目id
                                          t.PROJECT_NAME,        -- 项目名称
                                          table1.BIS_CONT_ID,    -- 合同id
                                          table1.STORE_TYPE,     -- 物业类型
                                          table1.BIS_SHOP_ID,    -- 商家id
                                          table1.NAME_CN,        -- 商家名称
                                          t.must_year_month,     -- 应收月
                                          t.MUST_TYPE,           -- 费项
                                          t.oweMoney owe_cont    -- 欠费
                                   from (
                                            -- 合同表
                                            select BIS_CONT.bis_project_id,
                                                   BIS_CONT.bis_cont_id,
                                                   BIS_CONT.bis_shop_id,
                                                   bis_cont.STORE_TYPE,                           -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                                                   case
                                                       when bis_cont.bis_shop_name is null then bis_shop.NAME_CN
                                                       else bis_cont.bis_shop_name end as NAME_CN -- 结果表取合同表中品牌名称，如合同表中品牌名称为空取品牌表中名称，  如全部为空  及为空
                                            from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                                                     left join ods.ods_pl_powerdes_bis_shop_dt bis_shop
                                                               on BIS_CONT.BIS_SHOP_ID = BIS_SHOP.BIS_SHOP_ID
                                        ) table1
                                            inner join
                                        (
                                            select bis_project.project_name, -- 项目名称
                                                   d.bis_project_id,         -- 项目id
                                                   d.bis_cont_id,            -- 合同id
                                                   d.must_year_month,        -- 应收月
                                                   d.MUST_TYPE,              -- 费项
                                                   d.oweMoney                -- 欠费
                                            from (
                                                     select t.bis_project_id,                                                          -- 项目id
                                                            t.bis_cont_id,                                                             -- 合同id
                                                            t.MUST_TYPE,                                                               -- 费项（1：租金 2：物管）
                                                            substr(t.rent_last_pay_date, 0, 7)                        must_year_month, -- 应收月
                                                            sum(t.mustMoney) + sum(t1.adjMoney) - sum(fact.factMoney) oweMoney         -- 欠费
                                                     from (
                                                              -- 应收
                                                              SELECT BIS_MUST2.bis_project_id,
                                                                     BIS_MUST2.bis_cont_id,
                                                                     BIS_MUST2.qz_year_month,
                                                                     BIS_MUST2.MUST_TYPE,
                                                                     BIS_MUST2.billing_period_end,
                                                                     BIS_MUST2.billing_period_begin,
                                                                     BIS_MUST2.bis_must_id,
                                                                     BIS_MUST2.rent_last_pay_date, -- 应收日期
                                                                     sum(nvl(BIS_MUST2.money, 0)) as mustMoney
                                                              FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                                              where BIS_MUST2.must_type in ('1', '2')
                                                                and BIS_MUST2.is_show = 1
                                                                and BIS_MUST2.is_delete = 0
                                                                and substr(BIS_MUST2.rent_last_pay_date, 0, 7) <=
                                                                    substr(CURRENT_DATE, 0, 7)
                                                              group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                                                       BIS_MUST2.bis_must_id,
                                                                       BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                                                                       BIS_MUST2.billing_period_end,
                                                                       BIS_MUST2.billing_period_begin,
                                                                       BIS_MUST2.rent_last_pay_date
                                                          ) t
                                                              left join
                                                          (
                                                              -- 特殊情况：两条应收除了 应收id和应收日期不一样（权责 ，费项 账期开始、账期结束 都一样），这个时候减免没法知道和哪条应收对应
                                                              -- 处理方法：取这两条应收中应收日期最小的一条应收和减免关联
                                                              select must.bis_must_id, -- 应收id
                                                                     adj.adjMoney      -- 减免
                                                              from (
                                                                       -- 应收
                                                                       SELECT BIS_MUST2.bis_project_id,
                                                                              BIS_MUST2.bis_cont_id,
                                                                              BIS_MUST2.qz_year_month,
                                                                              BIS_MUST2.MUST_TYPE,
                                                                              BIS_MUST2.billing_period_end,
                                                                              BIS_MUST2.billing_period_begin,
                                                                              min(BIS_MUST2.rent_last_pay_date) rent_last_pay_date, -- 应收日期
                                                                              min(BIS_MUST2.bis_must_id)   as   bis_must_id,
                                                                              sum(nvl(BIS_MUST2.money, 0)) as   mustMoney
                                                                       FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                                                       where BIS_MUST2.must_type in ('1', '2')
                                                                         and BIS_MUST2.is_show = 1
                                                                         and BIS_MUST2.is_delete = 0
                                                                         and substr(BIS_MUST2.rent_last_pay_date, 0, 7) <=
                                                                             substr(CURRENT_DATE, 0, 7)
                                                                       group by BIS_MUST2.bis_project_id,
                                                                                BIS_MUST2.bis_cont_id,
                                                                                BIS_MUST2.qz_year_month,
                                                                                BIS_MUST2.MUST_TYPE,
                                                                                BIS_MUST2.billing_period_end,
                                                                                BIS_MUST2.billing_period_begin
                                                                   ) must
                                                                       left join
                                                                   (
                                                                       -- 减免
                                                                       SELECT bis_mf_adjust.bis_cont_id,
                                                                              bis_mf_adjust.billing_period_end,
                                                                              bis_mf_adjust.billing_period_begin,
                                                                              bis_mf_adjust.qz_year_month,
                                                                              bis_mf_adjust.FEE_TYPE,
                                                                              sum(nvl(bis_mf_adjust.adjust_money, 0)) as adjMoney
                                                                       FROM ods.ods_pl_powerdes_bis_mf_adjust_dt bis_mf_adjust
                                                                       where bis_mf_adjust.is_del = 1
                                                                         and bis_mf_adjust.fee_type in ('1', '2')
                                                                       group by bis_mf_adjust.bis_cont_id,
                                                                                bis_mf_adjust.qz_year_month,
                                                                                bis_mf_adjust.FEE_TYPE,
                                                                                bis_mf_adjust.billing_period_end,
                                                                                bis_mf_adjust.billing_period_begin
                                                                   ) adj
                                                                       -- 合同 权责 费项 账期开始  账期结束
                                                                   on must.bis_cont_id = adj.bis_cont_id and
                                                                      must.qz_year_month = adj.qz_year_month and
                                                                      must.MUST_TYPE = adj.FEE_TYPE and
                                                                      must.billing_period_end = adj.billing_period_end and
                                                                      must.billing_period_begin = adj.billing_period_begin
                                                          ) t1 on t.bis_must_id = t1.bis_must_id
                                                              left join
                                                          (
                                                              -- 实收
                                                              SELECT bis_fact2.bis_cont_id,
                                                                     bis_fact2.bis_must_id,
                                                                     bis_fact2.fact_type,
                                                                     sum(nvl(bis_fact2.money, 0)) as factMoney
                                                              FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
                                                              where bis_fact2.is_delete = 1
                                                                and bis_fact2.fact_type in ('1', '2')
                                                                and bis_fact2.bis_must_id is not null
                                                              group by bis_fact2.bis_cont_id, bis_fact2.bis_must_id,
                                                                       bis_fact2.fact_type
                                                          ) fact
                                                          on t.bis_cont_id = fact.bis_cont_id and
                                                             t.bis_must_id = fact.bis_must_id and
                                                             t.MUST_TYPE = fact.FACT_TYPE
                                                     group by t.bis_project_id, t.bis_cont_id,
                                                              substr(t.rent_last_pay_date, 0, 7),
                                                              t.MUST_TYPE
                                                 ) d
                                                     left join ods.ods_pl_powerdes_bis_project_dt bis_project
                                                               on d.bis_project_id = bis_project.bis_project_id
                                        ) t on table1.BIS_PROJECT_ID = t.BIS_PROJECT_ID and
                                               table1.BIS_CONT_ID = t.BIS_CONT_ID
                               ) t2 on t1.bis_project_id = t2.bis_project_id and t1.year_month1 = t2.must_year_month
                      ) t1 on t.bis_project_id = t1.bis_project_id and t.bis_shop_id = t1.bis_shop_id and
                              t.bis_cont_id = t1.bis_cont_id and
                              t.store_type = t1.store_type
                          and t.must_type = t1.must_type and t.year_month1 = t1.year_month1
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
                      ) t2 on t.bis_project_id = t2.out_mall_id
             ) table4
        group by table4.bis_project_id, table4.bis_cont_id, table4.bis_shop_id, table4.year_month1
    ) table5 on table1.bis_project_id = table5.bis_project_id and table1.bis_cont_id = table5.bis_cont_id and
                table1.bis_shop_id = table5.bis_shop_id and table1.year_month1 = table5.year_month1;










