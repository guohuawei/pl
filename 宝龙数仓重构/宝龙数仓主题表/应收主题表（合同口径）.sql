-- 应收主题表（合同口径）
select table1.bis_project_id,                                     -- 项目id
       table1.PROJECT_NAME,                                       -- 项目名称
       mst.bis_must_id,                                           -- 应收id
       table1.bis_cont_id,                                        -- 合同id
       table1.BIS_SHOP_ID,                                        -- 商家id(品牌)
       table1.NAME_CN,                                            -- 商家名称
       table1.store_type,                                         -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
       mst.qz_year_month,                                         -- 应收权责月
       mst.must_type,                                             -- 费项类型
       PMS_FIN_CHARGE_ITEM_DICT.CHARGE_ITEM_NAME,                 -- 费项中文名称
       PMS_FIN_CHARGE_ITEM_DICT.CHARGE_ITEM_TYPE,                 -- 收费类型
       (nvl(mst.mustMoney, 0) + nvl(mst.adjMoney, 0)) must_money, -- 应收金额(应收 + 减免)
       mst.rent_last_pay_date,                                    -- 应收日期
       mst.billing_period_begin,                                  -- 账期开始时间
       mst.billing_period_end,                                    -- 账期结束时间
       mst.rent_last_pay_date,                                    -- 最迟缴费日
       nvl(mst.adjMoney, 0),                                      -- 减免
       table1.bis_store_nos,                                      -- 铺位号
       table1.company_name,                                       -- 铺位承租方
       table1.cont_no,                                            -- 合同号
       current_date                                               -- ETL时间
from
    -- 合同表
    (
        select BIS_CONT.bis_project_id,
               BIS_CONT.bis_cont_id,
               BIS_CONT.bis_shop_id,
               bis_cont.cont_no,
               bis_cont.bis_store_nos,
               bis_cont.STORE_TYPE,                            -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
               case
                   when bis_cont.bis_shop_name is null then bis_shop.NAME_CN
                   else bis_cont.bis_shop_name end as NAME_CN, -- 结果表取合同表中品牌名称，如合同表中品牌名称为空取品牌表中名称，  如全部为空  及为空
               bis_project.PROJECT_NAME,
               case
                   when bis_cont.CONT_TYPE_CD = 3 then leasee.leasee_name
                   when bis_cont.CONT_TYPE_CD = 2 and bis_shop_conn.PART_NAME is null then leasee.leasee_name
                   when bis_cont.CONT_TYPE_CD = 2 and leasee.leasee_name is null then bis_shop_conn.PART_NAME
                   else bis_shop_conn.PART_NAME end   company_name
        from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                 LEFT JOIN ods.ods_pl_powerdes_bis_shop_conn_dt bis_shop_conn
                           ON bis_shop_conn.BIS_SHOP_CONN_ID = bis_cont.Bis_Shop_Conn_Id
                 LEFT JOIN ods.ods_pl_powerdes_bis_more_manage_leasee_dt leasee
                           on leasee.bis_cont_id = bis_cont.bis_cont_id
                 left join ods.ods_pl_powerdes_bis_project_dt bis_project
                           on BIS_CONT.BIS_PROJECT_ID = BIS_PROJECT.BIS_PROJECT_ID
                 left join ods.ods_pl_powerdes_bis_shop_dt bis_shop on BIS_CONT.BIS_SHOP_ID = BIS_SHOP.BIS_SHOP_ID
    ) table1
        left join
    (
        select t.bis_project_id,       -- 项目id
               t.bis_cont_id,          -- 合同id
               t.qz_year_month,        -- 权责月
               t.MUST_TYPE,            -- 费项
               t.billing_period_end,   -- 账期开始
               t.billing_period_begin, -- 账期结束
               t.bis_must_id,          -- 应收id
               t.rent_last_pay_date,   -- 应收日期
               t.mustMoney,            -- 应收金额
               t1.adjMoney             -- 减免金额

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
                 where BIS_MUST2.must_type in ('1', '2', '34', '57')
                   and BIS_MUST2.is_show = 1
                   and BIS_MUST2.is_delete = 0
                 group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id, BIS_MUST2.bis_must_id,
                          BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE, BIS_MUST2.billing_period_end,
                          BIS_MUST2.billing_period_begin, BIS_MUST2.rent_last_pay_date
             ) t
                 left join
             (
                 -- 特殊情况：两条应收除了 应收id和应收日期不一样（权责 ，费项 账期开始、账期结束 都一样），这个时候减免没法知道和哪条应收对应
                 -- 处理方法：取这两条应收中应收日期最小的一条应收和减免关联
                 select must.bis_must_id, -- 应收id
                        adj.adjMoney      -- 减免
                 from (
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
                          where BIS_MUST2.must_type in ('1', '2', '34', '57')
                            and BIS_MUST2.is_show = 1
                            and BIS_MUST2.is_delete = 0
                          group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                   BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE, BIS_MUST2.billing_period_end,
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
                            and bis_mf_adjust.fee_type in ('1', '2', '34', '57')
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
    ) mst on table1.bis_cont_id = mst.bis_cont_id and table1.bis_project_id = mst.bis_project_id
        left join (select distinct * from ods.ods_pl_powerdes_pms_fin_charge_item_dict_dt) PMS_FIN_CHARGE_ITEM_DICT
                  on mst.MUST_TYPE = PMS_FIN_CHARGE_ITEM_DICT.CHARGE_ITEM_CODE
where PMS_FIN_CHARGE_ITEM_DICT.IS_DEL = '0';