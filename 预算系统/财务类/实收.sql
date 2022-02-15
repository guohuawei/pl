/*
    财务类--实收
*/
SELECT bis_fact2.BIS_FACT_ID, -- 实收id
       bis_fact2.bis_must_id, -- 应收id
       bis_cont.BIS_CONT_ID,-- 合同code
       bis_cont.cont_no,-- 合同编号
       bis_cont.cont_name,                                                                                        -- 合同名称
       CASE WHEN bis_cont.bis_shop_id IS NULL THEN bis_cont.bis_shop_name ELSE bis_shop.name_cn END brandName,    -- 租户品牌
       bis_cont.bis_shop_id,                                                                                      -- 品牌code
       CASE
           WHEN bis_cont.cont_type_cd = 3 THEN bis_more_manage_leasee.leasee_name
           WHEN bis_cont.cont_type_cd = 2 AND bis_shop_conn.part_name IS NULL THEN bis_more_manage_leasee.leasee_name
           WHEN bis_cont.cont_type_cd = 2 AND bis_more_manage_leasee.leasee_name IS NULL THEN bis_shop_conn.part_name
           ELSE bis_shop_conn.part_name END        AS                                               companyName,  -- 商家
       CASE
           WHEN bis_cont.cont_type_cd = 3 THEN bis_more_manage_leasee.more_manage_leasee_id
           WHEN bis_cont.cont_type_cd = 2 AND bis_shop_conn.bis_shop_conn_id IS NULL
               THEN bis_more_manage_leasee.more_manage_leasee_id
           WHEN bis_cont.cont_type_cd = 2 AND bis_more_manage_leasee.more_manage_leasee_id IS NULL
               THEN bis_shop_conn.bis_shop_conn_id
           ELSE bis_shop_conn.bis_shop_conn_id END AS                                               company_ID,   -- 商家code
       bis_fact2.FACT_NO,                                                                                         -- 核销编号
       bis_cont.bis_store_nos,                                                                                    -- 铺位号
       bis_cont.bis_store_ids,                                                                                    -- 铺位id
       pms_fin_charge_item_dict.CHARGE_ITEM_NAME,                                                                 -- 费项
       bis_fact2.fact_type                                                                          fee_type,     -- 费项code
       bis_fact2.FACT_DATE,                                                                                       -- 实收时间
       bis_fact2.BILLING_PERIOD_BEGIN,                                                                            -- 账期开始时间
       bis_fact2.BILLING_PERIOD_END,                                                                              -- 账期结束时间

       bis_fact2.IS_DELETE,                                                                                       -- 状态 1:正常；0：删除
       bis_fact2.IS_KEEP_OFFSET                                                                     fact_type,    -- 类型 0普通核销1保证金冲抵

       bis_fact2.MONEY,                                                                                           -- 核销金额
       substr(bis_fact2.QZ_YEAR_MONTH, 0, 4),                                                                     -- 权责年
       substr(bis_fact2.QZ_YEAR_MONTH, 6, 7),                                                                     -- 权责月

       substr(bis_fact2.QZ_YEAR_MONTH, 0, 4),                                                                     -- 财务年
       substr(bis_fact2.QZ_YEAR_MONTH, 6, 7),                                                                     -- 财务月

       CASE
           WHEN bis_fact2.BIS_MUST_ID is null then 'pd'
           else 'pms' end                          as                                               data_sources, --数据来源
       null                                                                                         adjust_MONEY, --  减免金额
       pms_fin_charge_item_dict.updated_date as pms_fin_charge_item_dict_updated_date, --收费项目字典表更新时间
       bis_cont.updated_date as bis_cont_updated_date, -- 合同更新时间
       bis_shop.updated_date as bis_shop_updated_date, -- 商家表更新时间
       bis_shop_conn.updated_date as bis_shop_conn_updated_date, -- 商家供应商公司表更新时间
       bis_more_manage_leasee.updated_date as bis_more_manage_leasee_updated_date, -- 合同类型为多经或者（2019年11月之后）新增的销售合同的承租方表更新时间
       case
           when bis_fact2.is_delete = '1' THEN 'N'
           ELSE 'Y' END                            AS                                               IS_DEL,       -- 是否删除
       bis_fact2.updated_date,                                                                                    -- 更新时间
       bis_fact2.updator,                                                                                         -- 更新人员
       bis_fact2.created_date,                                                                                    -- 创建时间
       bis_fact2.creator,                                                                                         -- 创建人
       'oracle'                                    as                                               source        -- 数据来源（oracle,mysql,http）


FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
         LEFT JOIN ods.ods_pl_powerdes_pms_fin_charge_item_dict_dt pms_fin_charge_item_dict
                   ON bis_fact2.FACT_TYPE = pms_fin_charge_item_dict.CHARGE_ITEM_CODE AND
                      pms_fin_charge_item_dict.is_del = '0'
         LEFT JOIN ods.ods_pl_powerdes_bis_cont_dt bis_cont ON bis_cont.bis_cont_id = bis_fact2.bis_cont_id
         LEFT JOIN ods.ods_pl_powerdes_bis_shop_dt bis_shop ON bis_cont.bis_shop_id = bis_shop.bis_shop_id
         LEFT JOIN ods.ods_pl_powerdes_bis_shop_conn_dt bis_shop_conn
                   ON bis_cont.bis_shop_conn_id = bis_shop_conn.bis_shop_conn_id
         LEFT JOIN ods.ods_pl_powerdes_bis_more_manage_leasee_dt bis_more_manage_leasee
                   ON bis_cont.bis_cont_id = bis_more_manage_leasee.bis_cont_id
where bis_fact2.is_delete = '1';