/*
    财务类---应收表
*/

-- 应收表
SELECT bis_must2.BIS_MUST_ID,                                                                                     -- 应收code
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
           ELSE bis_shop_conn.bis_shop_conn_id END AS                                               company_code, -- 商家code
       bis_must2.MUST_NO,                                                                                         -- 应收编号
       bis_cont.bis_store_nos,                                                                                    -- 铺位号
       bis_cont.bis_store_ids,                                                                                    -- 铺位id
       pms_fin_charge_item_dict.CHARGE_ITEM_NAME,                                                                 -- 费项
       bis_must2.must_type,                                                                                       -- 费项code
       bis_must2.RENT_LAST_PAY_DATE,                                                                              -- 应收时间
       bis_must2.BILLING_PERIOD_BEGIN,                                                                            -- 账期开始时间
       bis_must2.BILLING_PERIOD_END,                                                                              -- 账期结束时间

       bis_must2.IS_DELETE,                                                                                       -- 状态 0:正常；1：删除
       bis_must2.IS_SHOW,                                                                                         -- 是否显示状态(0：否，1：是     [应收类型为水、电、水电公摊、空调的数据，只有在录入界面录入金额之后才会在收费中心、应收查询等界面查询显示])
       bis_must2.STATUS_CD,                                                                                       -- 收款状态(0：未交清，1：已交清)

       bis_must2.MONEY,                                                                                           -- 应收金额
       substr(bis_must2.QZ_YEAR_MONTH, 0, 4),                                                                     -- 权责年
       substr(bis_must2.QZ_YEAR_MONTH, 6, 7),                                                                     -- 权责月

       substr(bis_must2.QZ_YEAR_MONTH, 0, 4),                                                                     -- 财务年
       substr(bis_must2.QZ_YEAR_MONTH, 6, 7),                                                                     -- 财务月
       fact.fact_money                                                                              fact_money,   -- 核销金额
       CASE
           WHEN bis_must2.RES_NO is null then 'pd'
           else 'pms' end                          as                                               data_sources, --数据来源
       null                                                                                         adjust_MONEY, --  减免金额
       pms_fin_charge_item_dict.updated_date as pms_fin_charge_item_dict_updated_date, --收费项目字典表更新时间
       bis_cont.updated_date as bis_cont_updated_date, -- 合同更新时间
       bis_shop.updated_date as bis_shop_updated_date, -- 商家表更新时间
       bis_shop_conn.updated_date as bis_shop_conn_updated_date, -- 商家供应商公司表更新时间
       bis_more_manage_leasee.updated_date as bis_more_manage_leasee_updated_date, -- 合同类型为多经或者（2019年11月之后）新增的销售合同的承租方表更新时间
       case
           when bis_must2.is_delete = '0' THEN 'N'
           ELSE 'Y' END                            AS                                               IS_DEL,       -- 是否删除
       bis_must2.updated_date,                                                                                    -- 更新时间
       bis_must2.updator,                                                                                         -- 更新人员
       bis_must2.created_date,                                                                                    -- 创建时间
       bis_must2.creator,                                                                                         -- 创建人
       'oracle'                                    as                                               source        -- 数据来源（oracle,mysql,http）


FROM ods.ods_pl_powerdes_bis_must2_dt bis_must2
         LEFT JOIN ods.ods_pl_powerdes_pms_fin_charge_item_dict_dt pms_fin_charge_item_dict
                   ON bis_must2.must_type = pms_fin_charge_item_dict.CHARGE_ITEM_CODE AND
                      pms_fin_charge_item_dict.is_del = '0'
         LEFT JOIN ods.ods_pl_powerdes_bis_cont_dt bis_cont ON bis_cont.bis_cont_id = bis_must2.bis_cont_id
         LEFT JOIN ods.ods_pl_powerdes_bis_shop_dt bis_shop ON bis_cont.bis_shop_id = bis_shop.bis_shop_id
         LEFT JOIN ods.ods_pl_powerdes_bis_shop_conn_dt bis_shop_conn
                   ON bis_cont.bis_shop_conn_id = bis_shop_conn.bis_shop_conn_id
         LEFT JOIN ods.ods_pl_powerdes_bis_more_manage_leasee_dt bis_more_manage_leasee
                   ON bis_cont.bis_cont_id = bis_more_manage_leasee.bis_cont_id
         LEFT JOIN (SELECT bis_must_id,
                           sum(money) fact_money
                    FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
                    WHERE is_delete = '1'
                      AND bis_must_id is not null
                    GROUP BY bis_must_id
) fact ON fact.bis_must_id = bis_must2.bis_must_id
where bis_must2.is_delete = '0';




