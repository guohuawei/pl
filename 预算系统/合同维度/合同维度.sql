/*
    合同维度
    dws_bis_cont_big
*/
select bis_cont.bis_project_id,                                                             -- 项目id
       table1.bis_floor_id,                                                                 -- 楼层id
       table1.building_id,                                                                  -- 楼栋id
       bis_project.project_name,                                                            -- 所属项目
       bis_project.oper_status,                                                             -- 营运状态
       bis_cont.cont_name,                                                                  -- 合同名称
       bis_cont.bis_cont_id,                                                                -- 合同id
       bis_cont.cont_no,                                                                    --合同编号
       bis_cont.cont_start_date,                                                            --合同开始时间
       bis_cont.cont_end_date,                                                              -- 合同结束时间
       bis_cont.sign_date,                                                                  -- 合同签约时间
       bis_cont.cont_to_fail_date,                                                          -- 合同解约时间
       bis_cont.fitment_start_date,                                                         -- 合同装修开始时间
       bis_cont.fitment_end_date,                                                           -- 合同装修结束时间
       bis_cont.cont_money,                                                                 -- 合同总金额
       bis_cont.pay_way,                                                                    -- 租金方式
       bis_cont.pay_way_prop,                                                               -- 计费方式
       bis_cont.bis_store_ids,                                                              -- 铺位ID
       bis_cont.bis_store_nos,                                                              -- 铺位编号
       bis_cont.inner_square,                                                               -- 合同套内面积
       bis_cont.square,                                                                     -- 合同建筑面积
       bis_cont.rent_square,                                                                -- 合同计租面积
       bis_cont.proportion_ids,                                                             -- 签约业态
       bis_cont.layout_cd,                                                                  -- 规划业态
       table1.floor_num,                                                                    -- 所属楼层
       table1.building_num,                                                                 -- 所属楼栋
       bis_cont.equity_nature,                                                              -- 产权性质
       null                                 as                          bd_yye,             -- 保底营业额
       null                                 as                          rent_type,          -- 租赁类型
       CASE
           WHEN bis_cont.bis_shop_id IS NULL THEN bis_cont.bis_shop_name
           ELSE bis_shop.name_cn END                                    brandName,          -- 品牌
       CASE
           WHEN bis_cont.cont_type_cd = 3 THEN bis_more_manage_leasee.leasee_name
           WHEN bis_cont.cont_type_cd = 2 AND bis_shop_conn.part_name IS NULL THEN bis_more_manage_leasee.leasee_name
           WHEN bis_cont.cont_type_cd = 2 AND bis_more_manage_leasee.leasee_name IS NULL THEN bis_shop_conn.part_name
           ELSE bis_shop_conn.part_name END AS                          companyName,        -- 承租方
       bis_cont.status_cd,                                                                  -- 合同审批状态
       bis_cont.rent_date,                                                                  -- 计租开始时间
       bis_cont.actual_open_date,                                                           -- 预计进场时间
       (datediff(bis_cont.cont_end_date, bis_cont.cont_start_date) + 1) cont_total_time,    -- 合同总时长
       null                                 as                          commission,         -- 佣金
       null                                 as                          commission_destiny, -- 佣金点数
       bis_cont.CONT_TYPE_CD,                                                               -- 合同类别
       bis_cont.STORE_TYPE,                                                                 -- 物业类型
       bis_cont.MANAGEMENT_FORMS,                                                           -- 经营状态
       bis_cont.MANAGE_CD,                                                                  -- 商家性质
       bis_project.updated_date as bis_project_updated_date, -- 项目更新时间
       bis_shop.updated_date as bis_shop_updated_date, -- 店铺更新时间
       bis_shop_conn.updated_date as bis_shop_conn_updated_date, -- 商家供应商公司表更新时间
       bis_more_manage_leasee.updated_date as bis_more_manage_leasee_updated_date,
       case
           when bis_cont.EFFECT_FLG = 'D' THEN 'Y'
           else 'N' END                     as                          IS_DEL,             -- 是否删除
       bis_cont.updated_date,                                                               -- 更新时间
       bis_cont.updator,                                                                    -- 更新人员
       bis_cont.created_date,                                                               -- 创建时间
       bis_cont.creator,                                                                    -- 创建人
       "oracle"                             as                          source              -- 数据来源

from ods.ods_pl_powerdes_bis_cont_dt bis_cont
         left join
     (
         select table11.bis_cont_id,                     -- 合同号
                collect_list(bis_floor.bis_floor_id) as bis_floor_id, -- 楼层id
                collect_list(bis_floor.building_id)  as building_id,  -- 楼栋id
                collect_list(bis_floor.building_num) as building_num, -- 楼栋
                collect_list(bis_floor.floor_num)    as floor_num     -- 楼层

         from  (
                   SELECT tmp.bis_cont_id,
                          tmp.bis_store_ids,
                          bis_store_id
                   FROM (
                            SELECT bis_cont_id,
                                   bis_store_ids
                            FROM ods.ods_pl_powerdes_bis_cont_dt
                        ) tmp LATERAL VIEW explode(split(bis_store_ids, ',')) tmp1 AS bis_store_id
               ) table11
                  left join ods.ods_pl_powerdes_bis_store_dt bis_store
                            on table11.bis_store_id = bis_store.bis_store_id
                  left join ods.ods_pl_powerdes_bis_floor_dt bis_floor
                            on bis_store.bis_floor_id = bis_floor.bis_floor_id
         group by table11.bis_cont_id
     ) table1 on bis_cont.bis_cont_id = table1.bis_cont_id
         LEFT JOIN ods.ods_pl_powerdes_bis_project_dt bis_project
                   on bis_cont.bis_project_id = bis_project.bis_project_id
         LEFT JOIN ods.ods_pl_powerdes_bis_shop_dt bis_shop
                   ON bis_cont.bis_shop_id = bis_shop.bis_shop_id
         LEFT JOIN ods.ods_pl_powerdes_bis_shop_conn_dt bis_shop_conn
                   ON bis_cont.bis_shop_conn_id = bis_shop_conn.bis_shop_conn_id
         LEFT JOIN ods.ods_pl_powerdes_bis_more_manage_leasee_dt bis_more_manage_leasee
                   ON bis_cont.bis_cont_id = bis_more_manage_leasee.bis_cont_id;
