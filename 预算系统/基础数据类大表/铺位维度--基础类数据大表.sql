/*
   基础数据类大表（pms）
   dws_bis_store_big
*/
select nvl(bis_store.BIS_STORE_ID, bis_multi.bis_multi_id),          -- 铺位id
       nvl(bis_store.store_no, bis_multi.multi_name),                -- 新铺位编号
       nvl(bis_store.old_store_no, bis_multi.multi_name),            -- 老铺位编号
       bis_store.layout_cd,                                          -- 规划业态
       bis_store.issuing_layout_cd,                                  -- 签批业态
       null                     as business_type,                    -- 商业类型
       bis_store.fee_square,                                         -- 计费面积
       null                     as rent_status,                      --  租赁状态
       null                     as rent_way,                         -- 计租方式
       bis_store_merge.store_type,                                   -- 铺位类型 1:店铺 2：多经
       nvl(bis_project.bis_project_id, bis_project2.bis_project_id), -- 项目id
       nvl(bis_project.short_name, bis_project2.short_name),         -- 项目名称
       nvl(bis_project.open_date, bis_project2.open_date),           -- 开业日期
       nvl(bis_project.operate_area, bis_project2.operate_area),     -- 区域
       nvl(bis_project.city, bis_project2.city),                     -- 城市
       nvl(bis_floor.charge_type, bis_floor2.charge_type),           -- 物业类型
       case
           when bis_store.EQUITY_NATURE = '1' then '自持'
           when bis_store.EQUITY_NATURE = '2' and bis_store.MANAGEMENT_STATUS = '1' then '已售返祖'
           when bis_store.EQUITY_NATURE = '2' and bis_store.MANAGEMENT_STATUS = '2' then '已售不返祖'
           else '其他'
           end                  as property_right,                   -- 产权信息
       null                     as business_scope,                   -- 经营范围
       nvl(bis_floor.building_num, bis_floor2.building_num),         -- 楼栋名称
       nvl(bis_floor.floor_num, bis_floor2.floor_num),               -- 楼层名称
       nvl(bis_floor.bis_floor_id, bis_floor2.bis_floor_id),         -- 楼层id
       nvl(bis_floor.building_id, bis_floor2.building_id),           -- 楼栋id
       null                     as principal,                        -- 负责人
       nvl(bis_store.square_equity, bis_multi.max_area),             -- 建筑面积
       nvl(bis_store.inner_square_equity, 0),                        -- 套内面积
       nvl(bis_store.rent_square, bis_multi.square),                 -- 计租面积
       nvl(bis_project.oper_status, bis_project2.oper_status),       -- 经营状态
       null                     as building_status,                  -- 楼栋状态
       nvl(bis_floor.is_active_floor, bis_floor2.is_active_floor),   -- 楼层状态
       bis_store_merge.multi_type,                                   -- 点位类型
       bis_project.updated_date as bis_project_updated_date,         -- 项目更新时间
       bis_floor.updated_date   as bis_floor_updated_date,           -- 楼层更新时间
       bis_store_merge.IS_DEL,                                       -- 是否删除
       bis_store_merge.updated_date,                                 -- 更新时间
       bis_store_merge.updator,                                      -- 更新人员
       bis_store_merge.created_date,                                 -- 创建时间
       bis_store_merge.creator,                                      -- 创建人
       "oracle"                 as source                            -- 数据来源


from (
         select bis_store.bis_store_id,
                '1'                 as store_type,
                null                as multi_type, -- 点位类型
                bis_store.is_delete as IS_DEL,     -- 是否删除
                bis_store.updated_date,            -- 更新时间
                bis_store.updator,                 -- 更新人员
                bis_store.created_date,            -- 创建时间
                bis_store.creator,                 -- 创建人
                "oracle"            as source      -- 数据来源
         from ods.ods_pl_powerdes_bis_store_dt bis_store
         where bis_store.is_delete = 'N'
           and bis_store.status_cd = '1'
         union all
         select bis_multi.bis_multi_id,
                '2'                         as store_type,
                bis_multi.multi_charge_type as multi_type, -- 点位类型
                null                        as IS_DEL,     -- 是否删除
                bis_multi.updated_date,                    -- 更新时间
                bis_multi.updator,                         -- 更新人员
                bis_multi.created_date,                    -- 创建时间
                bis_multi.creator,                         -- 创建人
                "oracle"                    as source      -- 数据来源
         from ods.ods_pl_powerdes_bis_multi_dt bis_multi
     ) bis_store_merge
         left join ods.ods_pl_powerdes_bis_store_dt bis_store
                   on bis_store.bis_store_id = bis_store_merge.bis_store_id
         left join ods.ods_pl_powerdes_bis_multi_dt bis_multi
                   on bis_multi.bis_multi_id = bis_store_merge.bis_store_id
         left join ods.ods_pl_powerdes_bis_project_dt bis_project
                   on bis_store.bis_project_id = bis_project.bis_project_id
         left join ods.ods_pl_powerdes_bis_project_dt bis_project2
                   on bis_multi.bis_project_id = bis_project2.bis_project_id
         left join ods.ods_pl_powerdes_bis_floor_dt bis_floor
                   on bis_store.bis_floor_id = bis_floor.bis_floor_id
         left join ods.ods_pl_powerdes_bis_floor_dt bis_floor2
                   on bis_multi.bis_floor_id = bis_floor2.bis_floor_id;