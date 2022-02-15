/*
  B:铺位维度--时间维度
*/
select bis_store.bis_project_id,            -- 项目id
       bis_store.bis_store_id,              -- 店铺id
       bis_store_price.seq_year,            -- 一铺一价年分
       bis_store_price.rent_price,          -- 铺位标准单价
       bis_store_price.rent_price,          -- 租金标准
       bis_store_price.mgr_price,           -- 物管标准
       null                as bhz_standard, -- 保证金标准
       null                as tax,          -- 税额
       bis_floor.floor_num,                 -- '所属楼层'
       bis_floor.building_num,              -- '所属楼栋'
       bis_store.is_delete AS IS_DEL,       -- 是否删除
       bis_store.updated_date,              -- 更新时间
       bis_store.updator,                   -- 更新人员
       bis_store.created_date,              -- 创建时间
       bis_store.creator,                   -- 创建人
       'oracle'            as source       -- 数据来源
from ods.ods_pl_powerdes_bis_store_dt bis_store
         left join ods.ods_pl_powerdes_bis_store_price_dt bis_store_price
                   on bis_store.bis_store_id = bis_store_price.bis_store_id
         left join ods.ods_pl_powerdes_bis_floor_dt bis_floor
                   on bis_store.bis_floor_id = bis_floor.bis_floor_id

