/*
 * A:铺位维度--物理铺位信息基础表
 dws_basic_data_store_big
 */


select bis_project.project_name,                                                               -- 所在项目
       bis_cont.status_cd                                         as manage_status,            -- 经营状态 （0：未签约；1：已审核；2：已解约；3：未审核;4：无效合同（补充商务条件产生）;5、待处理）
       bis_cont.bis_cont_id,                                                                   -- 合同id
       bis_cont.cont_no,                                                                       -- '合同编号'
       bis_cont.cont_start_date,                                                               -- '合同开始时间'
       bis_cont.cont_end_date,                                                                 -- '合同结束时间'
       bis_cont.sign_date,                                                                     -- '合同签约时间'
       bis_cont.cont_money,                                                                    -- '合同总金额'
       bis_cont.pay_way,                                                                       -- 合同计租规则
       bis_cont.PAY_WAY_PROP,                                                                  -- 合同计费规则
       bis_store.bis_store_id,                                                                 -- 铺位id
       bis_store.store_no,                                                                     -- '铺位编号'
       bis_store.status_cd,                                                                    -- '铺位状态'
       bis_store.inner_square_equity,                                                          -- '铺位套内面积'
       bis_store.square_equity,                                                                -- '铺位建筑面积'
       bis_store.rent_square,                                                                  -- '铺位计租面积'
       bis_store.issuing_layout_cd,                                                            -- '合同业态'
       bis_store.layout_cd,                                                                    -- '规划业态'
       bis_floor.floor_num,                                                                    -- '楼层号'
       bis_floor.building_num,                                                                 -- '楼栋号'
       bis_store.equity_nature,                                                                -- 产权性质
       null                                                       as bd_yye,                   -- 保底营业额
       null                                                       as rent_type,                -- 租赁类型
       null                                                       as store_name,               -- 铺位名称
       '1'                                                        as store_type,               -- 铺位类型 1:店铺 2：多经
       null                                                       as jy_type,                  -- 经营类型
       bis_cont.updated_date                                      as bis_cont_updated_date,    -- 合同更新时间
       bis_project.updated_date                                   as bis_project_updated_date, -- 项目更新时间
       bis_floor.updated_date                                     as bis_floor_updated_date,   -- 楼层更新时间
       datediff(bis_cont.cont_start_date, bis_cont.cont_end_date) as cont_lease,               -- 合同租期
       bis_cont.MULTI_FREE_RENT_PERIOD,                                                        -- 合同免租期
       table1.total_mgr_price,                                                                 -- 合同管理费
       table1.total_rent_price,                                                                -- 合同租金
       table1.total_rent_price / bis_cont.rent_square,                                         -- 合同租金单价
       table1.total_mgr_price / bis_cont.rent_square,                                          -- 合同管理费单价
       bis_cont.rent_square                                       as store_area,               -- 铺位合同面积
       null                                                       as cont_id_rule,             -- 合同ID规则
       null                                                       as cont_code_rule,           -- 合同编号规则
       null                                                       as cont_freeze_date,         -- 合同冻结时间
       null                                                       as cont_unfreeze_date,       -- 合同解冻时间
       null                                                       as cont_subsidy_type,        -- 合同补贴类型
       null                                                       as asset_code,               -- 资产编号
       null                                                       as asset_code_rule,          -- 资产编号规则
       null                                                       as store_id_rule,            -- 铺位id规则
       null                                                       as store_code_rule,          -- 铺位编号规则
       null                                                       as expected_turnover,        -- 预期营业额
       null                                                       as actual_turnover,          -- 实际营业额
       null                                                       as excrete_record_count,     -- 拆铺次数
       null                                                       as merge_record_count,       -- 合铺次数
       null                                                       as excrete_record_date,      -- 拆铺时间
       null                                                       as merge_record_date,        -- 合铺时间
       bis_store.is_delete,                                                                    -- 是否删除
       bis_store.updated_date,                                                                 -- 更新时间
       bis_store.updator,                                                                      -- 更新人员
       bis_store.created_date,                                                                 -- 创建时间
       bis_store.creator,                                                                      -- 创建人
       "oracle"                                                   as source                    -- 数据来源

from ods.ods_pl_powerdes_bis_store_dt bis_store

         LEFT JOIN
     (
         SELECT tmp.bis_cont_id,
                tmp.bis_store_ids,
                bis_store_id
         FROM (
                  SELECT bis_cont_id,
                         bis_store_ids
                  FROM ods.ods_pl_powerdes_bis_cont_dt
              ) tmp LATERAL VIEW explode(split(bis_store_ids, ',')) tmp1 AS bis_store_id
     ) table11
     on bis_store.bis_store_id = table11.bis_store_id

         left join ods.ods_pl_powerdes_bis_cont_dt bis_cont
                   on table11.bis_cont_id = bis_cont.bis_cont_id

         left join ods.ods_pl_powerdes_bis_project_dt bis_project
                   on bis_store.bis_project_id = bis_project.bis_project_id

         left join ods.ods_pl_powerdes_bis_floor_dt bis_floor
                   on bis_store.bis_floor_id = bis_floor.bis_floor_id
         left join
     (
         select bis_cont.bis_cont_id,
                sum(bis_store_price.rent_price * bis_store.rent_square) as total_rent_price, -- 合同租金
                sum(bis_store_price.mgr_price * bis_store.rent_square)  as total_mgr_price   -- 合同管理费
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
                                    where effect_flg <> 'D'
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
     on table1.bis_cont_id = bis_cont.bis_cont_id
where bis_store.is_delete = 'N'
  and bis_store.status_cd = '1'

union all

select bis_project.project_name,                                                               -- 所在项目
       bis_cont.status_cd                                         as manage_status,            -- 经营状态（0：未签约；1：已审核；2：已解约；3：未审核;4：无效合同（补充商务条件产生）;5、待处理）
       bis_cont.bis_cont_id,                                                                   -- 合同id
       bis_cont.cont_no,                                                                       -- 合同编号
       bis_cont.cont_start_date,                                                               -- 合同开始时间
       bis_cont.cont_end_date,                                                                 -- 合同结束时间
       bis_cont.sign_date,                                                                     -- 合同签约时间
       bis_cont.cont_money,                                                                    -- 合同总金额
       bis_cont.pay_way,                                                                       -- 合同计租规则
       bis_cont.PAY_WAY_PROP,                                                                  -- 合同计费规则
       bis_multi.bis_multi_id,                                                                 -- 铺位id
       null                                                       as store_no,                 -- '铺位编号'
       null                                                       as status_cd,                -- '铺位状态'
       null                                                       as inner_square_equity,      -- '铺位套内面积'
       null                                                       as square_equity,            -- '铺位建筑面积'
       null                                                       as rent_square,              -- '铺位计租面积'
       null                                                       as issuing_layout_cd,        -- '合同业态'
       null                                                       as layout_cd,                -- '规划业态'
       bis_floor.floor_num,                                                                    -- '楼层号'
       bis_floor.building_num,                                                                 -- '楼栋号'
       null                                                       as equity_nature,            -- 产权性质
       null                                                       as bd_yye,                   -- 保底营业额
       null                                                       as rent_type,                -- 租赁类型
       null                                                       as store_name,               -- 铺位名称
       '2'                                                        as store_type,               -- 铺位类型 1:店铺 2：多经
       null                                                       as jy_type,                  -- 经营类型
       bis_cont.updated_date                                      as bis_cont_updated_date,    -- 合同更新时间
       bis_project.updated_date                                   as bis_project_updated_date, -- 项目更新时间
       bis_floor.updated_date                                     as bis_floor_updated_date,   -- 楼层更新时间
       datediff(bis_cont.cont_start_date, bis_cont.cont_end_date) as cont_lease,               -- 合同租期
       bis_cont.MULTI_FREE_RENT_PERIOD,                                                        -- 合同免租期
       table1.total_mgr_price,                                                                 -- 合同管理费
       table1.total_rent_price,                                                                -- 合同租金
       table1.total_rent_price / bis_cont.rent_square,                                         -- 合同租金单价
       table1.total_mgr_price / bis_cont.rent_square,                                          -- 合同管理费单价
       bis_cont.rent_square                                       as store_area,               -- 铺位合同面积
       null                                                       as cont_id_rule,             -- 合同ID规则
       null                                                       as cont_code_rule,           -- 合同编号规则
       null                                                       as cont_freeze_date,         -- 合同冻结时间
       null                                                       as cont_unfreeze_date,       -- 合同解冻时间
       null                                                       as cont_subsidy_type,        -- 合同补贴类型
       null                                                       as asset_code,               -- 资产编号
       null                                                       as asset_code_rule,          -- 资产编号规则
       null                                                       as store_id_rule,            -- 铺位id规则
       null                                                       as store_code_rule,          -- 铺位编号规则
       null                                                       as expected_turnover,        -- 预期营业额
       null                                                       as actual_turnover,          -- 实际营业额
       null                                                       as excrete_record_count,     -- 拆铺次数
       null                                                       as merge_record_count,       -- 合铺次数
       null                                                       as excrete_record_date,      -- 拆铺时间
       null                                                       as merge_record_date,        -- 合铺时间
       null                                                       as is_delete,                -- 是否删除
       bis_multi.updated_date,                                                                 -- 更新时间
       bis_multi.updator,                                                                      -- 更新人员
       bis_multi.created_date,                                                                 -- 创建时间
       bis_multi.creator,                                                                      -- 创建人
       "oracle"                                                   as source                    -- 数据来源

from ods.ods_pl_powerdes_bis_multi_dt bis_multi

         left join ods.ods_pl_powerdes_bis_cont_dt bis_cont
                   on bis_multi.bis_cont_id = bis_cont.bis_cont_id
         left join ods.ods_pl_powerdes_bis_project_dt bis_project
                   on bis_multi.bis_project_id = bis_project.bis_project_id
         left join ods.ods_pl_powerdes_bis_floor_dt bis_floor
                   on bis_multi.bis_floor_id = bis_floor.bis_floor_id
         left join
     (
         select bis_cont.bis_cont_id,
                sum(bis_store_price.rent_price * bis_store.rent_square) as total_rent_price, -- 合同租金
                sum(bis_store_price.mgr_price * bis_store.rent_square)  as total_mgr_price   -- 合同管理费
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


