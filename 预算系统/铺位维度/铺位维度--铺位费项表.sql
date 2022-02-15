/*
 * A:铺位维度--铺位费项表
 dws_bis_store_width_big
 */

select bis_project.bis_project_id,                                                                          -- 项目id
       bis_floor.bis_floor_id,                                                                              -- 楼层id
       bis_floor.building_id,                                                                               -- 楼栋id
       bis_project.project_name,                                                                            -- '所在项目'
       bis_cont.status_cd                                         as                    manage_status,      -- 经营状态（0：未签约；1：已审核；2：已解约；3：未审核;4：无效合同（补充商务条件产生）;5、待处理）
       bis_cont.bis_cont_id,                                                                                -- 合同id
       bis_cont.cont_no,                                                                                    -- '合同编号'
       bis_cont.cont_start_date,                                                                            -- '合同开始时间'
       bis_cont.cont_end_date,                                                                              -- '合同结束时间'
       bis_cont.sign_date,                                                                                  -- '合同签约时间'
       bis_cont.cont_money,                                                                                 -- '合同总金额'
       bis_cont.pay_way,                                                                                    -- 租金方式
       bis_cont.PAY_WAY_PROP,                                                                               -- 计费方式
       nvl(bis_store.BIS_STORE_ID, bis_multi.bis_multi_id),                                                 -- 铺位id
       nvl(bis_store.store_no, bis_multi.multi_name),                                                       -- '铺位编号'
       bis_store.status_cd,                                                                                 -- '铺位状态'
       nvl(bis_store.inner_square_equity, 0),                                                               -- '铺位套内面积'
       nvl(bis_store.square_equity, bis_multi.max_area),                                                    -- '铺位建筑面积'
       nvl(bis_store.rent_square, bis_multi.square),                                                        -- '铺位计租面积'
       bis_store.issuing_layout_cd,                                                                         -- '签约业态'
       bis_store.layout_cd,                                                                                 -- '规划业态'
       nvl(bis_floor.building_num, bis_floor2.building_num),                                                -- 楼栋名称
       nvl(bis_floor.floor_num, bis_floor2.floor_num),                                                      -- 楼层名称
       bis_store.equity_nature,                                                                             -- 产权性质
       table11.discount_rate,                                                                               -- 扣率
       null                                                       as                    bd_yye,             -- 保底营业额
       null                                                       as                    rent_type,          -- 租赁类型
       null                                                       as                    store_name,         -- 铺位名称
       bis_store_merge.store_type,                                                                          -- 铺位类型 1:店铺 2：多经
       null                                                       as                    jy_type,            -- 经营类型
       table11.qz_year_month,                                                                               -- 全责月
       table11.fee_type,                                                                                    -- 费项
       table11.must_money,                                                                                  --每月应收
       table11.fact_money,                                                                                  -- 每月实收
       bis_cont.multi_pay_date,                                                                             -- 交付日
       bis_cont.multi_decora_start_date,                                                                    -- 装修开始时间
       bis_cont.multi_decora_end_date,                                                                      -- 装修结束时间
       null                                                       as                    start_rent_date,    -- 起租日
       null                                                       as                    actual_lease,       --  实际租期
       null                                                       as                    calendar_month,     -- 自然月
       null                                                       as                    customize_month,    -- 自定义月
       null                                                       as                    incompletion_month, -- 不完全月
       bis_cont.rent_start,                                                                                 -- 计租起始日
       datediff(bis_cont.cont_start_date, bis_cont.cont_end_date) as                    tenancy_term,       -- 租期
       bis_cont.pay_cycle_cd,                                                                               -- 计费周期
       add_months(bis_cont.rent_start_date, 12 * (cast(bis_must_rent.YEAR as int) - 1)) start_rent_free,-- 免租期开始时间
       add_months(bis_cont.rent_start_date,cast(nvl(REGEXP_REPLACE(free_rent_period, '[^0-9]', ''), 0) as int))  end_rent_free,      -- 免租期结束时间
       (add_months(bis_cont.rent_start_date,cast(nvl(REGEXP_REPLACE(free_rent_period, '[^0-9]', ''), 0) as int)) - add_months(bis_cont.rent_start_date, 12 * (cast(bis_must_rent.YEAR as int) - 1))) as rent_free_date, -- 免租期
       bis_store_merge.IS_DEL,                                                                              -- 是否删除
       bis_store_merge.updated_date,                                                                        -- 更新时间
       bis_store_merge.updator,                                                                             -- 更新人员
       bis_store_merge.created_date,                                                                        -- 创建时间
       bis_store_merge.creator,                                                                             -- 创建人
       "oracle"                                                   as                    source              -- 数据来源

from (
         select bis_store.bis_store_id,
                '1'                 as store_type,
                bis_store.is_delete as IS_DEL, -- 是否删除
                bis_store.updated_date,        -- 更新时间
                bis_store.updator,             -- 更新人员
                bis_store.created_date,        -- 创建时间
                bis_store.creator,             -- 创建人
                "oracle"            as source  -- 数据来源
         from ods.ods_pl_powerdes_bis_store_dt bis_store
         where bis_store.is_delete = 'N'
           and bis_store.status_cd = '1'
         union
         select bis_multi.bis_multi_id,
                '2'      as store_type,
                null     as IS_DEL,     -- 是否删除
                bis_multi.updated_date, -- 更新时间
                bis_multi.updator,      -- 更新人员
                bis_multi.created_date, -- 创建时间
                bis_multi.creator,      -- 创建人
                "oracle" as source      -- 数据来源
         from ods.ods_pl_powerdes_bis_multi_dt bis_multi
     ) bis_store_merge
         left join ods.ods_pl_powerdes_bis_store_dt bis_store
                   on bis_store_merge.bis_store_id = bis_store.bis_store_id
         left join ods.ods_pl_powerdes_bis_multi_dt bis_multi
                   on bis_multi.bis_multi_id = bis_store_merge.bis_store_id
         LEFT JOIN (
    SELECT tmp.bis_cont_id,
           tmp.bis_store_ids,
           bis_store_id
    FROM (
             SELECT bis_cont_id,
                    bis_store_ids
             FROM ods.ods_pl_powerdes_bis_cont_dt
         ) tmp LATERAL VIEW explode(split(bis_store_ids, ',')) tmp1 AS bis_store_id
) table12
                   on bis_store.bis_store_id = table12.bis_store_id
         left join ods.ods_pl_powerdes_bis_cont_dt bis_cont
                   on table12.bis_cont_id = bis_cont.bis_cont_id
         left join ods.ods_pl_powerdes_bis_project_dt bis_project
                   on bis_store.bis_project_id = bis_project.bis_project_id
         left join ods.ods_pl_powerdes_bis_project_dt bis_project2
                   on bis_multi.bis_project_id = bis_project2.bis_project_id
         left join ods.ods_pl_powerdes_bis_floor_dt bis_floor
                   on bis_store.bis_floor_id = bis_floor.bis_floor_id
         left join ods.ods_pl_powerdes_bis_floor_dt bis_floor2
                   on bis_multi.bis_floor_id = bis_floor2.bis_floor_id
         left join
     (
         select t.bis_project_id, -- 项目id
                t.bis_cont_id,    -- 合同id
                t.bis_store_id,   -- 店铺id
                t.qz_year_month,  -- 全责月
                t.fee_type,       -- 费项
                t.must_money,     --每月应收
                t.fact_money,     -- 每月实收
                t.discount_rate   -- 扣率

         from dws.dws_basic_data_store_rent_big t where t.dt = date_format(current_date(),'yyyyMMdd')
     ) table11 on bis_store_merge.bis_store_id = table11.bis_store_id
         left join ods.ods_pl_powerdes_bis_must_rent_dt bis_must_rent
                   ON bis_cont.BIS_CONT_ID = bis_must_rent.BIS_CONT_ID
where year(add_months(bis_cont.rent_start_date, 12 * (cast(bis_must_rent.YEAR as int) - 1))) = date_format(current_date, 'yyyy');


