/*
  合同大表 ：DWS_BIS_CONT_WIDTH_BIG
*/
select bis_cont_big.bis_project_id,                                     -- 项目id
       bis_cont_big.bis_floor_id,                                       -- 楼层id
       bis_cont_big.building_id,                                        -- 楼栋id
       bis_cont_big.project_name,                                       -- 所属项目
       bis_cont_big.oper_status,                                        -- 营运状态
       bis_cont_big.cont_name,                                          -- 合同名称
       bis_cont_big.bis_cont_id,                                        -- 合同id
       bis_cont_big.cont_no,                                            --合同编号
       bis_cont_big.cont_start_date,                                    --合同开始时间
       bis_cont_big.cont_end_date,                                      -- 合同结束时间
       bis_cont_big.sign_date,                                          -- 合同签约时间
       bis_cont_big.cont_to_fail_date,                                  -- 合同解约时间
       bis_cont_big.fitment_start_date,                                 -- 合同装修开始时间
       bis_cont_big.fitment_end_date,                                   -- 合同装修结束时间
       bis_cont_big.cont_money,                                         -- 合同总金额
       bis_cont_big.pay_way,                                            -- 租金方式
       bis_cont_big.pay_way_prop,                                       -- 计费方式
       bis_cont_big.bis_store_ids,                                      -- 铺位ID
       bis_cont_big.bis_store_nos,                                      -- 铺位编号
       bis_cont_big.inner_square,                                       -- 合同套内面积
       bis_cont_big.square,                                             -- 合同建筑面积
       bis_cont_big.rent_square,                                        -- 合同计租面积
       bis_cont_big.proportion_ids,                                     -- 签约业态
       bis_cont_big.layout_cd,                                          -- 规划业态
       bis_cont_big.floor_num,                                          -- 所属楼层
       bis_cont_big.building_num,                                       -- 所属楼栋
       bis_cont_big.equity_nature,                                      -- 产权性质
       mst.discount_rate,                                               -- 扣率
       bis_cont_big.bd_yye,                                             -- 保底营业额
       bis_cont_big.rent_type,                                          -- 租赁类型
       bis_cont_big.brandName,                                          -- 品牌
       bis_cont_big.companyName,                                        -- 商家
       bis_cont_big.status_cd,                                          -- 合同审批状态
       bis_cont_big.rent_date,                                          -- 计租开始时间
       bis_cont_big.actual_open_date,                                   -- 预计进场时间
       bis_cont_big.cont_total_time,                                    -- 合同总时长
       bis_cont_big.commission,                                         -- 佣金
       bis_cont_big.commission_destiny,                                 -- 佣金点数
       bis_cont_big.cont_type,                                          -- 合同类别
       bis_cont_big.store_type,                                         -- 物业类型
       bis_cont_big.management_forms,                                   -- 经营状态
       bis_cont_big.manage_cd,                                          -- 商家性质
       coalesce(mst.qz_year_month, adj.qz_year_month) as qz_year_month, -- 权责月
       coalesce(mst.must_type, adj.fee_type)          as fee_type,      -- 费项
       (nvl(mst.must_base, 0) + nvl(adj.must_adj, 0)) as must_money,    -- 每月应收
       (nvl(fct.fact_base, 0) + nvl(adj.fact_adj, 0)) as fact_money,    -- 每月实收
       bis_cont_big.IS_DEL,                                             -- 是否删除
       bis_cont_big.updated_date,                                       -- 更新时间
       bis_cont_big.updator,                                            -- 更新人员
       bis_cont_big.created_date,                                       -- 创建时间
       bis_cont_big.creator,                                            -- 创建人
       bis_cont_big.source                                              -- 数据来源

from dws.dws_bis_cont_big bis_cont_big
         inner join
     -- 应收费用表
         (select bis_cont_id,
                 qz_year_month,
                 must_type,
                 max(discount_rate) as   discount_rate,
                 sum(money)              must_base,
                 max(rent_last_pay_date) must_date
          from ods.ods_pl_powerdes_bis_must2_dt
          where (is_delete = '0' or is_delete is null)
            --and must_type in ('1', '2', '34') -- 34为多经场地使用费
            and must_type is not null
          group by bis_cont_id, qz_year_month, must_type
         ) mst
     on bis_cont_big.bis_cont_id = mst.bis_cont_id
         left outer join
     -- 实收费用表
         (select bis_cont_id,
                 qz_year_month,
                 fact_type,
                 sum(money)                                                fact_base,
                 sum(case when fact_method = '8' then money else 0 end) as fact_hexiao, -- 核销收入
                 max(fact_date)                                            fact_date
          from ods.ods_pl_powerdes_bis_fact2_dt
          where is_delete = '1' -- 实收表is_delete = '1'为有效
            --and fact_type in ('1', '2', '34')
            and fact_type is not null
          group by bis_cont_id, qz_year_month, fact_type
         ) fct
     on mst.bis_cont_id = fct.bis_cont_id and mst.qz_year_month = fct.qz_year_month and
        mst.must_type = fct.fact_type
         left outer join
     -- 调整费用表
         (select bis_cont_id,
                 qz_year_month,
                 fee_type,
                 sum(case when adjust_type = '1' then adjust_money else 0 end) as must_adj, -- 调整类型（1：应收，2：实收）
                 sum(case when adjust_type = '2' then adjust_money else 0 end) as fact_adj
          from ods.ods_pl_powerdes_bis_mf_adjust_dt
          where (is_del = '1' or is_del is null) -- 是否删除（0：是，1：否）
            --and fee_type in ('1', '2', '34')
            and fee_type is not null
          group by bis_cont_id, qz_year_month, fee_type
         ) adj
     on mst.bis_cont_id = adj.bis_cont_id and mst.qz_year_month = adj.qz_year_month and
        mst.must_type = adj.fee_type
where bis_cont_big.dt = date_format(current_date(),'yyyyMMdd');
