-- -- 1 意向  2：蓄水  3：意向金  4： 商务  5： 合同、完成   6：审图  7： 进场  8： 开业
select t.bis_project_id,                                -- 项目id
       t.short_name,                                    -- 项目名称
       max(t.OPEN_DATE)              OPEN_DATE,         -- 开业时间
       max(t.real_completed_ym)      real_completed_ym, -- 进度完成时间（YYYY-MM）
       max(table1.total_SQUARE)      total_SQUARE,      -- 总建筑面积
       max(table1.total_RENT_SQUARE) total_RENT_SQUARE, -- 总计租面积
       max(table2.total_brand_count) total_brand_count, -- 总品牌数量
       sum(shop_cnt_1)      as       shop_cnt_1,        -- 意向品牌数量
       sum(rent_square_1)   as       rent_square_1,     -- 意向计租面积
       sum(SQUARE_EQUITY_1) as       SQUARE_EQUITY_1,   -- 意向建筑面积
       sum(shop_cnt_2)      as       shop_cnt_2,        -- 蓄水品牌数量
       sum(rent_square_2)   as       rent_square_2,     -- 蓄水计租面积
       sum(SQUARE_EQUITY_2) as       SQUARE_EQUITY_2,   -- 蓄水建筑面积
       sum(shop_cnt_3)      as       shop_cnt_3,        -- 意向金品牌数
       sum(rent_square_3)   as       rent_square_3,     -- 意向金计租面积
       sum(SQUARE_EQUITY_3) as       SQUARE_EQUITY_3,   -- 意向金建筑面积
       sum(shop_cnt_4)      as       shop_cnt_4,        -- 商务品牌数
       sum(rent_square_4)   as       rent_square_4,     -- 商务计租面积
       sum(SQUARE_EQUITY_4) as       SQUARE_EQUITY_4,   -- 商务建筑面积
       sum(shop_cnt_5)      as       shop_cnt_5,        -- 合同、完成 品牌数
       sum(rent_square_5)   as       rent_square_5,     -- 合同、完成 计租面积
       sum(SQUARE_EQUITY_5) as       SQUARE_EQUITY_5,   -- 合同、完成 建筑面积
       sum(shop_cnt_6)      as       shop_cnt_6,        -- 审图品牌数
       sum(rent_square_6)   as       rent_square_6,     -- 审图计租面积
       sum(SQUARE_EQUITY_6) as       SQUARE_EQUITY_6,   -- 审图建筑面积
       sum(shop_cnt_7)      as       shop_cnt_7,        -- 进场品牌数
       sum(rent_square_7)   as       rent_square_7,     -- 进场计租面积
       sum(SQUARE_EQUITY_7) as       SQUARE_EQUITY_7,   -- 进场建筑面积
       sum(shop_cnt_8)      as       shop_cnt_8,        -- 开业品牌数
       sum(rent_square_8)   as       rent_square_8,     -- 开业计租面积
       sum(SQUARE_EQUITY_8) as       SQUARE_EQUITY_8,   -- 开业建筑面积
       max(table3.total_barnd)       total_barnd,       -- 合同网批品牌数
       max(table3.total_rent_square) total_rent_square, -- 合同网批计租面积
       max(table3.total_square)      total_square,      -- 合同网批建筑面积
       max(table4.total_barnd)       total_barnd,       -- 合同签署品牌数
       max(table4.total_rent_square) total_rent_square, -- 合同签署计租面积
       max(table4.total_square)      total_square,      -- 合同签署建筑面积
       null                 as       initial_charge_p,  -- 品牌首期费用
       null                 as       initial_charge_j,  -- 建筑面积首期费用
       null                 as       initial_charge_jz  -- 计租面积首期费用
from (
         select bis_project_id,
                short_name,
                real_completed_ym,                                                             -- 进度完成时间
                max(OPEN_DATE)                                             as OPEN_DATE,       -- 开业时间
                -- -- 1 意向  2：蓄水  3：意向金  4： 商务  5： 合同、完成   6：审图  7： 开业  8： 进场
                case when step_no = '1' then count(bis_shop_id) else 0 end as shop_cnt_1,      -- 品牌数
                case when step_no = '1' then sum(rent_square) else 0 end   as rent_square_1,   -- 计租面积之和
                case when step_no = '1' then sum(SQUARE_EQUITY) else 0 end as SQUARE_EQUITY_1, -- 建筑面积之和

                case when step_no = '2' then count(bis_shop_id) else 0 end as shop_cnt_2,
                case when step_no = '2' then sum(rent_square) else 0 end   as rent_square_2,
                case when step_no = '2' then sum(SQUARE_EQUITY) else 0 end as SQUARE_EQUITY_2,

                case when step_no = '3' then count(bis_shop_id) else 0 end as shop_cnt_3,
                case when step_no = '3' then max(rent_square) else 0 end   as rent_square_3,
                case when step_no = '3' then sum(SQUARE_EQUITY) else 0 end as SQUARE_EQUITY_3,

                case when step_no = '4' then count(bis_shop_id) else 0 end as shop_cnt_4,
                case when step_no = '4' then max(rent_square) else 0 end   as rent_square_4,
                case when step_no = '4' then sum(SQUARE_EQUITY) else 0 end as SQUARE_EQUITY_4,

                case when step_no = '5' then count(bis_shop_id) else 0 end as shop_cnt_5,
                case when step_no = '5' then max(rent_square) else 0 end   as rent_square_5,
                case when step_no = '5' then sum(SQUARE_EQUITY) else 0 end as SQUARE_EQUITY_5,

                case when step_no = '6' then count(bis_shop_id) else 0 end as shop_cnt_6,
                case when step_no = '6' then max(rent_square) else 0 end   as rent_square_6,
                case when step_no = '6' then sum(SQUARE_EQUITY) else 0 end as SQUARE_EQUITY_6,

                case when step_no = '7' then count(bis_shop_id) else 0 end as shop_cnt_7,
                case when step_no = '7' then max(rent_square) else 0 end   as rent_square_7,
                case when step_no = '7' then sum(SQUARE_EQUITY) else 0 end as SQUARE_EQUITY_7,

                case when step_no = '8' then count(bis_shop_id) else 0 end as shop_cnt_8,
                case when step_no = '8' then max(rent_square) else 0 end   as rent_square_8,
                case when step_no = '8' then sum(SQUARE_EQUITY) else 0 end as SQUARE_EQUITY_8
         from (
                  -- 明细
                  select st.bis_project_id,      -- 项目id
                         st.short_name,          -- 项目名称
                         zsps.real_completed_ym, -- 进度完成时间
                         st.bis_store_id,        -- 铺位
                         st.store_no,            -- 铺位号
                         st.rent_square,         -- 铺位计租面积
                         st.SQUARE_EQUITY,       -- 铺位建筑面积
                         st.layout_cd,
                         st.issuing_layout_cd,
                         st.charge_type,
                         st.building_num,        -- 铺位所属楼栋
                         st.floor_num,           --铺位所属楼层
                         st.effect_date,
                         zsp.bis_shop_id,        -- 铺位对应的品牌
                         zsps.step_no,           -- 进度号
                         zsps.step_display_name,
                         st.open_date            -- 项目开业时间
                  from (
                           select prj.bis_project_id,
                                  prj.short_name,
                                  prj.oper_status,
                                  prj.OPEN_DATE,
                                  st.bis_store_id,  -- 铺位
                                  st.store_no,
                                  st.rent_square,   -- 计租面积
                                  st.SQUARE_EQUITY, -- 建筑面积
                                  st.layout_cd,
                                  st.issuing_layout_cd,
                                  st.effect_date,
                                  fl.charge_type,
                                  fl.building_num,
                                  fl.floor_num
                           from (select bis_project_id,
                                        short_name,
                                        oper_status,
                                        open_date
                                 from ODS.ods_pl_powerdes_bis_project_dt bis_project
                                 where OPER_STATUS = '1'
                                   and is_business_project = '1' -- 筹备期项目  -- 商业项目
                                ) prj
                                    left join
                                (select bis_project_id,
                                        bis_floor_id,
                                        bis_store_id,  -- 铺位
                                        store_no,
                                        rent_square,   -- 计租面积
                                        SQUARE_EQUITY, -- 建筑面积
                                        layout_cd,
                                        issuing_layout_cd,
                                        equity_nature,
                                        management_status,
                                        store_position,
                                        effect_date
                                 from ODS.ods_pl_powerdes_bis_store_dt bis_store
                                 where is_delete = 'N'
                                   and status_cd = '1'
                                   -- /*产权性质是自持；或产权性质是销售，但返租的*/
                                   and (EQUITY_NATURE = '1' or (EQUITY_NATURE = '2' and MANAGEMENT_STATUS = '1'))
                                   -- /*考核商铺*/
                                   and IS_ASSESS = 'Y'
                                ) st
                                on prj.bis_project_id = st.bis_project_id
                                    left outer join
                                (select bis_floor_id,
                                        charge_type,
                                        building_num,
                                        floor_num
                                 from ODS.ods_pl_powerdes_bis_floor_dt bis_floor
                                ) fl
                                on st.bis_floor_id = fl.bis_floor_id) st
                           left outer join
                       (select bis_store_id, -- 招商铺位
                               zs_plan_id
                        from ODS.ods_pl_powerdes_bis_store_zs_plan_rel_dt bis_store_zs_plan_rel
                       ) rel
                       on rel.bis_store_id = st.bis_store_id
                           left outer join
                       (select id,
                               project_id,
                               bis_shop_id -- 招商品牌
                        from ODS.ods_pl_powerdes_zs_plan_dt zs_plan
                        where del_flag <> '1'
                          and zs_status in ('1', '2')
                          and zs_plan_step_no <> '-1' -- 进度号
                       ) zsp
                       on rel.zs_plan_id = zsp.id
                           left outer join
                       (select zs_plan_id,
                               step_no, -- 进度号
                               step_display_name,
                               real_completed_date,
                               substr(real_completed_date, 0, 7) real_completed_ym
                        from ODS.ods_pl_powerdes_zs_plan_step_dt zs_plan_step
                        where status = '1'
                       ) zsps
                       on zsps.zs_plan_id = zsp.id
                           left outer join
                       (select bis_shop.bis_shop_id -- 品牌
                        from ODS.ods_pl_powerdes_bis_shop_dt bis_shop
                       ) shst
                       on zsp.bis_shop_id = shst.bis_shop_id
              ) detail
         group by bis_project_id, short_name, step_no, real_completed_ym
     ) t
         left join
     (
         select nvl(sum(t.rent_square), 0)   as total_RENT_SQUARE, -- 计租面积,
                nvl(sum(t.SQUARE_EQUITY), 0) as total_SQUARE,      -- 建筑面积,
                max(bp.PROJECT_NAME),
                bp.BIS_PROJECT_ID
         from ods.ods_pl_powerdes_bis_store_dt t,
              ods.ods_pl_powerdes_bis_floor_dt bf,
              ods.ods_pl_powerdes_bis_project_dt bp
         where t.bis_floor_id = bf.bis_floor_id
           and bf.bis_project_id = bp.bis_project_id
           and t.status_cd = '1'
           and t.IS_DELETE = 'N'
           and bf.charge_type = '1'
           and bp.OPER_STATUS = '1'
           and bp.is_business_project = '1'
         group by bp.BIS_PROJECT_ID
     ) table1 on t.bis_project_id = table1.bis_project_id
         left join
     (
         select BIS_PROJECT_ID,
                count(BIS_SHOP_ID) as total_brand_count -- 品牌数
         from ODS.ods_pl_powerdes_bis_cont_dt BIS_CONT
         where EFFECT_FLG <> 'D'
           and STATUS_CD in ('1', '2')
         group by BIS_PROJECT_ID
     ) table2 on t.bis_project_id = table2.bis_project_id
         left join
     ( -- 合同网批
         select BIS_CONT.BIS_PROJECT_ID,
                count(BIS_CONT.BIS_SHOP_ID) as total_barnd,      -- 品牌数量
                sum(SQUARE)                 as total_square,     -- 建筑面积
                sum(RENT_SQUARE)            as total_rent_square -- 计租面积
         from ODS.ods_pl_powerdes_zs_plan_step_dt ZS_PLAN_STEP
                  left join ODS.ods_pl_powerdes_res_approve_info_dt RES_APPROVE_INFO
                            on STRING(RES_APPROVE_INFO.DISPLAY_NO) = ZS_PLAN_STEP.DISPLAY_NO
                  left join ODS.ods_pl_powerdes_bis_cont_dt bis_cont
                            on BIS_CONT.RES_APPROVE_INFO_ID = RES_APPROVE_INFO.RES_APPROVE_INFO_ID
         where length(ZS_PLAN_STEP.DISPLAY_NO) > 0
           and ZS_PLAN_STEP.STEP_NO = '5'
           and BIS_CONT.EFFECT_FLG <> 'D'
         group by BIS_CONT.BIS_PROJECT_ID
     ) table3 on table3.BIS_PROJECT_ID = t.BIS_PROJECT_ID

         left join
     ( -- 合同签署
         select BIS_CONT.BIS_PROJECT_ID,
                count(BIS_CONT.BIS_SHOP_ID) as total_barnd,      -- 品牌数量
                sum(SQUARE)                 as total_square,     -- 建筑面积
                sum(RENT_SQUARE)            as total_rent_square -- 计租面积
         from ODS.ods_pl_powerdes_zs_plan_step_dt ZS_PLAN_STEP
                  left join ODS.ods_pl_powerdes_res_approve_info_dt RES_APPROVE_INFO
                            on STRING(RES_APPROVE_INFO.DISPLAY_NO) = ZS_PLAN_STEP.DISPLAY_NO
                  left join ODS.ods_pl_powerdes_bis_cont_dt bis_cont
                            on BIS_CONT.RES_APPROVE_INFO_ID = RES_APPROVE_INFO.RES_APPROVE_INFO_ID
         where length(ZS_PLAN_STEP.DISPLAY_NO) > 0
           and ZS_PLAN_STEP.STEP_NO = '5'
           and BIS_CONT.STATUS_CD = '1'
           and BIS_CONT.EFFECT_FLG <> 'D'
         group by BIS_CONT.BIS_PROJECT_ID
     ) table4 on table4.BIS_PROJECT_ID = t.BIS_PROJECT_ID
group by t.bis_project_id, t.short_name, t.real_completed_ym;




