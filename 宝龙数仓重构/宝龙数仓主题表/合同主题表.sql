select t2.area_name,                                                                             -- 项目区域
       t2.id,                                                                                    -- 区域id
       bis_project.stage,                                                                        -- 项目阶段('1、筹备期 2、培育期 3、稳定期 4、调改期')
       bis_project.province,                                                                     -- 项目所属省份
       BIS_PROJECT.PROJECT_NAME,                                                                 -- 项目名称
       bis_project.short_name,                                                                   -- 项目简称
       bis_project.bis_project_id,                                                               -- 项目id
       bis_project.is_business_project,                                                          -- 是否是商业项目(1:商业项目)
       bis_project.oper_status,                                                                  -- '项目状态（1：在建；2：在营）'
       bis_project.open_date,                                                                    -- 项目开业时间
       BIS_CONT.bis_cont_id,                                                                     -- 合同id
       BIS_CONT.cont_no,                                                                         -- 合同号
       table1.bis_shop_id,                                                                       -- 商家id
       nvl(table1.cooperative_brand, BIS_CONT.bis_shop_name),                                    -- 商家名称
       table1.company_name,                                                                      -- 品牌集团名称
       table1.bis_shop_status,                                                                   -- 商家状态（有效 or 无效）
       if(table1.bis_shop_id is null, t6.primary_forms, table1.primary_forms),                   -- 一级业态
       if(table1.bis_shop_id is null, t6.secondary_formats, table1.secondary_formats),           -- 二级业态
       if(table1.bis_shop_id is null, t6.thirdly_formats, table1.thirdly_formats),               -- 三级业态
       if(table1.bis_shop_id is null, t6.primary_forms_code, table1.primary_forms_code),         -- 一级业态code
       if(table1.bis_shop_id is null, t6.secondary_formats_code, table1.secondary_formats_code), -- 二级业态code
       if(table1.bis_shop_id is null, t6.thirdly_formats_code, table1.thirdly_formats_code),     -- 三级业态code
       BIS_CONT.cont_start_date,                                                                 -- 合同开始时间
       BIS_CONT.cont_end_date,                                                                   -- 合同结束时间
       BIS_CONT.cont_to_fail_date,                                                               -- 合同解约时间
       BIS_CONT.sign_date,                                                                       -- 合同签约时间
       BIS_CONT.fitment_start_date,                                                              -- 合同装修开始时间
       BIS_CONT.fitment_end_date,                                                                -- 合同装修结束时间
       bis_store.bis_store_id,                                                                   -- 铺位id
       bis_store.store_no,                                                                       -- 铺位号
       BIS_CONT.RENT_SQUARE,                                                                     -- 合同计租面积
       BIS_CONT.square,                                                                          -- 合同建筑面积
       bis_cont.inner_square,                                                                    -- 合同套内面积
       bis_cont.equity_nature,                                                                   -- 合同产权性质
       bis_cont.status_cd,                                                                       -- 合同状态'（0：未签约；1：已审核；2：已解约；3：未审核;4：无效合同（补充商务条件产生）;5、待处理）'
       bis_cont.cont_type_cd,                                                                    -- 合同类别 [1:自持商铺合同 2:已售商铺合同 3:多经合同 4:广告位合同]
       BIS_CONT.store_type,                                                                      -- 物业类型 1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼,
       BIS_CONT.manage_cd,                                                                       -- 商家性质 ,1主力店 2次主力店 3大商铺 4小商铺 5一般商铺  6中型商铺 7商业街'
       table3.free_rent_period,                                                                  -- 合同免租期
       bis_floor.bis_floor_id,                                                                   -- 楼层id
       bis_floor.floor_num,                                                                      -- 楼层名称
       bis_floor.building_id,                                                                    -- 楼栋id
       bis_floor.building_num,                                                                   -- 楼栋名称
       bis_store.rent_square,                                                                    -- 铺位计租面积
       bis_store.is_assess,                                                                      -- 铺位是否考核(bis_store.IS_ASSESS = 'Y'为考核)
       nvl(t4.open_date, bis_shop_open_info.open_date),                                          -- 开业网批中的开业日期
       t3.attach_num,                                                                            -- 合同双签数量(双签数量>0 才是双签合同)
       nvl(t4.complete_date, bis_shop_open_info.open_date),                                      -- 开业网批完成时间
       nvl(t4.real_open_date, bis_shop_open_info.open_date),                                     -- 已开业日期：开业网批完成时间、开业网批中的开业日期，两者日期相比取后面的日期。
       t3.contract_no,                                                                           -- 合同文本库号
       bis_store.equity_nature,                                                                  -- 铺位产权性质(1,自持；2,可售;3,自持/销售;4,委托管理)
       bis_store.rent_status,                                                                    -- 租赁性质(1,已租；2,未租)
       bis_store.management_status,                                                              -- 是否返祖（1，返租；2，不返租)
       bis_store.STORE_POSITION,                                                                 -- 商铺定位(1主力店-百货 2主力店-超市 3主力店-影院 5大租户 4次主力店 6中租户 7小租户 8商业街')
       t3.created_date,                                                                          -- 双签合同上传日期
       t5.COMPLETE_DATE,                                                                         -- 合同网批通过时间
       BIS_CONT.EFFECT_FLG,                                                                      -- 合同状态（Y:生效 N:失效）
       bis_store.LAST_DATE,                                                                      -- 铺位返租到期日期
       BIS_CONT.pay_cycle_cd,                                                                    -- 租金支付周期：1:月付 2:季付
       BIS_CONT.pay_way,                                                                         -- 租金支付方式：1:固定租金 2:提成租金 3:两者取高 4:其他
       BIS_CONT.PROP_PAY_CYCLE_CD,                                                               -- 物业费支付周期：1 : 2:  3：
       bis_cont.RENT_YEARS,                                                                      -- 合同租期
       bis_cont.RES_APPROVE_ID1,                                                                 -- 商务条件网批ID
       bis_cont.RES_APPROVE_ID2,                                                                 -- 租赁合同网批ID
       bis_cont.APPROVE_ID,                                                                      -- 解约网批ID
       bis_cont.CONTRACT_TEMPLET_INFO_ID2,                                                       -- 合同文本库id
       t4.res_approve_info_id,                                                                   -- 开业网批id
       bis_cont.ACTUAL_OPEN_DATE,                                                                -- 约定开业时间
       bis_cont.LAST_OPEN_DATE,                                                                  -- 最迟开业时间
       bis_cont.IS_CREATE_RENT,                                                                  -- 0:不生成租金 1：生成租金
       bis_cont.IS_CREATE_MANAGER_FEE,                                                           -- 0:不生成物管费 1：生成物管费
       bis_cont.MULTI_CHARGE_TYPE,                                                               -- 多经类型
       current_date                                                                              -- ETL时间
from ods.ods_pl_powerdes_bis_cont_dt BIS_CONT
         left join
     (
         select t3.bis_shop_id,
                t3.primary_forms,             -- 一级业态
                t3.secondary_formats,         -- 二级业态
                t3.thirdly_formats,           -- 三级业态
                t3.primary_forms_code,        -- 一级业态code
                t3.secondary_formats_code,    -- 二级业态code
                t3.thirdly_formats_code,      -- 三级业态code
                t3.name_cn cooperative_brand, -- 商家名称
                t3.company_name,              -- 品牌集团名称
                t3.bis_shop_status            -- 商家状态
         from dwd.dwd_bis_shop_primary_forms_big_dt t3
         where t3.dt = date_format(current_date, 'yyyyMMdd')
     ) table1 ON table1.BIS_SHOP_ID = BIS_CONT.BIS_SHOP_ID
         left join ods.ods_pl_powerdes_bis_project_dt bis_project
                   on BIS_CONT.BIS_PROJECT_ID = BIS_PROJECT.BIS_PROJECT_ID

         left join
     (
         select BIS_CONT_ID,
                concat_ws(",", collect_list(free_rent_period)) free_rent_period
         from ods.ods_pl_powerdes_bis_must_rent_dt BIS_MUST_RENT
         group by BIS_CONT_ID
     ) table3 on BIS_CONT.BIS_CONT_ID = table3.BIS_CONT_ID
         left join
     (
         select bs_area.id,
                bs_area.area_name,  -- 区域名称
                bs_mall.area,
                bs_mall.out_mall_id -- 项目id
         from ods.ods_pl_pms_bis_db_bs_area_dt bs_area
                  left join ods.ods_pl_pms_bis_db_bs_mall_dt bs_mall
                            on bs_area.id = bs_mall.area
         where bs_mall.is_del = '0'
           and stat = '2'
     ) t2 on bis_project.bis_project_id = t2.out_mall_id
         left join ods.ods_pl_powerdes_bis_store_dt bis_store
                   on BIS_CONT.bis_store_ids = bis_store.bis_store_id and
                      BIS_CONT.bis_project_id = bis_store.bis_project_id
         left join ods.ods_pl_powerdes_bis_floor_dt bis_floor
                   on bis_store.bis_floor_id = bis_floor.bis_floor_id and
                      bis_store.bis_project_id = bis_floor.bis_project_id
         left join
     (
         -- 计算是否是双签合同
         select distinct bis_cont.bis_project_id,              -- 项目id
                         bis_cont.bis_cont_id,                 -- 合同id
                         bis_cont.cont_no,                     -- 合同号
                         bis_cont.cont_type_cd,                -- 合同状态
                         bis_cont.status_cd,                   -- 合同类型
                         bis_cont.rent_square open_area,       -- 已开业面积
                         bis_cont.contract_templet_info_id2,
                         bis_cont.STORE_TYPE,                  -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                         bis_cont.cont_start_date,             -- 合同开始时间
                         bis_cont.cont_end_date,               -- 合同结束时间
                         bis_cont.cont_to_fail_date,           -- 合同解约时间
                         BIS_SHOP_OPEN_INFO.OPEN_DATE,         -- 合同开业时间
                         SC_CONTRACT_TEMPLET_INFO.contract_templet_info_id,
                         SC_CONTRACT_TEMPLET_INFO.contract_no, -- 合同文本库号
                         b.attach_num,                         -- 双签数量(双签数量>0 双签合同)
                         b.created_date                        -- 双签时间

         from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                  left join ods.ods_pl_powerdes_bis_shop_open_info_dt BIS_SHOP_OPEN_INFO
                            on bis_cont.BIS_CONT_ID = BIS_SHOP_OPEN_INFO.BIS_CONT_ID and DEL_FLAG = '0'
                  left join ods.ods_pl_powerdes_SC_CONTRACT_TEMPLET_INFO_dt SC_CONTRACT_TEMPLET_INFO
                            on bis_cont.contract_templet_info_id2 =
                               SC_CONTRACT_TEMPLET_INFO.contract_templet_info_id and
                               nvl(SC_CONTRACT_TEMPLET_INFO.is_del, 0) <> '1'
                  left join
              (
                  select count(*)                                  attach_num, -- 双签数量
                         SC_CONTRACT_INFO_ATTACH.contract_templet_info_id,
                         SC_CONTRACT_INFO_ATTACH.attach_type_cd,
                         max(SC_CONTRACT_INFO_ATTACH.created_date) created_date
                  from ods.ods_pl_powerdes_sc_contract_info_attach_dt SC_CONTRACT_INFO_ATTACH
                  where SC_CONTRACT_INFO_ATTACH.attach_type_cd = '990'
                  group by SC_CONTRACT_INFO_ATTACH.contract_templet_info_id, SC_CONTRACT_INFO_ATTACH.attach_type_cd
              ) b
              on SC_CONTRACT_TEMPLET_INFO.contract_templet_info_id = b.contract_templet_info_id
         where bis_cont.EFFECT_FLG <> 'D'
     ) t3 on BIS_CONT.bis_project_id = t3.bis_project_id and BIS_CONT.bis_cont_id = t3.bis_cont_id
         left join
     (
         -- 开业网批
         select a.BIS_CONT_ID,
                a.OPEN_DATE,
                RES_APPROVE_INFO.res_approve_info_id, -- 开业网批id
                RES_APPROVE_INFO.COMPLETE_DATE,
                case
                    when a.OPEN_DATE > RES_APPROVE_INFO.COMPLETE_DATE then a.OPEN_DATE
                    else RES_APPROVE_INFO.COMPLETE_DATE end as real_open_date
         from ods.ods_pl_powerdes_res_approve_info_dt RES_APPROVE_INFO
                  left join ods.ods_pl_powerdes_bis_shop_open_info_dt a
                            on a.OPEN_DISPLAY_NO = RES_APPROVE_INFO.DISPLAY_NO
         where RES_APPROVE_INFO.status_cd = '2'
           and (del_flag = '0' or del_flag is null)
     ) t4 on BIS_CONT.bis_cont_id = t4.bis_cont_id
         left join
     (
         select bis_cont_id,
                max(open_date) open_date
         from ods.ods_pl_powerdes_bis_shop_open_info_dt
         where (del_flag = '0' or del_flag is null)
         group by bis_cont_id
     ) bis_shop_open_info on BIS_CONT.bis_cont_id = bis_shop_open_info.bis_cont_id
         left join
     (
         -- 合同网批完成时间
         select BIS_CONT.BIS_PROJECT_ID,
                BIS_CONT.BIS_CONT_ID,
                RES_APPROVE_INFO.COMPLETE_DATE -- 合同网批通过时间
         from ods.ods_pl_powerdes_zs_plan_step_dt ZS_PLAN_STEP
                  left join ods.ods_pl_powerdes_res_approve_info_dt RES_APPROVE_INFO
                            on string(RES_APPROVE_INFO.DISPLAY_NO) = ZS_PLAN_STEP.DISPLAY_NO
                  left join ods.ods_pl_powerdes_bis_cont_dt bis_cont
                            on BIS_CONT.RES_APPROVE_INFO_ID = RES_APPROVE_INFO.RES_APPROVE_INFO_ID
         where length(ZS_PLAN_STEP.DISPLAY_NO) > 0
           and ZS_PLAN_STEP.STEP_NO = '5'
           and BIS_CONT.EFFECT_FLG <> 'D'
     ) t5 on BIS_CONT.bis_project_id = t5.bis_project_id and BIS_CONT.bis_cont_id = t5.bis_cont_id
         left join
     ( -- 这里计算的业态是：从合同角度算的，因为有的合同没有品牌
         -- 新业态
         SELECT t1.BIS_SHOP_ID,
                t1.BIS_CONT_ID,
                t1.PROPORTION_NAMES,
                max(t1.SORT_TYPE3)             SORT_TYPE3,
                max(t1.thirdly_formats_code)   thirdly_formats_code,   -- 三级业态code
                max(t1.thirdly_formats)        thirdly_formats,        -- 三级业态
                max(t1.SORT_TYPE2)             SORT_TYPE2,
                max(t1.secondary_formats_code) secondary_formats_code, -- 二级业态code
                max(t1.secondary_formats)      secondary_formats,      -- 二级业态
                max(t1.SORT_TYPE1)             SORT_TYPE1,
                max(t1.primary_forms_code)     primary_forms_code,     -- 一级业态code
                max(t1.primary_forms)          primary_forms           -- 一级业态
         from (
                  select t1.BIS_SHOP_ID,
                         t1.BIS_CONT_ID,
                         t1.PROPORTION_NAMES,
                         t2.SORT_TYPE SORT_TYPE3,
                         t2.LAYOUT_CD thirdly_formats_code,   -- 三级业态code
                         t2.SORT_NAME thirdly_formats,        -- 三级业态
                         t3.SORT_TYPE SORT_TYPE2,
                         t3.LAYOUT_CD secondary_formats_code, -- 二级业态code
                         t3.SORT_NAME secondary_formats,      -- 二级业态
                         t4.SORT_TYPE SORT_TYPE1,
                         t4.LAYOUT_CD primary_forms_code,     -- 一级业态code
                         t4.SORT_NAME primary_forms           -- 一级业态
                  from ods.ods_pl_powerdes_bis_cont_dt t1
                           LEFT JOIN ods.ods_pl_powerdes_bis_shop_sort_new_dt t2
                                     ON t1.PROPORTION_IDS = t2.BIS_SHOP_SORT_ID
                           left join ods.ods_pl_powerdes_bis_shop_sort_new_dt t3 ON t2.PARENT_ID = t3.BIS_SHOP_SORT_ID
                           left join ods.ods_pl_powerdes_bis_shop_sort_new_dt t4 ON t3.PARENT_ID = t4.BIS_SHOP_SORT_ID
                  where PROPORTION_IDS is not null
                    and t4.sort_name is not null
                    and t3.sort_name is not null
                    and t2.sort_name is not null -- and bis_cont_id in ('8a7b859c6f666153016f698a28fc5747','8a7b868b7c0da24b017c0e03358906d0')


                  union all

                  select t1.BIS_SHOP_ID,
                         t1.BIS_CONT_ID,
                         t1.PROPORTION_NAMES,
                         t1.SORT_TYPE3,
                         t1.thirdly_formats_code,   -- 三级业态code
                         t1.thirdly_formats,        -- 三级业态
                         t1.SORT_TYPE2,
                         t1.secondary_formats_code, -- 二级业态code
                         t1.secondary_formats,      -- 二级业态
                         t1.SORT_TYPE1,
                         t2.primary_forms_code,     -- 一级业态code
                         t1.primary_forms           -- 一级业态
                  from (
                           -- 老业态
                           select t1.BIS_SHOP_ID,
                                  t1.BIS_CONT_ID,
                                  t1.PROPORTION_NAMES,
                                  t2.SORT_TYPE                        SORT_TYPE3,
                                  t2.LAYOUT_CD                        thirdly_formats_code,   -- 三级业态code
                                  t2.SORT_NAME                        thirdly_formats,        -- 三级业态
                                  t3.SORT_TYPE                        SORT_TYPE2,
                                  t3.LAYOUT_CD                        secondary_formats_code, -- 二级业态code
                                  t3.SORT_NAME                        secondary_formats,      -- 二级业态
                                  t4.SORT_TYPE                        SORT_TYPE1,
                                  t4.LAYOUT_CD                        primary_forms_code,     -- 一级业态code
                                  case
                                      when t1.PROPORTION_NAMES like '%超市%' then '超市'
                                      when t1.PROPORTION_NAMES like '%影院%' then '影院'
                                      when t1.PROPORTION_NAMES like '%百货%' then '次主力店'
                                      when t1.PROPORTION_NAMES like '%餐饮%' then '餐饮'
                                      when t1.PROPORTION_NAMES like '%名品%' then '服装'
                                      when t1.PROPORTION_NAMES like '%服装%' then '服装'
                                      when t1.PROPORTION_NAMES like '%配套-配套集合店%' then '零售配套'
                                      when t1.PROPORTION_NAMES like '%配套-鞋包/珠宝配饰/化妆品/精品%' then '零售配套'
                                      when t1.PROPORTION_NAMES like '%配套-文娱类配套-文具%' then '零售配套'
                                      when t1.PROPORTION_NAMES like '%配套-文娱类配套-音像%' then '零售配套'
                                      when t1.PROPORTION_NAMES like '%配套-文娱类配套-礼品精品%' then '零售配套'
                                      when t1.PROPORTION_NAMES like '%配套-文娱类配套-艺术品%' then '零售配套'
                                      when t1.PROPORTION_NAMES like '%配套-文娱类配套-书店%' then '生活配套'
                                      when t1.PROPORTION_NAMES like '%配套-休闲娱乐%' then '生活配套'
                                      when t1.PROPORTION_NAMES like '%配套-生活配套/服务%' then '生活配套'
                                      when t1.PROPORTION_NAMES like '%儿童%' then '儿童'
                                      else t1.PROPORTION_NAMES end as primary_forms           -- 一级业态
                           from ods.ods_pl_powerdes_bis_cont_dt t1
                                    LEFT JOIN ods.ods_pl_powerdes_bis_shop_sort_dt t2
                                              ON t1.PROPORTION_IDS = t2.BIS_SHOP_SORT_ID
                                    left join ods.ods_pl_powerdes_bis_shop_sort_dt t3
                                              ON t2.PARENT_ID = t3.BIS_SHOP_SORT_ID
                                    left join ods.ods_pl_powerdes_bis_shop_sort_dt t4
                                              ON t3.PARENT_ID = t4.BIS_SHOP_SORT_ID
                           where PROPORTION_IDS is not null
                           -- and bis_cont_id in ('8a7b859c6f666153016f698a28fc5747','8a7b868b7c0da24b017c0e03358906d0')
                       ) t1
                           left join
                       (
                           -- 新业态
                           select distinct t4.LAYOUT_CD primary_forms_code, -- 一级业态code
                                           t4.SORT_NAME primary_forms       -- 一级业态
                           from ods.ods_pl_powerdes_bis_cont_dt t1
                                    LEFT JOIN ods.ods_pl_powerdes_bis_shop_sort_new_dt t2
                                              ON t1.PROPORTION_IDS = t2.BIS_SHOP_SORT_ID
                                    left join ods.ods_pl_powerdes_bis_shop_sort_new_dt t3
                                              ON t2.PARENT_ID = t3.BIS_SHOP_SORT_ID
                                    left join ods.ods_pl_powerdes_bis_shop_sort_new_dt t4
                                              ON t3.PARENT_ID = t4.BIS_SHOP_SORT_ID
                           where PROPORTION_IDS is not null
                             and t4.LAYOUT_CD is not null
                             and t4.SORT_NAME is not null
                       ) t2 on t1.primary_forms = t2.primary_forms
              ) t1
         GROUP BY t1.BIS_SHOP_ID, t1.BIS_CONT_ID, t1.PROPORTION_NAMES
     ) t6 on BIS_CONT.bis_cont_id = t6.bis_cont_id
where BIS_CONT.EFFECT_FLG <> 'D';