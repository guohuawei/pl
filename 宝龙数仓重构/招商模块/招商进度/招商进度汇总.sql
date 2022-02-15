/*
    从 宝龙招商进度汇总中间表 取数据
*/
insert OVERWRITE table dws.dws_bis_attract_investment_total_big_dt partition (dt = '${hiveconf:nowdate}')
select nvl(t2.project_name, t1.short_name),           -- '项目名称'
       nvl(t2.bis_project_id, t1.bis_project_id),     -- '项目id'
       nvl(t2.open_date, t1.open_date),               -- '项目开业时间'
       t1.yw_date,                                    -- '进度完成时间'
       nvl(t2.structure_square, table1.total_SQUARE), -- '建筑面积'
       nvl(t2.rent_square, table1.total_RENT_SQUARE), -- '计租面积'
       nvl(t2.brand_count, table2.total_brand_count), -- '品牌数量'
       nvl(t2.scondition_type, '1'),                  -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积）'
       t2.intention_brand_count,                      -- 意向品牌数-实际
       t2.sy_brand_count_t,                           -- 商务品牌数-目标
       t2.sy_brand_count,                             -- 商务品牌数-实际
       t2.sy_brand_count_c,                           -- 商务品牌数-目标
       t2.htwp_brand_count_t,                         -- 合同网批品牌数-目标
       t2.htwp_brand_count,                           -- 合同网批品牌数-实际
       t2.htwp_brand_count_c,                         -- 合同网批品牌数-差值
       t2.htqs_brand_count_t,                         -- 合同签署品牌数-目标
       t2.htqs_brand_count,                           -- 合同签署品牌数-实际
       t2.htqs_brand_count_c,                         -- 合同签署品牌数-差值
       t2.jc_brand_count_t,                           -- 进场品牌数-目标
       t2.jc_brand_count,                             -- 进场品牌数-实际
       t2.jc_brand_count_c,                           -- 进场品牌数-差值
       t2.ky_brand_count_t,                           -- 开业品牌数--目标
       t2.ky_brand_count,                             -- 开业品牌数--实际
       t2.ky_brand_count_c,                           -- 开业品牌数--差值
       t2.initial_charge_brand_t,                     -- 品牌首期费用-目标
       t2.initial_charge_brand,                       -- 品牌首期费用-实际
       t2.initial_charge_brand_c                      -- 品牌首期费用-差值
from (
         select bis_project_id,
                yw_date,
                short_name,
                oper_status,
                open_date
         from ODS.ods_pl_powerdes_bis_project_dt bis_project,
              dim.pl_date
         where OPER_STATUS = '1'
           and is_business_project = '1'
           and substr(pl_date.yw_date, 0, 4) between '2010' and '2022'
     ) t1
         left join
     (
         select BIS_PROJECT_ID,
                count(BIS_SHOP_ID) as total_brand_count -- 品牌数
         from ODS.ods_pl_powerdes_bis_cont_dt BIS_CONT
         where EFFECT_FLG <> 'D'
           and STATUS_CD in ('1', '2')
         group by BIS_PROJECT_ID
     ) table2 on t1.bis_project_id = table2.bis_project_id
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
     ) table1 on t1.bis_project_id = table1.bis_project_id
         left join
     (select project_name,                   -- '项目名称'
             bis_project_id,                 -- '项目id'
             open_date,                      -- '项目开业时间'
             real_completed_ym,              -- '进度完成时间'
             structure_square,               -- '建筑面积'
             rent_square,                    -- '计租面积'
             brand_count,                    -- '品牌数量'
             '1'  as scondition_type,        -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积）'
             intention_brand_count,          -- 意向品牌数-实际
             null as sy_brand_count_t,       -- 商务品牌数-目标
             sy_brand_count,                 -- 商务品牌数-实际
             null as sy_brand_count_c,       -- 商务品牌数-目标
             null as htwp_brand_count_t,     -- 合同网批品牌数-目标
             htwp_brand_count,               -- 合同网批品牌数-实际
             null as htwp_brand_count_c,     -- 合同网批品牌数-差值
             null as htqs_brand_count_t,     -- 合同签署品牌数-目标
             htqs_brand_count,               -- 合同签署品牌数-实际
             null as htqs_brand_count_c,     -- 合同签署品牌数-差值
             null as jc_brand_count_t,       -- 进场品牌数-目标
             jc_brand_count,                 -- 进场品牌数-实际
             null as jc_brand_count_c,       -- 进场品牌数-差值
             null as ky_brand_count_t,       -- 开业品牌数--目标
             ky_brand_count,                 -- 开业品牌数--实际
             null as ky_brand_count_c,       -- 开业品牌数--差值
             null as initial_charge_brand_t, -- 品牌首期费用-目标
             initial_charge_brand,           -- 品牌首期费用-实际
             null as initial_charge_brand_c  -- 品牌首期费用-差值
      from dws.dws_bis_attract_investment_total_big_temp_dt
      where dt = date_format(current_date(), 'yyyyMMdd')
     ) t2
     on t1.bis_project_id = t2.bis_project_id and t1.yw_date = t2.real_completed_ym;


insert into table dws.dws_bis_attract_investment_total_big_dt partition (dt = '${hiveconf:nowdate}')
select nvl(t2.project_name, t1.short_name),           -- '项目名称'
       nvl(t2.bis_project_id, t1.bis_project_id),     -- '项目id'
       nvl(t2.open_date, t1.open_date),               -- '项目开业时间'
       t1.yw_date,                                    -- '进度完成时间'
       nvl(t2.structure_square, table1.total_SQUARE), -- '建筑面积'
       nvl(t2.rent_square, table1.total_RENT_SQUARE), -- '计租面积'
       nvl(t2.brand_count, table2.total_brand_count), -- '品牌数量'
       nvl(t2.scondition_type, '2'),                  -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积）'
       t2.intention_jz_area_count,                    -- 意向建筑面积-实际
       t2.sy_jz_area_count_t,                         --商务建筑面积-目标
       t2.sy_jz_area_count,                           --商务建筑面积-实际
       t2.sy_jz_area_count_c,                         --商务建筑面积-目标
       t2.htwp_jz_area_count_t,                       -- 合同网批建筑面积-目标
       t2.htwp_jz_area_count,                         -- 合同网批建筑面积-实际
       t2.htwp_jz_area_count_c,                       -- 合同网批建筑面积-差值
       t2.htqs_jz_area_count_t,                       -- 合同签署建筑面积-目标
       t2.htqs_jz_area_count,                         -- 合同签署建筑面积-实际
       t2.htqs_jz_area_count_c,                       -- 合同签署建筑面积-差值
       t2.jc_jz_area_count_t,                         -- 进场建筑面积-目标
       t2.jc_jz_area_count,                           -- 进场建筑面积-实际
       t2.jc_jz_area_count_c,                         -- 进场建筑面积-差值
       t2.ky_jz_area_count_t,                         -- 开业建筑面积--目标
       t2.ky_jz_area_count,                           -- 开业建筑面积--实际
       t2.ky_jz_area_count_c,                         -- 开业建筑面积--差值
       t2.initial_charge_j_area_t,                    -- 建筑面积首期费用-目标
       t2.initial_charge_j_area,                      -- 建筑面积首期费用-实际
       t2.initial_charge_j_area_c                     -- 建筑面积首期费用-差值
from (
         select bis_project_id,
                yw_date,
                short_name,
                oper_status,
                open_date
         from ODS.ods_pl_powerdes_bis_project_dt bis_project,
              dim.pl_date
         where OPER_STATUS = '1'
           and is_business_project = '1'
           and substr(pl_date.yw_date, 0, 4) between '2010' and '2022'
     ) t1
         left join
     (
         select BIS_PROJECT_ID,
                count(BIS_SHOP_ID) as total_brand_count -- 品牌数
         from ODS.ods_pl_powerdes_bis_cont_dt BIS_CONT
         where EFFECT_FLG <> 'D'
           and STATUS_CD in ('1', '2')
         group by BIS_PROJECT_ID
     ) table2 on t1.bis_project_id = table2.bis_project_id
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
     ) table1 on t1.bis_project_id = table1.bis_project_id
         left join
     (
         select project_name,                    -- '项目名称'
                bis_project_id,                  -- '项目id'
                open_date,                       -- '项目开业时间'
                real_completed_ym,               -- '进度完成时间'
                structure_square,                -- '建筑面积'
                rent_square,                     -- '计租面积'
                brand_count,                     -- '品牌数量'
                '2'  as scondition_type,         -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积）'
                intention_jz_area_count,         -- 意向建筑面积-实际
                null as sy_jz_area_count_t,      --商务建筑面积-目标
                sy_jz_area_count,                --商务建筑面积-实际
                null as sy_jz_area_count_c,      --商务建筑面积-目标
                null as htwp_jz_area_count_t,    -- 合同网批建筑面积-目标
                htwp_jz_area_count,              -- 合同网批建筑面积-实际
                null as htwp_jz_area_count_c,    -- 合同网批建筑面积-差值
                null as htqs_jz_area_count_t,    -- 合同签署建筑面积-目标
                htqs_jz_area_count,              -- 合同签署建筑面积-实际
                null as htqs_jz_area_count_c,    -- 合同签署建筑面积-差值
                null as jc_jz_area_count_t,      -- 进场建筑面积-目标
                jc_jz_area_count,                -- 进场建筑面积-实际
                null as jc_jz_area_count_c,      -- 进场建筑面积-差值
                null as ky_jz_area_count_t,      -- 开业建筑面积--目标
                ky_jz_area_count,                -- 开业建筑面积--实际
                null as ky_jz_area_count_c,      -- 开业建筑面积--差值
                null as initial_charge_j_area_t, -- 建筑面积首期费用-目标
                initial_charge_j_area,           -- 建筑面积首期费用-实际
                null as initial_charge_j_area_c  -- 建筑面积首期费用-差值
         from dws.dws_bis_attract_investment_total_big_temp_dt
         where dt = date_format(current_date(), 'yyyyMMdd')
     ) t2 on t1.bis_project_id = t2.bis_project_id and t1.yw_date = t2.real_completed_ym;



insert into table dws.dws_bis_attract_investment_total_big_dt partition (dt = '${hiveconf:nowdate}')
select nvl(t2.project_name, t1.short_name),           -- '项目名称'
       nvl(t2.bis_project_id, t1.bis_project_id),     -- '项目id'
       nvl(t2.open_date, t1.open_date),               -- '项目开业时间'
       t1.yw_date,                                    -- '进度完成时间'
       nvl(t2.structure_square, table1.total_SQUARE), -- '建筑面积'
       nvl(t2.rent_square, table1.total_RENT_SQUARE), -- '计租面积'
       nvl(t2.brand_count, table2.total_brand_count), -- '品牌数量'
       nvl(t2.scondition_type, 3),                    -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积）'
       t2.intention_rent_area_count,                  -- 意向计租面积-实际
       t2.sy_rent_area_count_t,                       --商务计租面积-目标
       t2.sy_rent_area_count,                         --商务计租面积-实际
       t2.sy_rent_area_count_c,                       --商务计租面积-目标
       t2.htwp_rent_area_count_t,                     -- 合同网批计租面积-目标
       t2.htwp_rent_area_count,                       -- 合同网批计租面积-实际
       t2.htwp_rent_area_count_c,                     -- 合同网批计租面积-差值
       t2.htqs_rent_area_count_t,                     -- 合同签署计租面积-目标
       t2.htqs_rent_area_count,                       -- 合同签署计租面积-实际
       t2.htqs_rent_area_count_c,                     -- 合同签署计租面积-差值
       t2.jc_rent_area_count_t,                       -- 进场计租面积-目标
       t2.jc_rent_area_count,                         -- 进场计租面积-实际
       t2.jc_rent_area_count_c,                       -- 进场计租面积-差值
       t2.ky_rent_area_count_t,                       -- 开业计租面积--目标
       t2.ky_rent_area_count,                         -- 开业计租面积--实际
       t2.ky_rent_area_count_c,                       -- 开业计租面积--差值
       t2.initial_charge_jz_area_t,                   -- 计租面积首期费用-目标
       t2.initial_charge_jz_area,                     -- 计租面积首期费用-实际
       t2.initial_charge_jz_area_c                    -- 计租面积首期费用-差值
from (
         select bis_project_id,
                yw_date,
                short_name,
                oper_status,
                open_date
         from ODS.ods_pl_powerdes_bis_project_dt bis_project,
              dim.pl_date
         where OPER_STATUS = '1'
           and is_business_project = '1'
           and substr(pl_date.yw_date, 0, 4) between '2010' and '2022'
     ) t1
         left join
     (
         select BIS_PROJECT_ID,
                count(BIS_SHOP_ID) as total_brand_count -- 品牌数
         from ODS.ods_pl_powerdes_bis_cont_dt BIS_CONT
         where EFFECT_FLG <> 'D'
           and STATUS_CD in ('1', '2')
         group by BIS_PROJECT_ID
     ) table2 on t1.bis_project_id = table2.bis_project_id
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
     ) table1 on t1.bis_project_id = table1.bis_project_id
         left join
     (
         select project_name,                     -- '项目名称'
                bis_project_id,                   -- '项目id'
                open_date,                        -- '项目开业时间'
                real_completed_ym,                -- '进度完成时间'
                structure_square,                 -- '建筑面积'
                rent_square,                      -- '计租面积'
                brand_count,                      -- '品牌数量'
                '3'  as scondition_type,          -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积）'
                intention_rent_area_count,        -- 意向计租面积-实际
                null as sy_rent_area_count_t,     --商务计租面积-目标
                sy_rent_area_count,               --商务计租面积-实际
                null as sy_rent_area_count_c,     --商务计租面积-目标
                null as htwp_rent_area_count_t,   -- 合同网批计租面积-目标
                htwp_rent_area_count,             -- 合同网批计租面积-实际
                null as htwp_rent_area_count_c,   -- 合同网批计租面积-差值
                null as htqs_rent_area_count_t,   -- 合同签署计租面积-目标
                htqs_rent_area_count,             -- 合同签署计租面积-实际
                null as htqs_rent_area_count_c,   -- 合同签署计租面积-差值
                null as jc_rent_area_count_t,     -- 进场计租面积-目标
                jc_rent_area_count,               -- 进场计租面积-实际
                null as jc_rent_area_count_c,     -- 进场计租面积-差值
                null as ky_rent_area_count_t,     -- 开业计租面积--目标
                ky_rent_area_count,               -- 开业计租面积--实际
                null as ky_rent_area_count_c,     -- 开业计租面积--差值
                null as initial_charge_jz_area_t, -- 计租面积首期费用-目标
                initial_charge_jz_area,           -- 计租面积首期费用-实际
                null as initial_charge_jz_area_c  -- 计租面积首期费用-差值
         from dws.dws_bis_attract_investment_total_big_temp_dt
         where dt = date_format(current_date(), 'yyyyMMdd')
     ) t2
     on t1.bis_project_id = t2.bis_project_id and t1.yw_date = t2.real_completed_ym;