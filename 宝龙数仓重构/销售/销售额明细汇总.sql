select t1.BIS_PROJECT_ID,                 -- 项目id
       t1.short_name,                     -- 项目名称
       t1.store_type,                     -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
       t1.primary_forms,                  -- 一级业态
       t1.SALES_DATE,                     -- 销售日期
       t1.SALES_MONEY,                    -- （考核 + 不考核）销售额,
       bis_project.province,              -- 项目所属省份
       bis_project.oper_status,           -- '项目状态（1：在建；2：在营）'
       bis_project.stage,                 -- 项目阶段('1、筹备期 2、培育期 3、稳定期 4、调改期')
       t2.area_name,                      -- 区域名称
       t2.id,                             -- 区域id
       dt.WEEK_NUM,                       -- 周几
       case
           when dt.WEEK_NUM = '5' or dt.WEEK_NUM = '6' or dt.WEEK_NUM = '7' then '1'
           else '0' end is_holiday,       -- 是否是节假日(0：工作日 1：节假日  【周一 至 周四 为工作日】，【周五 至 周日 为节假日】)
       null as          day_sales_target, -- 日销售额指标
       t1.assess_SALES_MONEY,             -- 考核销售额
       t1.no_assess_SALES_MONEY,          -- 不考核销售额

       t1.zc_SALES_MONEY,                 -- 自持销售额
       t1.ys_SALES_MONEY,                 -- 已售销售额
       t1.dj_SALES_MONEY,                 -- 多经销售额
       t1.htqb_SALES_MONEY,               -- （自持 +已售 + 多经）销售额
       t1.khqc_ctqb_SALES_MONEY,          -- （考核 + 不考核） + （自持 +已售 + 多经）销售额
       t1.kh_htqb_SALES_MONEY,            -- 考核  + （自持 +已售 + 多经）销售额
       t1.bkh_htqb_SALES_MONEY,           -- 不考核 + （自持 +已售 + 多经）销售额
       t1.khqc_dj_SALES_MONEY,          -- （考核 + 不考核） + 多经销售额
       t1.kh_dj_SALES_MONEY,            -- 考核  + 多经销售额
       t1.bkh_dj_SALES_MONEY,           -- 不考核 + 多经销售额
       t1.khqc_ys_SALES_MONEY,          -- （考核 + 不考核） + 已售销售额
       t1.kh_ys_SALES_MONEY,            -- 考核  + 已售销售额
       t1.bkh_ys_SALES_MONEY,           -- 不考核 + 已售销售额
       t1.khqc_zc_SALES_MONEY,          -- （考核 + 不考核） + 自持销售额
       t1.kh_zc_SALES_MONEY,            -- 考核  + 自持销售额
       t1.bkh_zc_SALES_MONEY,           -- 不考核 + 自持销售额
       current_date                       -- ETL时间
from (
         select BIS_SALES_DAY.BIS_PROJECT_ID,                                                                         -- 项目id
                bis_project.short_name,                                                                               -- 项目名称
                bis_cont.store_type,                                                                                  -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                bis_cont.cont_type_cd,                                                                                -- 合同类别 [1:自持商铺合同 2:已售商铺合同 3:多经合同 4:广告位合同]
                t8.primary_forms,                                                                                     -- 一级业态
                BIS_SALES_DAY.SALES_DATE,                                                                             -- 销售日期
                sum(case
                        when is_assess in ('Y', 'N') THEN NVL(SALES_MONEY, 0)
                        ELSE 0 END)                                                            SALES_MONEY,           -- （考核 + 不考核）销售额
                sum(case when is_assess = 'Y' THEN NVL(SALES_MONEY, 0) ELSE 0 END)             assess_SALES_MONEY,    -- 考核销售额
                sum(case when is_assess = 'N' THEN NVL(SALES_MONEY, 0) ELSE 0 END)             no_assess_SALES_MONEY, -- 不考核销售额
                sum(case when bis_cont.cont_type_cd = '1' THEN NVL(SALES_MONEY, 0) ELSE 0 END) zc_SALES_MONEY,        -- 自持销售额
                sum(case when bis_cont.cont_type_cd = '2' THEN NVL(SALES_MONEY, 0) ELSE 0 END) ys_SALES_MONEY,        -- 已售销售额
                sum(case when bis_cont.cont_type_cd = '3' THEN NVL(SALES_MONEY, 0) ELSE 0 END) dj_SALES_MONEY,        -- 多经销售额
                sum(case
                        when bis_cont.cont_type_cd in ('1', '2', '3') THEN NVL(SALES_MONEY, 0)
                        ELSE 0 END)                                                            htqb_SALES_MONEY,      -- （自持 +已售 + 多经）销售额


                sum(case
                        when bis_cont.cont_type_cd in ('1', '2', '3') AND is_assess in ('Y', 'N')
                            THEN NVL(SALES_MONEY, 0)
                        ELSE 0 END)                                                            khqc_ctqb_SALES_MONEY, -- （考核 + 不考核） + （自持 +已售 + 多经）销售额
                sum(case
                        when bis_cont.cont_type_cd in ('1', '2', '3') AND is_assess = 'Y' THEN NVL(SALES_MONEY, 0)
                        ELSE 0 END)                                                            kh_htqb_SALES_MONEY,   -- 考核  + （自持 +已售 + 多经）销售额
                sum(case
                        when bis_cont.cont_type_cd in ('1', '2', '3') AND is_assess = 'N' THEN NVL(SALES_MONEY, 0)
                        ELSE 0 END)                                                            bkh_htqb_SALES_MONEY,  -- 不考核 + （自持 +已售 + 多经）销售额


                sum(case
                        when bis_cont.cont_type_cd = '3' AND is_assess in ('Y', 'N') THEN NVL(SALES_MONEY, 0)
                        ELSE 0 END)                                                            khqc_dj_SALES_MONEY, -- （考核 + 不考核） + 多经销售额
                sum(case
                        when bis_cont.cont_type_cd = '3' AND is_assess = 'Y' THEN NVL(SALES_MONEY, 0)
                        ELSE 0 END)                                                            kh_dj_SALES_MONEY,   -- 考核  + 多经销售额
                sum(case
                        when bis_cont.cont_type_cd = '3' AND is_assess = 'N' THEN NVL(SALES_MONEY, 0)
                        ELSE 0 END)                                                            bkh_dj_SALES_MONEY,  -- 不考核 + 多经销售额

                sum(case
                        when bis_cont.cont_type_cd = '2' AND is_assess in ('Y', 'N') THEN NVL(SALES_MONEY, 0)
                        ELSE 0 END)                                                            khqc_ys_SALES_MONEY, -- （考核 + 不考核） + 已售销售额
                sum(case
                        when bis_cont.cont_type_cd = '2' AND is_assess = 'Y' THEN NVL(SALES_MONEY, 0)
                        ELSE 0 END)                                                            kh_ys_SALES_MONEY,   -- 考核  + 已售销售额
                sum(case
                        when bis_cont.cont_type_cd = '2' AND is_assess = 'N' THEN NVL(SALES_MONEY, 0)
                        ELSE 0 END)                                                            bkh_ys_SALES_MONEY,  -- 不考核 + 已售销售额

                sum(case
                        when bis_cont.cont_type_cd = '1' AND is_assess in ('Y', 'N') THEN NVL(SALES_MONEY, 0)
                        ELSE 0 END)                                                            khqc_zc_SALES_MONEY, -- （考核 + 不考核） + 自持销售额
                sum(case
                        when bis_cont.cont_type_cd = '1' AND is_assess = 'Y' THEN NVL(SALES_MONEY, 0)
                        ELSE 0 END)                                                            kh_zc_SALES_MONEY,   -- 考核  + 自持销售额
                sum(case
                        when bis_cont.cont_type_cd = '1' AND is_assess = 'N' THEN NVL(SALES_MONEY, 0)
                        ELSE 0 END)                                                            bkh_zc_SALES_MONEY,  -- 不考核 + 自持销售额
                t9.is_assess                                                                                          -- Y:考核 N:不考核
         from dwd.dwd_bis_sales_day_big_dt BIS_SALES_DAY
                  left join ods.ods_pl_powerdes_bis_cont_dt bis_cont
                            on BIS_SALES_DAY.bis_project_id = bis_cont.bis_project_id and
                               BIS_SALES_DAY.bis_cont_id = bis_cont.bis_cont_id -- and bis_cont.effect_flg <> 'D'
                  left join ods.ods_pl_powerdes_bis_project_dt bis_project
                            on BIS_SALES_DAY.bis_project_id = bis_project.bis_project_id
                  left join
              (
                  -- 新业态 + 老业态
                  select bis_shop_id,
                         primary_forms,
                         name_cn as cooperative_brand
                  from dwd.dwd_bis_shop_primary_forms_big_dt
                  where dt = date_format(current_date, 'yyyyMMdd')
              ) t8 on bis_cont.bis_shop_id = t8.bis_shop_id
                  inner join
              (
                  select t.bis_cont_id,
                         t.is_assess
                  from (
                           select t1.bis_cont_id,   -- 合同id
                                  concat_ws('_', collect_list(bis_store.is_assess)),
                                  IF(locate('Y', concat_ws('_', collect_list(bis_store.is_assess))) > 0, 'Y',
                                     'N') is_assess -- Y:考核 N:不考核
                           from (
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
                                                  ) tmp LATERAL VIEW explode(split(bis_store_ids, ',')) tmp1 AS bis_store_id
                                         ) t
                                ) t1
                                    left join ods.ods_pl_powerdes_bis_store_dt bis_store
                                               on bis_store.bis_store_id = t1.bis_store_id -- and IS_DELETE = 'N' and bis_store.status_cd = '1' -- 有效铺位
                           group by t1.bis_cont_id
                       ) t
              ) t9 on BIS_SALES_DAY.bis_cont_id = t9.bis_cont_id
         where BIS_SALES_DAY.dt = date_format(current_date, 'yyyyMMdd')
         group by BIS_SALES_DAY.BIS_PROJECT_ID, bis_project.short_name, bis_cont.store_type, t8.primary_forms,
                  BIS_SALES_DAY.SALES_DATE, t9.is_assess, bis_cont.cont_type_cd
     ) t1
         inner join
     (
         select year_month_day1,
                day_number_of_week as WEEK_NUM

         from dim.dim_pl_date
         where year between '2010' and '2030'
     ) dt
     on substr(t1.sales_date, 0, 10) = dt.year_month_day1
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
     ) t2 on t1.bis_project_id = t2.out_mall_id
         inner join ods.ods_pl_powerdes_bis_project_dt bis_project
                    on t1.bis_project_id = bis_project.bis_project_id;