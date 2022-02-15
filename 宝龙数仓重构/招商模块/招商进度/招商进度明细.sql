-- 品牌  -- 意向 1-12月统计
insert overwrite table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
SELECT t.project_name,
       t.bis_project_id,
       '1'                 as business_type, -- 业务类型(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更)
       max(t.scondition_type),                 -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       t.real_completed_y,
       MAX(t.month_value_01) as month_value_01,
       MAX(t.month_value_02) as month_value_02,
       MAX(t.month_value_03) as month_value_03,
       MAX(t.month_value_04) as month_value_04,
       MAX(t.month_value_05) as month_value_05,
       MAX(t.month_value_06) as month_value_06,
       MAX(t.month_value_07) as month_value_07,
       MAX(t.month_value_08) as month_value_08,
       MAX(t.month_value_09) as month_value_09,
       MAX(t.month_value_10) as month_value_10,
       MAX(t.month_value_11) as month_value_11,
       MAX(t.month_value_12) as month_value_12

FROM (SELECT project_name,
             bis_project_id,
             max(condition_type)                                                                 as scondition_type,
             SUBSTR(real_completed_ym, 1, 4)                                                      as real_completed_y,
             SUBSTR(real_completed_ym, 6, 7)                                                      as real_completed_m,
             -- 品牌  -- 意向 1-12月统计
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '01' THEN sum(intention_a) ELSE NULL end as month_value_01,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '02' THEN sum(intention_a) ELSE NULL end as month_value_02,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '03' THEN sum(intention_a) ELSE NULL end as month_value_03,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '04' THEN sum(intention_a) ELSE NULL end as month_value_04,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '05' THEN sum(intention_a) ELSE NULL end as month_value_05,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '06' THEN sum(intention_a) ELSE NULL end as month_value_06,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '07' THEN sum(intention_a) ELSE NULL end as month_value_07,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '08' THEN sum(intention_a) ELSE NULL end as month_value_08,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '09' THEN sum(intention_a) ELSE NULL end as month_value_09,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '10' THEN sum(intention_a) ELSE NULL end as month_value_10,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '11' THEN sum(intention_a) ELSE NULL end as month_value_11,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '12' THEN sum(intention_a) ELSE NULL end as month_value_12

      from dws.dws_bis_attract_investment_total_big_dt bis_attract_investment_total_big
      WHERE bis_attract_investment_total_big.condition_type = '1' -- 搜索条件类型（1：品牌 2：建筑面积 3：计租面积）
      GROUP BY project_name, bis_project_id, SUBSTR(real_completed_ym, 1, 4), SUBSTR(real_completed_ym, 6, 7)
     ) t
GROUP BY t.project_name, t.bis_project_id, t.real_completed_y;


-- 建筑面积  -- 意向 1-12月统计
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
SELECT t.project_name,
       t.bis_project_id,
       '1'                 as business_type, -- 业务类型(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更)
       max(t.scondition_type),                 -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       real_completed_y,
       MAX(t.month_value_01) as month_value_01,
       MAX(t.month_value_02) as month_value_02,
       MAX(t.month_value_03) as month_value_03,
       MAX(t.month_value_04) as month_value_04,
       MAX(t.month_value_05) as month_value_05,
       MAX(t.month_value_06) as month_value_06,
       MAX(t.month_value_07) as month_value_07,
       MAX(t.month_value_08) as month_value_08,
       MAX(t.month_value_09) as month_value_09,
       MAX(t.month_value_10) as month_value_10,
       MAX(t.month_value_11) as month_value_11,
       MAX(t.month_value_12) as month_value_12

FROM (SELECT project_name,
             bis_project_id,
             max(condition_type)                                                                 as scondition_type,
             SUBSTR(real_completed_ym, 1, 4)                                                      as real_completed_y,
             SUBSTR(real_completed_ym, 6, 7)                                                      as real_completed_m,
             -- 建筑面积  -- 意向 1-12月统计
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '01' THEN sum(intention_a) ELSE NULL end as month_value_01,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '02' THEN sum(intention_a) ELSE NULL end as month_value_02,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '03' THEN sum(intention_a) ELSE NULL end as month_value_03,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '04' THEN sum(intention_a) ELSE NULL end as month_value_04,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '05' THEN sum(intention_a) ELSE NULL end as month_value_05,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '06' THEN sum(intention_a) ELSE NULL end as month_value_06,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '07' THEN sum(intention_a) ELSE NULL end as month_value_07,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '08' THEN sum(intention_a) ELSE NULL end as month_value_08,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '09' THEN sum(intention_a) ELSE NULL end as month_value_09,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '10' THEN sum(intention_a) ELSE NULL end as month_value_10,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '11' THEN sum(intention_a) ELSE NULL end as month_value_11,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '12' THEN sum(intention_a) ELSE NULL end as month_value_12

      from dws.dws_bis_attract_investment_total_big_dt bis_attract_investment_total_big
      WHERE bis_attract_investment_total_big.condition_type = '2' -- 搜索条件类型（1：品牌 2：建筑面积 3：计租面积）
      GROUP BY project_name, bis_project_id, SUBSTR(real_completed_ym, 1, 4), SUBSTR(real_completed_ym, 6, 7)
     ) t
GROUP BY project_name, bis_project_id, real_completed_y;


-- 计租面积  -- 意向 1-12月统计
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
SELECT project_name,
       bis_project_id,
       '1'                 as business_type, -- 业务类型(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更)
       max(scondition_type),                 -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       real_completed_y,
       MAX(month_value_01) as month_value_01,
       MAX(month_value_02) as month_value_02,
       MAX(month_value_03) as month_value_03,
       MAX(month_value_04) as month_value_04,
       MAX(month_value_05) as month_value_05,
       MAX(month_value_06) as month_value_06,
       MAX(month_value_07) as month_value_07,
       MAX(month_value_08) as month_value_08,
       MAX(month_value_09) as month_value_09,
       MAX(month_value_10) as month_value_10,
       MAX(month_value_11) as month_value_11,
       MAX(month_value_12) as month_value_12

FROM (SELECT project_name,
             bis_project_id,
             max(condition_type)                                                                 as scondition_type,
             SUBSTR(real_completed_ym, 1, 4)                                                      as real_completed_y,
             SUBSTR(real_completed_ym, 6, 7)                                                      as real_completed_m,
             -- 计租面积  -- 意向 1-12月统计
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '01' THEN sum(intention_a) ELSE NULL end as month_value_01,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '02' THEN sum(intention_a) ELSE NULL end as month_value_02,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '03' THEN sum(intention_a) ELSE NULL end as month_value_03,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '04' THEN sum(intention_a) ELSE NULL end as month_value_04,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '05' THEN sum(intention_a) ELSE NULL end as month_value_05,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '06' THEN sum(intention_a) ELSE NULL end as month_value_06,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '07' THEN sum(intention_a) ELSE NULL end as month_value_07,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '08' THEN sum(intention_a) ELSE NULL end as month_value_08,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '09' THEN sum(intention_a) ELSE NULL end as month_value_09,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '10' THEN sum(intention_a) ELSE NULL end as month_value_10,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '11' THEN sum(intention_a) ELSE NULL end as month_value_11,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '12' THEN sum(intention_a) ELSE NULL end as month_value_12

      from dws.dws_bis_attract_investment_total_big_dt bis_attract_investment_total_big
      WHERE bis_attract_investment_total_big.condition_type = '3' -- 搜索条件类型（1：品牌 2：建筑面积 3：计租面积）
      GROUP BY project_name, bis_project_id, SUBSTR(real_completed_ym, 1, 4), SUBSTR(real_completed_ym, 6, 7)
     ) t
GROUP BY project_name, bis_project_id, real_completed_y;


-- 品牌  -- 商务 1-12月统计
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
SELECT project_name,
       bis_project_id,
       '2'                 as business_type, -- 业务类型(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更)
       max(scondition_type),                 -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       real_completed_y,
       MAX(month_value_01) as month_value_01,
       MAX(month_value_02) as month_value_02,
       MAX(month_value_03) as month_value_03,
       MAX(month_value_04) as month_value_04,
       MAX(month_value_05) as month_value_05,
       MAX(month_value_06) as month_value_06,
       MAX(month_value_07) as month_value_07,
       MAX(month_value_08) as month_value_08,
       MAX(month_value_09) as month_value_09,
       MAX(month_value_10) as month_value_10,
       MAX(month_value_11) as month_value_11,
       MAX(month_value_12) as month_value_12

FROM (SELECT project_name,
             bis_project_id,
             max(condition_type)                                                                as scondition_type,
             SUBSTR(real_completed_ym, 1, 4)                                                     as real_completed_y,
             SUBSTR(real_completed_ym, 6, 7)                                                     as real_completed_m,
             -- 品牌  -- 商务 1-12月统计
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '01' THEN sum(business_a) ELSE NULL end as month_value_01,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '02' THEN sum(business_a) ELSE NULL end as month_value_02,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '03' THEN sum(business_a) ELSE NULL end as month_value_03,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '04' THEN sum(business_a) ELSE NULL end as month_value_04,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '05' THEN sum(business_a) ELSE NULL end as month_value_05,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '06' THEN sum(business_a) ELSE NULL end as month_value_06,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '07' THEN sum(business_a) ELSE NULL end as month_value_07,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '08' THEN sum(business_a) ELSE NULL end as month_value_08,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '09' THEN sum(business_a) ELSE NULL end as month_value_09,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '10' THEN sum(business_a) ELSE NULL end as month_value_10,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '11' THEN sum(business_a) ELSE NULL end as month_value_11,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '12' THEN sum(business_a) ELSE NULL end as month_value_12

      from dws.dws_bis_attract_investment_total_big_dt bis_attract_investment_total_big
      WHERE bis_attract_investment_total_big.condition_type = '1' -- 搜索条件类型（1：品牌 2：建筑面积 3：计租面积）
      GROUP BY project_name, bis_project_id, SUBSTR(real_completed_ym, 1, 4), SUBSTR(real_completed_ym, 6, 7)
     ) t
GROUP BY project_name, bis_project_id, real_completed_y;


-- 建筑面积  -- 商务 1-12月统计
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
SELECT project_name,
       bis_project_id,
       '2'                 as business_type, -- 业务类型(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更)
       max(scondition_type),                 -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       real_completed_y,
       MAX(month_value_01) as month_value_01,
       MAX(month_value_02) as month_value_02,
       MAX(month_value_03) as month_value_03,
       MAX(month_value_04) as month_value_04,
       MAX(month_value_05) as month_value_05,
       MAX(month_value_06) as month_value_06,
       MAX(month_value_07) as month_value_07,
       MAX(month_value_08) as month_value_08,
       MAX(month_value_09) as month_value_09,
       MAX(month_value_10) as month_value_10,
       MAX(month_value_11) as month_value_11,
       MAX(month_value_12) as month_value_12

FROM (SELECT project_name,
             bis_project_id,
             max(condition_type)                                                                as scondition_type,
             SUBSTR(real_completed_ym, 1, 4)                                                     as real_completed_y,
             SUBSTR(real_completed_ym, 6, 7)                                                     as real_completed_m,
             -- 建筑面积  -- 商务 1-12月统计
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '01' THEN sum(business_a) ELSE NULL end as month_value_01,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '02' THEN sum(business_a) ELSE NULL end as month_value_02,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '03' THEN sum(business_a) ELSE NULL end as month_value_03,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '04' THEN sum(business_a) ELSE NULL end as month_value_04,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '05' THEN sum(business_a) ELSE NULL end as month_value_05,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '06' THEN sum(business_a) ELSE NULL end as month_value_06,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '07' THEN sum(business_a) ELSE NULL end as month_value_07,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '08' THEN sum(business_a) ELSE NULL end as month_value_08,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '09' THEN sum(business_a) ELSE NULL end as month_value_09,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '10' THEN sum(business_a) ELSE NULL end as month_value_10,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '11' THEN sum(business_a) ELSE NULL end as month_value_11,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '12' THEN sum(business_a) ELSE NULL end as month_value_12

      from dws.dws_bis_attract_investment_total_big_dt bis_attract_investment_total_big
      WHERE bis_attract_investment_total_big.condition_type = '2' -- 搜索条件类型（1：品牌 2：建筑面积 3：计租面积）
      GROUP BY project_name, bis_project_id, SUBSTR(real_completed_ym, 1, 4), SUBSTR(real_completed_ym, 6, 7)
     ) t
GROUP BY project_name, bis_project_id, real_completed_y;


-- 计租面积  -- 商务 1-12月统计
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
SELECT project_name,
       bis_project_id,
       '2'                 as business_type, -- 业务类型(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更)
       max(scondition_type),                 -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       real_completed_y,
       MAX(month_value_01) as month_value_01,
       MAX(month_value_02) as month_value_02,
       MAX(month_value_03) as month_value_03,
       MAX(month_value_04) as month_value_04,
       MAX(month_value_05) as month_value_05,
       MAX(month_value_06) as month_value_06,
       MAX(month_value_07) as month_value_07,
       MAX(month_value_08) as month_value_08,
       MAX(month_value_09) as month_value_09,
       MAX(month_value_10) as month_value_10,
       MAX(month_value_11) as month_value_11,
       MAX(month_value_12) as month_value_12

FROM (SELECT project_name,
             bis_project_id,
             max(condition_type)                                                                as scondition_type,
             SUBSTR(real_completed_ym, 1, 4)                                                     as real_completed_y,
             SUBSTR(real_completed_ym, 6, 7)                                                     as real_completed_m,
             -- 计租面积  -- 商务 1-12月统计
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '01' THEN sum(business_a) ELSE NULL end as month_value_01,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '02' THEN sum(business_a) ELSE NULL end as month_value_02,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '03' THEN sum(business_a) ELSE NULL end as month_value_03,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '04' THEN sum(business_a) ELSE NULL end as month_value_04,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '05' THEN sum(business_a) ELSE NULL end as month_value_05,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '06' THEN sum(business_a) ELSE NULL end as month_value_06,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '07' THEN sum(business_a) ELSE NULL end as month_value_07,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '08' THEN sum(business_a) ELSE NULL end as month_value_08,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '09' THEN sum(business_a) ELSE NULL end as month_value_09,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '10' THEN sum(business_a) ELSE NULL end as month_value_10,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '11' THEN sum(business_a) ELSE NULL end as month_value_11,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '12' THEN sum(business_a) ELSE NULL end as month_value_12

      from dws.dws_bis_attract_investment_total_big_dt bis_attract_investment_total_big
      WHERE bis_attract_investment_total_big.condition_type = '3' -- 搜索条件类型（1：品牌 2：建筑面积 3：计租面积）
      GROUP BY project_name, bis_project_id, SUBSTR(real_completed_ym, 1, 4), SUBSTR(real_completed_ym, 6, 7)
     ) t
GROUP BY project_name, bis_project_id, real_completed_y;


-- 品牌  -- 合同网批 1-12月统计
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
SELECT project_name,
       bis_project_id,
       '3'                 as business_type, -- 业务类型(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更)
       max(scondition_type),                 -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       real_completed_y,
       MAX(month_value_01) as month_value_01,
       MAX(month_value_02) as month_value_02,
       MAX(month_value_03) as month_value_03,
       MAX(month_value_04) as month_value_04,
       MAX(month_value_05) as month_value_05,
       MAX(month_value_06) as month_value_06,
       MAX(month_value_07) as month_value_07,
       MAX(month_value_08) as month_value_08,
       MAX(month_value_09) as month_value_09,
       MAX(month_value_10) as month_value_10,
       MAX(month_value_11) as month_value_11,
       MAX(month_value_12) as month_value_12

FROM (SELECT project_name,
             bis_project_id,
             max(condition_type)            as scondition_type,
             SUBSTR(real_completed_ym, 1, 4) as real_completed_y,
             SUBSTR(real_completed_ym, 6, 7) as real_completed_m,
             -- 品牌  -- 合同网批 1-12月统计
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '01' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_01,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '02' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_02,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '03' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_03,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '04' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_04,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '05' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_05,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '06' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_06,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '07' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_07,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '08' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_08,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '09' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_09,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '10' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_10,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '11' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_11,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '12' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_12

      from dws.dws_bis_attract_investment_total_big_dt bis_attract_investment_total_big
      WHERE bis_attract_investment_total_big.condition_type = '1' -- 搜索条件类型（1：品牌 2：建筑面积 3：计租面积）
      GROUP BY project_name, bis_project_id, SUBSTR(real_completed_ym, 1, 4), SUBSTR(real_completed_ym, 6, 7)
     ) t
GROUP BY project_name, bis_project_id, real_completed_y;


-- 建筑面积  -- 合同网批 1-12月统计
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
SELECT project_name,
       bis_project_id,
       '3'                 as business_type, -- 业务类型(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更)
       max(scondition_type),                 -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       real_completed_y,
       MAX(month_value_01) as month_value_01,
       MAX(month_value_02) as month_value_02,
       MAX(month_value_03) as month_value_03,
       MAX(month_value_04) as month_value_04,
       MAX(month_value_05) as month_value_05,
       MAX(month_value_06) as month_value_06,
       MAX(month_value_07) as month_value_07,
       MAX(month_value_08) as month_value_08,
       MAX(month_value_09) as month_value_09,
       MAX(month_value_10) as month_value_10,
       MAX(month_value_11) as month_value_11,
       MAX(month_value_12) as month_value_12

FROM (SELECT project_name,
             bis_project_id,
             max(condition_type)            as scondition_type,
             SUBSTR(real_completed_ym, 1, 4) as real_completed_y,
             SUBSTR(real_completed_ym, 6, 7) as real_completed_m,
             -- 建筑面积  -- 合同网批 1-12月统计
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '01' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_01,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '02' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_02,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '03' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_03,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '04' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_04,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '05' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_05,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '06' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_06,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '07' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_07,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '08' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_08,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '09' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_09,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '10' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_10,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '11' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_11,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '12' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_12

      from dws.dws_bis_attract_investment_total_big_dt bis_attract_investment_total_big
      WHERE bis_attract_investment_total_big.condition_type = '2' -- 搜索条件类型（1：品牌 2：建筑面积 3：计租面积）
      GROUP BY project_name, bis_project_id, SUBSTR(real_completed_ym, 1, 4), SUBSTR(real_completed_ym, 6, 7)
     ) t
GROUP BY project_name, bis_project_id, real_completed_y;


-- 计租面积  -- 合同网批 1-12月统计
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
SELECT project_name,
       bis_project_id,
       '3'                 as business_type, -- 业务类型(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更)
       max(scondition_type),                 -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       real_completed_y,
       MAX(month_value_01) as month_value_01,
       MAX(month_value_02) as month_value_02,
       MAX(month_value_03) as month_value_03,
       MAX(month_value_04) as month_value_04,
       MAX(month_value_05) as month_value_05,
       MAX(month_value_06) as month_value_06,
       MAX(month_value_07) as month_value_07,
       MAX(month_value_08) as month_value_08,
       MAX(month_value_09) as month_value_09,
       MAX(month_value_10) as month_value_10,
       MAX(month_value_11) as month_value_11,
       MAX(month_value_12) as month_value_12

FROM (SELECT project_name,
             bis_project_id,
             max(condition_type)            as scondition_type,
             SUBSTR(real_completed_ym, 1, 4) as real_completed_y,
             SUBSTR(real_completed_ym, 6, 7) as real_completed_m,
             -- 计租面积  -- 合同网批 1-12月统计
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '01' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_01,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '02' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_02,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '03' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_03,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '04' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_04,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '05' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_05,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '06' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_06,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '07' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_07,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '08' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_08,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '09' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_09,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '10' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_10,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '11' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_11,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '12' THEN sum(cont_network_reformation_a)
                 ELSE NULL end               as month_value_12

      from dws.dws_bis_attract_investment_total_big_dt bis_attract_investment_total_big
      WHERE bis_attract_investment_total_big.condition_type = '3' -- 搜索条件类型（1：品牌 2：建筑面积 3：计租面积）
      GROUP BY project_name, bis_project_id, SUBSTR(real_completed_ym, 1, 4), SUBSTR(real_completed_ym, 6, 7)
     ) t
GROUP BY project_name, bis_project_id, real_completed_y;


-- 品牌  -- 合同签署 1-12月统计
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
SELECT project_name,
       bis_project_id,
       '4'                 as business_type, -- 业务类型(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更)
       max(scondition_type),                 -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       real_completed_y,
       MAX(month_value_01) as month_value_01,
       MAX(month_value_02) as month_value_02,
       MAX(month_value_03) as month_value_03,
       MAX(month_value_04) as month_value_04,
       MAX(month_value_05) as month_value_05,
       MAX(month_value_06) as month_value_06,
       MAX(month_value_07) as month_value_07,
       MAX(month_value_08) as month_value_08,
       MAX(month_value_09) as month_value_09,
       MAX(month_value_10) as month_value_10,
       MAX(month_value_11) as month_value_11,
       MAX(month_value_12) as month_value_12

FROM (SELECT project_name,
             bis_project_id,
             max(condition_type)                                                                 as scondition_type,
             SUBSTR(real_completed_ym, 1, 4)                                                      as real_completed_y,
             SUBSTR(real_completed_ym, 6, 7)                                                      as real_completed_m,
             -- 品牌  -- 合同签署 1-12月统计
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '01' THEN sum(cont_sign_a) ELSE NULL end as month_value_01,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '02' THEN sum(cont_sign_a) ELSE NULL end as month_value_02,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '03' THEN sum(cont_sign_a) ELSE NULL end as month_value_03,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '04' THEN sum(cont_sign_a) ELSE NULL end as month_value_04,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '05' THEN sum(cont_sign_a) ELSE NULL end as month_value_05,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '06' THEN sum(cont_sign_a) ELSE NULL end as month_value_06,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '07' THEN sum(cont_sign_a) ELSE NULL end as month_value_07,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '08' THEN sum(cont_sign_a) ELSE NULL end as month_value_08,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '09' THEN sum(cont_sign_a) ELSE NULL end as month_value_09,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '10' THEN sum(cont_sign_a) ELSE NULL end as month_value_10,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '11' THEN sum(cont_sign_a) ELSE NULL end as month_value_11,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '12' THEN sum(cont_sign_a) ELSE NULL end as month_value_12

      from dws.dws_bis_attract_investment_total_big_dt bis_attract_investment_total_big
      WHERE bis_attract_investment_total_big.condition_type = '1' -- 搜索条件类型（1：品牌 2：建筑面积 3：计租面积）
      GROUP BY project_name, bis_project_id, SUBSTR(real_completed_ym, 1, 4), SUBSTR(real_completed_ym, 6, 7)
     ) t
GROUP BY project_name, bis_project_id, real_completed_y;

-- 建筑面积  -- 合同签署 1-12月统计
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
SELECT project_name,
       bis_project_id,
       '4'                 as business_type, -- 业务类型(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更)
       max(scondition_type),                 -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       real_completed_y,
       MAX(month_value_01) as month_value_01,
       MAX(month_value_02) as month_value_02,
       MAX(month_value_03) as month_value_03,
       MAX(month_value_04) as month_value_04,
       MAX(month_value_05) as month_value_05,
       MAX(month_value_06) as month_value_06,
       MAX(month_value_07) as month_value_07,
       MAX(month_value_08) as month_value_08,
       MAX(month_value_09) as month_value_09,
       MAX(month_value_10) as month_value_10,
       MAX(month_value_11) as month_value_11,
       MAX(month_value_12) as month_value_12

FROM (SELECT project_name,
             bis_project_id,
             max(condition_type)                                                                 as scondition_type,
             SUBSTR(real_completed_ym, 1, 4)                                                      as real_completed_y,
             SUBSTR(real_completed_ym, 6, 7)                                                      as real_completed_m,
             -- 建筑面积  -- 合同签署 1-12月统计
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '01' THEN sum(cont_sign_a) ELSE NULL end as month_value_01,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '02' THEN sum(cont_sign_a) ELSE NULL end as month_value_02,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '03' THEN sum(cont_sign_a) ELSE NULL end as month_value_03,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '04' THEN sum(cont_sign_a) ELSE NULL end as month_value_04,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '05' THEN sum(cont_sign_a) ELSE NULL end as month_value_05,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '06' THEN sum(cont_sign_a) ELSE NULL end as month_value_06,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '07' THEN sum(cont_sign_a) ELSE NULL end as month_value_07,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '08' THEN sum(cont_sign_a) ELSE NULL end as month_value_08,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '09' THEN sum(cont_sign_a) ELSE NULL end as month_value_09,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '10' THEN sum(cont_sign_a) ELSE NULL end as month_value_10,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '11' THEN sum(cont_sign_a) ELSE NULL end as month_value_11,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '12' THEN sum(cont_sign_a) ELSE NULL end as month_value_12

      from dws.dws_bis_attract_investment_total_big_dt bis_attract_investment_total_big
      WHERE bis_attract_investment_total_big.condition_type = '2' -- 搜索条件类型（1：品牌 2：建筑面积 3：计租面积）
      GROUP BY project_name, bis_project_id, SUBSTR(real_completed_ym, 1, 4), SUBSTR(real_completed_ym, 6, 7)
     ) t
GROUP BY project_name, bis_project_id, real_completed_y;


-- 计租面积  -- 合同签署 1-12月统计
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
SELECT project_name,
       bis_project_id,
       '4'                 as business_type, -- 业务类型(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更)
       max(scondition_type),                 -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       real_completed_y,
       MAX(month_value_01) as month_value_01,
       MAX(month_value_02) as month_value_02,
       MAX(month_value_03) as month_value_03,
       MAX(month_value_04) as month_value_04,
       MAX(month_value_05) as month_value_05,
       MAX(month_value_06) as month_value_06,
       MAX(month_value_07) as month_value_07,
       MAX(month_value_08) as month_value_08,
       MAX(month_value_09) as month_value_09,
       MAX(month_value_10) as month_value_10,
       MAX(month_value_11) as month_value_11,
       MAX(month_value_12) as month_value_12

FROM (SELECT project_name,
             bis_project_id,
             max(condition_type)                                                                 as scondition_type,
             SUBSTR(real_completed_ym, 1, 4)                                                      as real_completed_y,
             SUBSTR(real_completed_ym, 6, 7)                                                      as real_completed_m,
             -- 计租面积  -- 合同签署 1-12月统计
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '01' THEN sum(cont_sign_a) ELSE NULL end as month_value_01,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '02' THEN sum(cont_sign_a) ELSE NULL end as month_value_02,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '03' THEN sum(cont_sign_a) ELSE NULL end as month_value_03,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '04' THEN sum(cont_sign_a) ELSE NULL end as month_value_04,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '05' THEN sum(cont_sign_a) ELSE NULL end as month_value_05,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '06' THEN sum(cont_sign_a) ELSE NULL end as month_value_06,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '07' THEN sum(cont_sign_a) ELSE NULL end as month_value_07,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '08' THEN sum(cont_sign_a) ELSE NULL end as month_value_08,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '09' THEN sum(cont_sign_a) ELSE NULL end as month_value_09,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '10' THEN sum(cont_sign_a) ELSE NULL end as month_value_10,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '11' THEN sum(cont_sign_a) ELSE NULL end as month_value_11,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '12' THEN sum(cont_sign_a) ELSE NULL end as month_value_12

      from dws.dws_bis_attract_investment_total_big_dt bis_attract_investment_total_big
      WHERE bis_attract_investment_total_big.condition_type = '3' -- 搜索条件类型（1：品牌 2：建筑面积 3：计租面积）
      GROUP BY project_name, bis_project_id, SUBSTR(real_completed_ym, 1, 4), SUBSTR(real_completed_ym, 6, 7)
     ) t
GROUP BY project_name, bis_project_id, real_completed_y;


-- 品牌  -- 进场 1-12月统计
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
SELECT project_name,
       bis_project_id,
       '5'                 as business_type, -- 业务类型(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更)
       max(scondition_type),                 -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       real_completed_y,
       MAX(month_value_01) as month_value_01,
       MAX(month_value_02) as month_value_02,
       MAX(month_value_03) as month_value_03,
       MAX(month_value_04) as month_value_04,
       MAX(month_value_05) as month_value_05,
       MAX(month_value_06) as month_value_06,
       MAX(month_value_07) as month_value_07,
       MAX(month_value_08) as month_value_08,
       MAX(month_value_09) as month_value_09,
       MAX(month_value_10) as month_value_10,
       MAX(month_value_11) as month_value_11,
       MAX(month_value_12) as month_value_12

FROM (SELECT project_name,
             bis_project_id,
             max(condition_type)                                                             as scondition_type,
             SUBSTR(real_completed_ym, 1, 4)                                                  as real_completed_y,
             SUBSTR(real_completed_ym, 6, 7)                                                  as real_completed_m,
             -- 品牌  -- 进场 1-12月统计
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '01' THEN sum(enter_a) ELSE NULL end as month_value_01,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '02' THEN sum(enter_a) ELSE NULL end as month_value_02,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '03' THEN sum(enter_a) ELSE NULL end as month_value_03,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '04' THEN sum(enter_a) ELSE NULL end as month_value_04,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '05' THEN sum(enter_a) ELSE NULL end as month_value_05,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '06' THEN sum(enter_a) ELSE NULL end as month_value_06,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '07' THEN sum(enter_a) ELSE NULL end as month_value_07,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '08' THEN sum(enter_a) ELSE NULL end as month_value_08,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '09' THEN sum(enter_a) ELSE NULL end as month_value_09,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '10' THEN sum(enter_a) ELSE NULL end as month_value_10,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '11' THEN sum(enter_a) ELSE NULL end as month_value_11,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '12' THEN sum(enter_a) ELSE NULL end as month_value_12

      from dws.dws_bis_attract_investment_total_big_dt bis_attract_investment_total_big
      WHERE bis_attract_investment_total_big.condition_type = '1' -- 搜索条件类型（1：品牌 2：建筑面积 3：计租面积）
      GROUP BY project_name, bis_project_id, SUBSTR(real_completed_ym, 1, 4), SUBSTR(real_completed_ym, 6, 7)
     ) t
GROUP BY project_name, bis_project_id, real_completed_y;


-- 建筑面积  -- 进场 1-12月统计
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
SELECT project_name,
       bis_project_id,
       '5'                 as business_type, -- 业务类型(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更)
       max(scondition_type),                 -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       real_completed_y,
       MAX(month_value_01) as month_value_01,
       MAX(month_value_02) as month_value_02,
       MAX(month_value_03) as month_value_03,
       MAX(month_value_04) as month_value_04,
       MAX(month_value_05) as month_value_05,
       MAX(month_value_06) as month_value_06,
       MAX(month_value_07) as month_value_07,
       MAX(month_value_08) as month_value_08,
       MAX(month_value_09) as month_value_09,
       MAX(month_value_10) as month_value_10,
       MAX(month_value_11) as month_value_11,
       MAX(month_value_12) as month_value_12

FROM (SELECT project_name,
             bis_project_id,
             max(condition_type)                                                             as scondition_type,
             SUBSTR(real_completed_ym, 1, 4)                                                  as real_completed_y,
             SUBSTR(real_completed_ym, 6, 7)                                                  as real_completed_m,
             -- 建筑面积  -- 进场 1-12月统计
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '01' THEN sum(enter_a) ELSE NULL end as month_value_01,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '02' THEN sum(enter_a) ELSE NULL end as month_value_02,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '03' THEN sum(enter_a) ELSE NULL end as month_value_03,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '04' THEN sum(enter_a) ELSE NULL end as month_value_04,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '05' THEN sum(enter_a) ELSE NULL end as month_value_05,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '06' THEN sum(enter_a) ELSE NULL end as month_value_06,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '07' THEN sum(enter_a) ELSE NULL end as month_value_07,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '08' THEN sum(enter_a) ELSE NULL end as month_value_08,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '09' THEN sum(enter_a) ELSE NULL end as month_value_09,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '10' THEN sum(enter_a) ELSE NULL end as month_value_10,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '11' THEN sum(enter_a) ELSE NULL end as month_value_11,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '12' THEN sum(enter_a) ELSE NULL end as month_value_12

      from dws.dws_bis_attract_investment_total_big_dt bis_attract_investment_total_big
      WHERE bis_attract_investment_total_big.condition_type = '2' -- 搜索条件类型（1：品牌 2：建筑面积 3：计租面积）
      GROUP BY project_name, bis_project_id, SUBSTR(real_completed_ym, 1, 4), SUBSTR(real_completed_ym, 6, 7)
     ) t
GROUP BY project_name, bis_project_id, real_completed_y;


-- 计租面积  -- 进场 1-12月统计
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
SELECT project_name,
       bis_project_id,
       '5'                 as business_type, -- 业务类型(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更)
       max(scondition_type),                 -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       real_completed_y,
       MAX(month_value_01) as month_value_01,
       MAX(month_value_02) as month_value_02,
       MAX(month_value_03) as month_value_03,
       MAX(month_value_04) as month_value_04,
       MAX(month_value_05) as month_value_05,
       MAX(month_value_06) as month_value_06,
       MAX(month_value_07) as month_value_07,
       MAX(month_value_08) as month_value_08,
       MAX(month_value_09) as month_value_09,
       MAX(month_value_10) as month_value_10,
       MAX(month_value_11) as month_value_11,
       MAX(month_value_12) as month_value_12

FROM (SELECT project_name,
             bis_project_id,
             max(condition_type)                                                             as scondition_type,
             SUBSTR(real_completed_ym, 1, 4)                                                  as real_completed_y,
             SUBSTR(real_completed_ym, 6, 7)                                                  as real_completed_m,
             -- 计租面积  -- 进场 1-12月统计
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '01' THEN sum(enter_a) ELSE NULL end as month_value_01,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '02' THEN sum(enter_a) ELSE NULL end as month_value_02,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '03' THEN sum(enter_a) ELSE NULL end as month_value_03,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '04' THEN sum(enter_a) ELSE NULL end as month_value_04,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '05' THEN sum(enter_a) ELSE NULL end as month_value_05,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '06' THEN sum(enter_a) ELSE NULL end as month_value_06,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '07' THEN sum(enter_a) ELSE NULL end as month_value_07,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '08' THEN sum(enter_a) ELSE NULL end as month_value_08,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '09' THEN sum(enter_a) ELSE NULL end as month_value_09,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '10' THEN sum(enter_a) ELSE NULL end as month_value_10,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '11' THEN sum(enter_a) ELSE NULL end as month_value_11,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '12' THEN sum(enter_a) ELSE NULL end as month_value_12

      from dws.dws_bis_attract_investment_total_big_dt bis_attract_investment_total_big
      WHERE bis_attract_investment_total_big.condition_type = '3' -- 搜索条件类型（1：品牌 2：建筑面积 3：计租面积）
      GROUP BY project_name, bis_project_id, SUBSTR(real_completed_ym, 1, 4), SUBSTR(real_completed_ym, 6, 7)
     ) t
GROUP BY project_name, bis_project_id, real_completed_y;


-- 品牌  -- 开业 1-12月统计
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
SELECT project_name,
       bis_project_id,
       '6'                 as business_type, -- 业务类型(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更)
       max(scondition_type),                 -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       real_completed_y,
       MAX(month_value_01) as month_value_01,
       MAX(month_value_02) as month_value_02,
       MAX(month_value_03) as month_value_03,
       MAX(month_value_04) as month_value_04,
       MAX(month_value_05) as month_value_05,
       MAX(month_value_06) as month_value_06,
       MAX(month_value_07) as month_value_07,
       MAX(month_value_08) as month_value_08,
       MAX(month_value_09) as month_value_09,
       MAX(month_value_10) as month_value_10,
       MAX(month_value_11) as month_value_11,
       MAX(month_value_12) as month_value_12

FROM (SELECT project_name,
             bis_project_id,
             max(condition_type)                                                                      as scondition_type,
             SUBSTR(real_completed_ym, 1, 4)                                                           as real_completed_y,
             SUBSTR(real_completed_ym, 6, 7)                                                           as real_completed_m,
             -- 品牌  -- 开业 1-12月统计
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '01' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_01,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '02' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_02,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '03' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_03,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '04' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_04,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '05' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_05,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '06' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_06,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '07' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_07,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '08' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_08,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '09' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_09,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '10' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_10,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '11' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_11,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '12' THEN sum(start_business_a) ELSE NULL end as month_value_12

      from dws.dws_bis_attract_investment_total_big_dt bis_attract_investment_total_big
      WHERE bis_attract_investment_total_big.condition_type = '1' -- 搜索条件类型（1：品牌 2：建筑面积 3：计租面积）
      GROUP BY project_name, bis_project_id, SUBSTR(real_completed_ym, 1, 4), SUBSTR(real_completed_ym, 6, 7)
     ) t
GROUP BY project_name, bis_project_id, real_completed_y;


-- 建筑面积  -- 开业 1-12月统计
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
SELECT project_name,
       bis_project_id,
       '6'                 as business_type, -- 业务类型(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更)
       max(scondition_type),                 -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       real_completed_y,
       MAX(month_value_01) as month_value_01,
       MAX(month_value_02) as month_value_02,
       MAX(month_value_03) as month_value_03,
       MAX(month_value_04) as month_value_04,
       MAX(month_value_05) as month_value_05,
       MAX(month_value_06) as month_value_06,
       MAX(month_value_07) as month_value_07,
       MAX(month_value_08) as month_value_08,
       MAX(month_value_09) as month_value_09,
       MAX(month_value_10) as month_value_10,
       MAX(month_value_11) as month_value_11,
       MAX(month_value_12) as month_value_12

FROM (SELECT project_name,
             bis_project_id,
             max(condition_type)                                                                      as scondition_type,
             SUBSTR(real_completed_ym, 1, 4)                                                           as real_completed_y,
             SUBSTR(real_completed_ym, 6, 7)                                                           as real_completed_m,
             -- 建筑面积  -- 开业 1-12月统计
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '01' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_01,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '02' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_02,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '03' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_03,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '04' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_04,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '05' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_05,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '06' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_06,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '07' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_07,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '08' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_08,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '09' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_09,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '10' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_10,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '11' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_11,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '12' THEN sum(start_business_a) ELSE NULL end as month_value_12

      from dws.dws_bis_attract_investment_total_big_dt bis_attract_investment_total_big
      WHERE bis_attract_investment_total_big.condition_type = '2' -- 搜索条件类型（1：品牌 2：建筑面积 3：计租面积）
      GROUP BY project_name, bis_project_id, SUBSTR(real_completed_ym, 1, 4), SUBSTR(real_completed_ym, 6, 7)
     ) t
GROUP BY project_name, bis_project_id, real_completed_y;


-- 计租面积  -- 开业 1-12月统计
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
SELECT project_name,
       bis_project_id,
       '6'                 as business_type, -- 业务类型(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更)
       max(scondition_type),                 -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       real_completed_y,
       MAX(month_value_01) as month_value_01,
       MAX(month_value_02) as month_value_02,
       MAX(month_value_03) as month_value_03,
       MAX(month_value_04) as month_value_04,
       MAX(month_value_05) as month_value_05,
       MAX(month_value_06) as month_value_06,
       MAX(month_value_07) as month_value_07,
       MAX(month_value_08) as month_value_08,
       MAX(month_value_09) as month_value_09,
       MAX(month_value_10) as month_value_10,
       MAX(month_value_11) as month_value_11,
       MAX(month_value_12) as month_value_12

FROM (SELECT project_name,
             bis_project_id,
             max(condition_type)                                                                      as scondition_type,
             SUBSTR(real_completed_ym, 1, 4)                                                           as real_completed_y,
             SUBSTR(real_completed_ym, 6, 7)                                                           as real_completed_m,
             -- 计租面积  -- 开业 1-12月统计
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '01' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_01,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '02' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_02,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '03' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_03,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '04' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_04,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '05' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_05,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '06' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_06,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '07' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_07,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '08' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_08,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '09' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_09,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '10' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_10,
             case
                 WHEN SUBSTR(real_completed_ym, 6, 7) = '11' THEN sum(start_business_a)
                 ELSE NULL end                                                                         as month_value_11,
             case WHEN SUBSTR(real_completed_ym, 6, 7) = '12' THEN sum(start_business_a) ELSE NULL end as month_value_12

      from dws.dws_bis_attract_investment_total_big_dt bis_attract_investment_total_big
      WHERE bis_attract_investment_total_big.condition_type = '3' -- 搜索条件类型（1：品牌 2：建筑面积 3：计租面积）
      GROUP BY project_name, bis_project_id, SUBSTR(real_completed_ym, 1, 4), SUBSTR(real_completed_ym, 6, 7)
     ) t
GROUP BY project_name, bis_project_id, real_completed_y;


-- 铺位信息变更
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
select bis_project.project_name,   -- 项目名称
       bis_project.bis_project_id, -- 项目id
       table1.business_type,       -- 审批类型（(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更）
       '4' as scondition_type,     -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       table1.complete_date,       -- 审批完成时间（年）
       table1.intention_t_01,      -- 一月审批数量
       table1.intention_t_02,      -- 二月审批数量
       table1.intention_t_03,      -- 三月审批数量
       table1.intention_t_04,      -- 四月审批数量
       table1.intention_t_05,      -- 五月审批数量
       table1.intention_t_06,      -- 六月审批数量
       table1.intention_t_07,      -- 七月审批数量
       table1.intention_t_08,      -- 八月审批数量
       table1.intention_t_09,      -- 九月审批数量
       table1.intention_t_10,      -- 十月审批数量
       table1.intention_t_11,      -- 十一月审批数量
       table1.intention_t_12       -- 十二月审批数量

from ( -- 意向
         select '16'                 as business_type,  -- 审批类型
                t.LAND_PROJECT_CD,
                max(t.complete_date) as complete_date,
                max(intention_t_01)  as intention_t_01, -- 一月开业数
                max(intention_t_02)  as intention_t_02,
                max(intention_t_03)  as intention_t_03,
                max(intention_t_04)  as intention_t_04,
                max(intention_t_05)  as intention_t_05,
                max(intention_t_06)  as intention_t_06,
                max(intention_t_07)  as intention_t_07,
                max(intention_t_08)  as intention_t_08,
                max(intention_t_09)  as intention_t_09,
                max(intention_t_10)  as intention_t_10,
                max(intention_t_11)  as intention_t_11,
                max(intention_t_12)  as intention_t_12
         from (select land_project_cd,
                      substr(RES_APPROVE_INFO.complete_date, 0, 4) AS complete_date,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '01' then count(1)
                          else null end                            as intention_t_01,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '02' then count(1)
                          else null end                            as intention_t_02,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '03' then count(1)
                          else null end                            as intention_t_03,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '04' then count(1)
                          else null end                            as intention_t_04,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '05' then count(1)
                          else null end                            as intention_t_05,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '06' then count(1)
                          else null end                            as intention_t_06,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '07' then count(1)
                          else null end                            as intention_t_07,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '08' then count(1)
                          else null end                            as intention_t_08,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '09' then count(1)
                          else null end                            as intention_t_09,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '10' then count(1)
                          else null end                            as intention_t_10,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '11' then count(1)
                          else null end                            as intention_t_11,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '12' then count(1)
                          else null end                            as intention_t_12
               from ods.ods_pl_powerdes_res_approve_info_dt RES_APPROVE_INFO
               where AUTH_TYPE_CD in
                     (select AUTH_TYPE_CD
                      from ods.ods_pl_powerdes_res_auth_type_dt RES_AUTH_TYPE
                      where ACTIVE = '1'
                        AND RES_MODULE_ID in ('8a7b859e70e8a8c60170ec714b264818')-- 铺位信息变更网批 总部+项目
                     )
                 and status_cd = '2' -- 网批完成
                 -- and LAND_PROJECT= '义乌商业公司'
                 -- and substr(RES_APPROVE_INFO.complete_date,0,4)  in('2019')
               group by RES_APPROVE_INFO.land_project_cd, substr(RES_APPROVE_INFO.complete_date, 0, 4),
                        substr(RES_APPROVE_INFO.complete_date, 6, 2)) t
         group by t.LAND_PROJECT_CD, t.complete_date
     ) table1
         left join ods.ods_pl_powerdes_bis_project_dt bis_project
                   on table1.land_project_cd = bis_project.org_cd
where bis_project.oper_status = '1' and is_business_project = '1';


-- 拆合铺
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
select bis_project.project_name,   -- 项目名称
       bis_project.bis_project_id, -- 项目id
       table1.business_type,       -- 审批类型（(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更）
       '4' as scondition_type,     -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       table1.complete_date,       -- 审批完成时间（年）
       table1.intention_t_01,      -- 一月审批数量
       table1.intention_t_02,      -- 二月审批数量
       table1.intention_t_03,      -- 三月审批数量
       table1.intention_t_04,      -- 四月审批数量
       table1.intention_t_05,      -- 五月审批数量
       table1.intention_t_06,      -- 六月审批数量
       table1.intention_t_07,      -- 七月审批数量
       table1.intention_t_08,      -- 八月审批数量
       table1.intention_t_09,      -- 九月审批数量
       table1.intention_t_10,      -- 十月审批数量
       table1.intention_t_11,      -- 十一月审批数量
       table1.intention_t_12       -- 十二月审批数量

from ( -- 意向
         select '15'                 as business_type,  -- 审批类型
                t.LAND_PROJECT_CD,
                max(t.complete_date) as complete_date,
                max(intention_t_01)  as intention_t_01, -- 一月开业数
                max(intention_t_02)  as intention_t_02,
                max(intention_t_03)  as intention_t_03,
                max(intention_t_04)  as intention_t_04,
                max(intention_t_05)  as intention_t_05,
                max(intention_t_06)  as intention_t_06,
                max(intention_t_07)  as intention_t_07,
                max(intention_t_08)  as intention_t_08,
                max(intention_t_09)  as intention_t_09,
                max(intention_t_10)  as intention_t_10,
                max(intention_t_11)  as intention_t_11,
                max(intention_t_12)  as intention_t_12
         from (select land_project_cd,
                      substr(RES_APPROVE_INFO.complete_date, 0, 4) AS complete_date,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '01' then count(1)
                          else null end                            as intention_t_01,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '02' then count(1)
                          else null end                            as intention_t_02,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '03' then count(1)
                          else null end                            as intention_t_03,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '04' then count(1)
                          else null end                            as intention_t_04,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '05' then count(1)
                          else null end                            as intention_t_05,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '06' then count(1)
                          else null end                            as intention_t_06,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '07' then count(1)
                          else null end                            as intention_t_07,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '08' then count(1)
                          else null end                            as intention_t_08,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '09' then count(1)
                          else null end                            as intention_t_09,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '10' then count(1)
                          else null end                            as intention_t_10,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '11' then count(1)
                          else null end                            as intention_t_11,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '12' then count(1)
                          else null end                            as intention_t_12
               from ods.ods_pl_powerdes_res_approve_info_dt RES_APPROVE_INFO
               where AUTH_TYPE_CD in
                     (select AUTH_TYPE_CD
                      from ods.ods_pl_powerdes_res_auth_type_dt RES_AUTH_TYPE
                      where ACTIVE = '1'
                        AND RES_MODULE_ID in ('8a7b859e70e8a8c60170ec714b264818') -- 拆合铺网批 总部+项目
                     )
                 and status_cd = '2' -- 网批完成
                 -- and LAND_PROJECT_CD = '637'
                 -- and to_char(RES_APPROVE_INFO.complete_date, 'YYYY')  in('2019','2020')
               group by RES_APPROVE_INFO.land_project_cd, substr(RES_APPROVE_INFO.complete_date, 0, 4),
                        substr(RES_APPROVE_INFO.complete_date, 6, 2)) t
         group by t.LAND_PROJECT_CD, t.complete_date
     ) table1
         left join ods.ods_pl_powerdes_bis_project_dt bis_project
                   on table1.land_project_cd = bis_project.org_cd
where bis_project.oper_status = '1' and is_business_project = '1';

-- 网批驳回率
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
select bis_project.project_name,                                                                -- 项目名称
       bis_project.bis_project_id,                                                              -- 项目id
       '14'                                                                 as business_type,   -- 审批类型（(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更）
       '4'                                                                  as scondition_type, -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       table1.complete_date,                                                                    -- 审批完成时间（年）
       case
           when table1.intention_t_01 = 0 then 0
           else round(table2.intention_t_01 / table1.intention_t_01, 2) end as bo01,            -- 一月审批驳回率
       case
           when table1.intention_t_02 = 0 then 0
           else round(table2.intention_t_02 / table1.intention_t_02, 2) end as bo02,            -- 二月审批驳回率
       case
           when table1.intention_t_03 = 0 then 0
           else round(table2.intention_t_03 / table1.intention_t_03, 2) end as bo03,            -- 三月审批驳回率
       case
           when table1.intention_t_04 = 0 then 0
           else round(table2.intention_t_04 / table1.intention_t_04, 2) end as bo04,            -- 四月审批驳回率
       case
           when table1.intention_t_05 = 0 then 0
           else round(table2.intention_t_05 / table1.intention_t_05, 2) end as bo05,            -- 五月审批驳回率
       case
           when table1.intention_t_06 = 0 then 0
           else round(table2.intention_t_06 / table1.intention_t_06, 2) end as bo06,            -- 六月审批驳回率
       case
           when table1.intention_t_07 = 0 then 0
           else round(table2.intention_t_07 / table1.intention_t_07, 2) end as bo07,            -- 七月审批驳回率
       case
           when table1.intention_t_08 = 0 then 0
           else round(table2.intention_t_08 / table1.intention_t_08, 2) end as bo08,            -- 八月审批驳回率
       case
           when table1.intention_t_09 = 0 then 0
           else round(table2.intention_t_09 / table1.intention_t_09, 2) end as bo09,            -- 九月审批驳回率
       case
           when table1.intention_t_10 = 0 then 0
           else round(table2.intention_t_10 / table1.intention_t_10, 2) end as bo10,            -- 十月审批驳回率
       case
           when table1.intention_t_11 = 0 then 0
           else round(table2.intention_t_11 / table1.intention_t_11, 2) end as bo11,            -- 十一月审批驳回率
       case
           when table1.intention_t_12 = 0 then 0
           else round(table2.intention_t_12 / table1.intention_t_12, 2) end as bo12             -- 十二月审批驳回率

from ( -- 网批完成数量
         select t.LAND_PROJECT_CD,
                t.LAND_PROJECT,
                max(t.complete_date)        as complete_date,
                max(t.STATUS_CD),
                nvl(max(intention_t_01), 0) as intention_t_01, -- 一月网批完成总数
                nvl(max(intention_t_02), 0) as intention_t_02,
                nvl(max(intention_t_03), 0) as intention_t_03,
                nvl(max(intention_t_04), 0) as intention_t_04,
                nvl(max(intention_t_05), 0) as intention_t_05,
                nvl(max(intention_t_06), 0) as intention_t_06,
                nvl(max(intention_t_07), 0) as intention_t_07,
                nvl(max(intention_t_08), 0) as intention_t_08,
                nvl(max(intention_t_09), 0) as intention_t_09,
                nvl(max(intention_t_10), 0) as intention_t_10,
                nvl(max(intention_t_11), 0) as intention_t_11,
                nvl(max(intention_t_12), 0) as intention_t_12
         from (select land_project_cd,
                      LAND_PROJECT,
                      substr(RES_APPROVE_INFO.complete_date, 0, 4) AS complete_date,
                      max(STATUS_CD)                               as STATUS_CD,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '01' then count(1)
                          else null end                            as intention_t_01,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '02' then count(1)
                          else null end                            as intention_t_02,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '03' then count(1)
                          else null end                            as intention_t_03,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '04' then count(1)
                          else null end                            as intention_t_04,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '05' then count(1)
                          else null end                            as intention_t_05,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '06' then count(1)
                          else null end                            as intention_t_06,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '07' then count(1)
                          else null end                            as intention_t_07,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '08' then count(1)
                          else null end                            as intention_t_08,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '09' then count(1)
                          else null end                            as intention_t_09,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '10' then count(1)
                          else null end                            as intention_t_10,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '11' then count(1)
                          else null end                            as intention_t_11,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '12' then count(1)
                          else null end                            as intention_t_12
               from ods.ods_pl_powerdes_res_approve_info_dt RES_APPROVE_INFO
               where RES_APPROVE_INFO.status_cd = '2' -- 网批完成和驳回
                     -- and LAND_PROJECT_CD = '637'
                     -- and to_char(RES_APPROVE_INFO.complete_date, 'YYYY')  in('2019','2020')
               group by RES_APPROVE_INFO.land_project_cd, RES_APPROVE_INFO.LAND_PROJECT,
                        substr(RES_APPROVE_INFO.complete_date, 0, 4), substr(RES_APPROVE_INFO.complete_date, 6, 2)
              ) t
         group by t.LAND_PROJECT_CD, t.LAND_PROJECT, t.complete_date
     ) table1
         left join
     ( -- 网批驳回数量
         select t.LAND_PROJECT_CD,
                max(t.complete_date) as complete_date,
                max(t.STATUS_CD),
                max(intention_t_01)  as intention_t_01, -- 一月网批完成总数
                max(intention_t_02)  as intention_t_02,
                max(intention_t_03)  as intention_t_03,
                max(intention_t_04)  as intention_t_04,
                max(intention_t_05)  as intention_t_05,
                max(intention_t_06)  as intention_t_06,
                max(intention_t_07)  as intention_t_07,
                max(intention_t_08)  as intention_t_08,
                max(intention_t_09)  as intention_t_09,
                max(intention_t_10)  as intention_t_10,
                max(intention_t_11)  as intention_t_11,
                max(intention_t_12)  as intention_t_12
         from (select land_project_cd,
                      LAND_PROJECT,
                      substr(RES_APPROVE_INFO.complete_date, 0, 4) AS complete_date,
                      max(STATUS_CD)                               as STATUS_CD,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '01' then count(1)
                          else null end                            as intention_t_01,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '02' then count(1)
                          else null end                            as intention_t_02,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '03' then count(1)
                          else null end                            as intention_t_03,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '04' then count(1)
                          else null end                            as intention_t_04,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '05' then count(1)
                          else null end                            as intention_t_05,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '06' then count(1)
                          else null end                            as intention_t_06,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '07' then count(1)
                          else null end                            as intention_t_07,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '08' then count(1)
                          else null end                            as intention_t_08,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '09' then count(1)
                          else null end                            as intention_t_09,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '10' then count(1)
                          else null end                            as intention_t_10,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '11' then count(1)
                          else null end                            as intention_t_11,
                      case
                          when substr(RES_APPROVE_INFO.complete_date, 6, 2) = '12' then count(1)
                          else null end                            as intention_t_12
               from ods.ods_pl_powerdes_res_approve_info_dt RES_APPROVE_INFO
               where RES_APPROVE_INFO.status_cd = '3' -- 网批完成和驳回
                     -- and LAND_PROJECT_CD = '637'
                     -- and to_char(RES_APPROVE_INFO.complete_date, 'YYYY')  in('2019','2020')
               group by RES_APPROVE_INFO.land_project_cd, RES_APPROVE_INFO.LAND_PROJECT,
                        substr(RES_APPROVE_INFO.complete_date, 0, 4),
                        substr(RES_APPROVE_INFO.complete_date, 6, 2)
              ) t
         group by t.LAND_PROJECT_CD, t.LAND_PROJECT, t.complete_date
     ) table2
     on table1.land_project_cd = table2.land_project_cd and table1.complete_date = table2.complete_date
         left join ods.ods_pl_powerdes_bis_project_dt bis_project on table1.land_project_cd = bis_project.org_cd
where bis_project.oper_status = '1' and is_business_project = '1';


-- 租金收缴率
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
select t1.project_name,                                             -- 项目名称
       t1.bis_project_id,                                           -- 项目id
       '7' as business_type,                                        -- 审批类型（(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴率,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更）
       '4' as scondition_type,                                      -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       t1.must_year,                                                -- 审批完成时间（年）
       if(t1.money_01 = 0, 0, round(t2.money_01 / t1.money_01, 2)), -- 一月租金收缴率
       if(t1.money_2 = 0, 0, round(t2.money_2 / t1.money_2, 2)),
       if(t1.money_3 = 0, 0, round(t2.money_3 / t1.money_3, 2)),
       if(t1.money_4 = 0, 0, round(t2.money_4 / t1.money_4, 2)),
       if(t1.money_5 = 0, 0, round(t2.money_5 / t1.money_5, 2)),
       if(t1.money_6 = 0, 0, round(t2.money_6 / t1.money_6, 2)),
       if(t1.money_7 = 0, 0, round(t2.money_7 / t1.money_7, 2)),
       if(t1.money_8 = 0, 0, round(t2.money_8 / t1.money_8, 2)),
       if(t1.money_9 = 0, 0, round(t2.money_9 / t1.money_9, 2)),
       if(t1.money_10 = 0, 0, round(t2.money_10 / t1.money_10, 2)),
       if(t1.money_11 = 0, 0, round(t2.money_11 / t1.money_11, 2)),
       if(t1.money_12 = 0, 0, round(t2.money_12 / t1.money_12, 2))
from (
         select table1.ORG_CD,
                table1.BIS_PROJECT_ID,
                table1.project_name,
                max(table1.must_type) as must_type, -- 1:租金应收
                max(table1.must_date) as must_year, -- 应收日期（年）
                MAX(table1.money_01)  AS money_01,  -- 一月应收
                MAX(table1.money_2)   AS money_2,
                MAX(table1.money_3)   AS money_3,
                MAX(table1.money_4)   AS money_4,
                MAX(table1.money_5)   AS money_5,
                MAX(table1.money_6)   AS money_6,
                MAX(table1.money_7)   AS money_7,
                MAX(table1.money_8)   AS money_8,
                MAX(table1.money_9)   AS money_9,
                MAX(table1.money_10)  AS money_10,
                MAX(table1.money_11)  AS money_11,
                MAX(table1.money_12)  AS money_12
         from (
                  select bis_project.ORG_CD,
                         bis_project.project_name,
                         bis_must2.BIS_PROJECT_ID,
                         max(bis_must2.must_type) as             must_type, -- 应收类型
                         substr(bis_must2.MUST_YEAR_MONTH, 0, 4) must_date, -- 应收日期(年)
                         case
                             when SUBSTR(bis_must2.MUST_YEAR_MONTH, 6, 7) = '01' then sum(MONEY)
                             else null end        as             money_01,  -- 一月应收
                         case
                             when SUBSTR(bis_must2.MUST_YEAR_MONTH, 6, 7) = '02' then sum(MONEY)
                             else null end        as             money_2,
                         case
                             when SUBSTR(bis_must2.MUST_YEAR_MONTH, 6, 7) = '03' then sum(MONEY)
                             else null end        as             money_3,
                         case
                             when SUBSTR(bis_must2.MUST_YEAR_MONTH, 6, 7) = '04' then sum(MONEY)
                             else null end        as             money_4,
                         case
                             when SUBSTR(bis_must2.MUST_YEAR_MONTH, 6, 7) = '05' then sum(MONEY)
                             else null end        as             money_5,
                         case
                             when SUBSTR(bis_must2.MUST_YEAR_MONTH, 6, 7) = '06' then sum(MONEY)
                             else null end        as             money_6,
                         case
                             when SUBSTR(bis_must2.MUST_YEAR_MONTH, 6, 7) = '07' then sum(MONEY)
                             else null end        as             money_7,
                         case
                             when SUBSTR(bis_must2.MUST_YEAR_MONTH, 6, 7) = '08' then sum(MONEY)
                             else null end        as             money_8,
                         case
                             when SUBSTR(bis_must2.MUST_YEAR_MONTH, 6, 7) = '09' then sum(MONEY)
                             else null end        as             money_9,
                         case
                             when SUBSTR(bis_must2.MUST_YEAR_MONTH, 6, 7) = '10' then sum(MONEY)
                             else null end        as             money_10,
                         case
                             when SUBSTR(bis_must2.MUST_YEAR_MONTH, 6, 7) = '11' then sum(MONEY)
                             else null end        as             money_11,
                         case
                             when SUBSTR(bis_must2.MUST_YEAR_MONTH, 6, 7) = '12' then sum(MONEY)
                             else null end        as             money_12
                  from ods.ods_pl_powerdes_bis_must2_dt bis_must2
                           left join ods.ods_pl_powerdes_bis_project_dt bis_project
                                     on BIS_MUST2.BIS_PROJECT_ID = BIS_PROJECT.BIS_PROJECT_ID
                  where bis_must2.MUST_TYPE = '1'
                    and bis_project.oper_status = '1' and is_business_project = '1'
                    -- and BIS_MUST2.BIS_PROJECT_ID in('402834702db81ec3012dbca135f20c6a','40282b8927a42dff0127a433cd3100d5')
                    -- and substr(bis_must2.MUST_YEAR_MONTH, 0, 4) = '2021'
                  group by BIS_MUST2.BIS_PROJECT_ID, bis_project.project_name,
                           BIS_PROJECT.ORG_CD, substr(bis_must2.MUST_YEAR_MONTH, 0, 4),
                           SUBSTR(bis_must2.MUST_YEAR_MONTH, 6, 7)
              ) table1
         group by table1.BIS_PROJECT_ID, table1.project_name, table1.ORG_CD, table1.must_date
     ) t1
         left join (
    select table1.ORG_CD,
           table1.BIS_PROJECT_ID,
           nvl(max(table1.fact_type), 0) as fact_type, -- 1:租金实收
           nvl(max(table1.fact_date), 0) as fact_year, -- 实收日期（年）
           nvl(MAX(table1.money_01), 0)  AS money_01,  -- 一月实收
           nvl(MAX(table1.money_2), 0)   AS money_2,
           nvl(MAX(table1.money_3), 0)   AS money_3,
           nvl(MAX(table1.money_4), 0)   AS money_4,
           nvl(MAX(table1.money_5), 0)   AS money_5,
           nvl(MAX(table1.money_6), 0)   AS money_6,
           nvl(MAX(table1.money_7), 0)   AS money_7,
           nvl(MAX(table1.money_8), 0)   AS money_8,
           nvl(MAX(table1.money_9), 0)   AS money_9,
           nvl(MAX(table1.money_10), 0)  AS money_10,
           nvl(MAX(table1.money_11), 0)  AS money_11,
           nvl(MAX(table1.money_12), 0)  AS money_12
    from (
             select bis_project.ORG_CD,
                    BIS_FACT2.BIS_PROJECT_ID,
                    max(BIS_FACT2.FACT_TYPE)                                               as fact_type, -- 实收类型
                    substr(FACT_DATE, 0, 4)                                                   fact_date, -- 实收日期(年)
                    case when substr(FACT_DATE, 6, 7) = '01' then sum(MONEY) else null end as money_01,  -- 一月实收
                    case when substr(FACT_DATE, 6, 7) = '02' then sum(MONEY) else null end as money_2,
                    case when substr(FACT_DATE, 6, 7) = '03' then sum(MONEY) else null end as money_3,
                    case when substr(FACT_DATE, 6, 7) = '04' then sum(MONEY) else null end as money_4,
                    case when substr(FACT_DATE, 6, 7) = '05' then sum(MONEY) else null end as money_5,
                    case when substr(FACT_DATE, 6, 7) = '06' then sum(MONEY) else null end as money_6,
                    case when substr(FACT_DATE, 6, 7) = '07' then sum(MONEY) else null end as money_7,
                    case when substr(FACT_DATE, 6, 7) = '08' then sum(MONEY) else null end as money_8,
                    case when substr(FACT_DATE, 6, 7) = '09' then sum(MONEY) else null end as money_9,
                    case when substr(FACT_DATE, 6, 7) = '10' then sum(MONEY) else null end as money_10,
                    case when substr(FACT_DATE, 6, 7) = '11' then sum(MONEY) else null end as money_11,
                    case when substr(FACT_DATE, 6, 7) = '12' then sum(MONEY) else null end as money_12
             from ods.ods_pl_powerdes_bis_fact2_dt BIS_FACT2
                      left join ods.ods_pl_powerdes_bis_project_dt bis_project
                                on BIS_FACT2.BIS_PROJECT_ID = BIS_PROJECT.BIS_PROJECT_ID
             where BIS_FACT2.fact_type = '1'
               and IS_DELETE = '1'
               and bis_project.oper_status = '1' and is_business_project = '1'
               -- and BIS_FACT2.BIS_PROJECT_ID = '40282b8927a42dff0127a433647400aa'
               -- and to_char(FACT_DATE, 'YYYY') = '2021'
             group by BIS_FACT2.BIS_PROJECT_ID, BIS_PROJECT.ORG_CD, substr(FACT_DATE, 0, 4),
                      substr(FACT_DATE, 6, 7)
         ) table1
    group by table1.BIS_PROJECT_ID, table1.ORG_CD, table1.fact_date
) t2 on t1.BIS_PROJECT_ID = t2.BIS_PROJECT_ID and t1.must_year = t2.fact_year;


-- 租金达成率
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
select t1.project_name,                                                           -- 项目名称
       t1.bis_project_id,                                                         -- 项目id
       '8' as business_type,                                                      -- 审批类型（(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴率,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更）
       '4' as scondition_type,                                                    -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       string(t1.budget_year),                                                    -- 审批完成时间（年）
       if(t1.budget_money_01 = 0, 0, round(t2.money_01 / t1.budget_money_01, 2)), -- 一月租金达成率
       if(t1.budget_money_02 = 0, 0, round(t2.money_2 / t1.budget_money_02, 2)),
       if(t1.budget_money_03 = 0, 0, round(t2.money_3 / t1.budget_money_03, 2)),
       if(t1.budget_money_04 = 0, 0, round(t2.money_4 / t1.budget_money_04, 2)),
       if(t1.budget_money_05 = 0, 0, round(t2.money_5 / t1.budget_money_05, 2)),
       if(t1.budget_money_06 = 0, 0, round(t2.money_6 / t1.budget_money_06, 2)),
       if(t1.budget_money_07 = 0, 0, round(t2.money_7 / t1.budget_money_07, 2)),
       if(t1.budget_money_08 = 0, 0, round(t2.money_8 / t1.budget_money_08, 2)),
       if(t1.budget_money_09 = 0, 0, round(t2.money_9 / t1.budget_money_09, 2)),
       if(t1.budget_money_10 = 0, 0, round(t2.money_10 / t1.budget_money_10, 2)),
       if(t1.budget_money_11 = 0, 0, round(t2.money_11 / t1.budget_money_11, 2)),
       if(t1.budget_money_12 = 0, 0, round(t2.money_12 / t1.budget_money_12, 2))
from (
         SELECT p.ORG_CD,
                p.BIS_PROJECT_ID,
                p.project_name,
                if(r.fee_type = '1', '租金', '物业费')      budget_type,     -- 预算类别
                max(r.YEAR) as                         budget_year,
                sum(if(month = 1, r.budget_money, 0))  budget_money_01, -- 一月租金预算
                sum(if(month = 2, r.budget_money, 0))  budget_money_02,
                sum(if(month = 3, r.budget_money, 0))  budget_money_03,
                sum(if(month = 4, r.budget_money, 0))  budget_money_04,
                sum(if(month = 5, r.budget_money, 0))  budget_money_05,
                sum(if(month = 6, r.budget_money, 0))  budget_money_06,
                sum(if(month = 7, r.budget_money, 0))  budget_money_07,
                sum(if(month = 8, r.budget_money, 0))  budget_money_08,
                sum(if(month = 9, r.budget_money, 0))  budget_money_09,
                sum(if(month = 10, r.budget_money, 0)) budget_money_10,
                sum(if(month = 11, r.budget_money, 0)) budget_money_11,
                sum(if(month = 12, r.budget_money, 0)) budget_money_12,
                sum(r.budget_money)
         FROM (
                  SELECT bis_project_id,
                         year,
                         month,
                         nvl(budget_income, 0)            as budget_money,
                         substr(string(sequnce_no), 1, 1) as fee_type
                  FROM ods.ods_pl_powerdes_bis_report_rent_quick_premoney_dt BIS_REPORT_RENT_QUICK_PREMONEY
                  WHERE sequnce_no in (111, 112, 113, 120, 130, 211, 212, 213, 220, 230)
                    -- AND year = '2019'
                    and substr(string(sequnce_no), 1, 1) = '1'
              ) r
                  LEFT JOIN
              ods.ods_pl_powerdes_bis_project_dt p ON r.bis_project_id = p.bis_project_id
         where p.oper_status = '1'
         GROUP BY p.BIS_PROJECT_ID, p.project_name, ORG_CD, r.YEAR, r.fee_type
     ) t1
         left join (
    select table1.ORG_CD,
           table1.BIS_PROJECT_ID,
           nvl(max(table1.fact_type), 0) as fact_type, -- 1:租金实收
           nvl(max(table1.fact_date), 0) as fact_year, -- 实收日期（年）
           nvl(MAX(table1.money_01), 0)  AS money_01,  -- 一月租金实收
           nvl(MAX(table1.money_2), 0)   AS money_2,
           nvl(MAX(table1.money_3), 0)   AS money_3,
           nvl(MAX(table1.money_4), 0)   AS money_4,
           nvl(MAX(table1.money_5), 0)   AS money_5,
           nvl(MAX(table1.money_6), 0)   AS money_6,
           nvl(MAX(table1.money_7), 0)   AS money_7,
           nvl(MAX(table1.money_8), 0)   AS money_8,
           nvl(MAX(table1.money_9), 0)   AS money_9,
           nvl(MAX(table1.money_10), 0)  AS money_10,
           nvl(MAX(table1.money_11), 0)  AS money_11,
           nvl(MAX(table1.money_12), 0)  AS money_12
    from (
             select bis_project.ORG_CD,
                    BIS_FACT2.BIS_PROJECT_ID,
                    max(BIS_FACT2.FACT_TYPE)                                                         as fact_type, -- 实收类型
                    substr(BIS_FACT2.FACT_DATE, 0, 4)                                                   fact_date, -- 实收日期(年)
                    case
                        when substr(BIS_FACT2.FACT_DATE, 6, 2) = '01' then sum(MONEY)
                        else null end                                                                as money_01,  -- 一月租金实收
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '02' then sum(MONEY) else null end as money_2,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '03' then sum(MONEY) else null end as money_3,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '04' then sum(MONEY) else null end as money_4,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '05' then sum(MONEY) else null end as money_5,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '06' then sum(MONEY) else null end as money_6,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '07' then sum(MONEY) else null end as money_7,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '08' then sum(MONEY) else null end as money_8,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '09' then sum(MONEY) else null end as money_9,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '10' then sum(MONEY) else null end as money_10,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '11' then sum(MONEY) else null end as money_11,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '12' then sum(MONEY) else null end as money_12
             from ods.ods_pl_powerdes_bis_fact2_dt BIS_FACT2
                      left join ods.ods_pl_powerdes_bis_project_dt bis_project
                                on BIS_FACT2.BIS_PROJECT_ID = BIS_PROJECT.BIS_PROJECT_ID
             where BIS_FACT2.FACT_TYPE = '1'
               and IS_DELETE = '1'
               and bis_project.oper_status = '1' and is_business_project = '1'
               -- and BIS_FACT2.BIS_PROJECT_ID = '40282b8927a42dff0127a433647400aa'
               -- and to_char(FACT_DATE, 'YYYY') = '2021'
             group by BIS_FACT2.BIS_PROJECT_ID, BIS_PROJECT.ORG_CD, substr(BIS_FACT2.FACT_DATE, 0, 4),
                      substr(BIS_FACT2.FACT_DATE, 6, 2)
         ) table1
    group by table1.BIS_PROJECT_ID, table1.ORG_CD, table1.fact_date
) t2 on t2.BIS_PROJECT_ID = t1.BIS_PROJECT_ID and t1.budget_year = t2.fact_year;


-- 物业费达成率
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
select t1.project_name,                                                           -- 项目名称
       t1.bis_project_id,                                                         -- 项目id
       '9' as business_type,                                                      -- 审批类型（(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴率,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更）
       '4' as scondition_type,                                                    -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       string(t1.budget_year),                                                    -- 审批完成时间（年）
       if(t1.budget_money_01 = 0, 0, round(t2.money_01 / t1.budget_money_01, 2)), -- 一月物业费达成率
       if(t1.budget_money_02 = 0, 0, round(t2.money_2 / t1.budget_money_02, 2)),
       if(t1.budget_money_03 = 0, 0, round(t2.money_3 / t1.budget_money_03, 2)),
       if(t1.budget_money_04 = 0, 0, round(t2.money_4 / t1.budget_money_04, 2)),
       if(t1.budget_money_05 = 0, 0, round(t2.money_5 / t1.budget_money_05, 2)),
       if(t1.budget_money_06 = 0, 0, round(t2.money_6 / t1.budget_money_06, 2)),
       if(t1.budget_money_07 = 0, 0, round(t2.money_7 / t1.budget_money_07, 2)),
       if(t1.budget_money_08 = 0, 0, round(t2.money_8 / t1.budget_money_08, 2)),
       if(t1.budget_money_09 = 0, 0, round(t2.money_9 / t1.budget_money_09, 2)),
       if(t1.budget_money_10 = 0, 0, round(t2.money_10 / t1.budget_money_10, 2)),
       if(t1.budget_money_11 = 0, 0, round(t2.money_11 / t1.budget_money_11, 2)),
       if(t1.budget_money_12 = 0, 0, round(t2.money_12 / t1.budget_money_12, 2))
from (
         SELECT p.ORG_CD,
                p.BIS_PROJECT_ID,
                p.project_name,
                if(r.fee_type = '1', '租金', '物业费')      budget_type,     -- 预算类别
                max(r.YEAR) as                         budget_year,
                sum(if(month = 1, r.budget_money, 0))  budget_money_01, -- 一月物管预算
                sum(if(month = 2, r.budget_money, 0))  budget_money_02,
                sum(if(month = 3, r.budget_money, 0))  budget_money_03,
                sum(if(month = 4, r.budget_money, 0))  budget_money_04,
                sum(if(month = 5, r.budget_money, 0))  budget_money_05,
                sum(if(month = 6, r.budget_money, 0))  budget_money_06,
                sum(if(month = 7, r.budget_money, 0))  budget_money_07,
                sum(if(month = 8, r.budget_money, 0))  budget_money_08,
                sum(if(month = 9, r.budget_money, 0))  budget_money_09,
                sum(if(month = 10, r.budget_money, 0)) budget_money_10,
                sum(if(month = 11, r.budget_money, 0)) budget_money_11,
                sum(if(month = 12, r.budget_money, 0)) budget_money_12,
                sum(r.budget_money)
         FROM (
                  SELECT bis_project_id,
                         year,
                         month,
                         nvl(budget_income, 0)            as budget_money,
                         substr(string(sequnce_no), 1, 1) as fee_type
                  FROM ods.ods_pl_powerdes_bis_report_rent_quick_premoney_dt BIS_REPORT_RENT_QUICK_PREMONEY
                  WHERE sequnce_no in (111, 112, 113, 120, 130, 211, 212, 213, 220, 230)
                    -- AND year = '2019'
                    and substr(string(sequnce_no), 1, 1) = '2'
              ) r
                  LEFT JOIN
              ods.ods_pl_powerdes_bis_project_dt p ON r.bis_project_id = p.bis_project_id
         where p.oper_status = '1'
         GROUP BY p.BIS_PROJECT_ID, p.project_name, ORG_CD, r.YEAR, r.fee_type
     ) t1
         left join (
    select table1.ORG_CD,
           table1.BIS_PROJECT_ID,
           nvl(max(table1.fact_type), 0) as fact_type, -- 1:物管实收
           nvl(max(table1.fact_date), 0) as fact_year, -- 实收日期（年）
           nvl(MAX(table1.money_01), 0)  AS money_01,  -- 一月物管实收
           nvl(MAX(table1.money_2), 0)   AS money_2,
           nvl(MAX(table1.money_3), 0)   AS money_3,
           nvl(MAX(table1.money_4), 0)   AS money_4,
           nvl(MAX(table1.money_5), 0)   AS money_5,
           nvl(MAX(table1.money_6), 0)   AS money_6,
           nvl(MAX(table1.money_7), 0)   AS money_7,
           nvl(MAX(table1.money_8), 0)   AS money_8,
           nvl(MAX(table1.money_9), 0)   AS money_9,
           nvl(MAX(table1.money_10), 0)  AS money_10,
           nvl(MAX(table1.money_11), 0)  AS money_11,
           nvl(MAX(table1.money_12), 0)  AS money_12
    from (
             select bis_project.ORG_CD,
                    BIS_FACT2.BIS_PROJECT_ID,
                    max(BIS_FACT2.FACT_TYPE)                                                         as fact_type, -- 实收类型
                    substr(BIS_FACT2.FACT_DATE, 0, 4)                                                   fact_date, -- 实收日期(年)
                    case
                        when substr(BIS_FACT2.FACT_DATE, 6, 2) = '01' then sum(MONEY)
                        else null end                                                                as money_01,  -- 一月实收
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '02' then sum(MONEY) else null end as money_2,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '03' then sum(MONEY) else null end as money_3,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '04' then sum(MONEY) else null end as money_4,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '05' then sum(MONEY) else null end as money_5,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '06' then sum(MONEY) else null end as money_6,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '07' then sum(MONEY) else null end as money_7,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '08' then sum(MONEY) else null end as money_8,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '09' then sum(MONEY) else null end as money_9,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '10' then sum(MONEY) else null end as money_10,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '11' then sum(MONEY) else null end as money_11,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '12' then sum(MONEY) else null end as money_12
             from ods.ods_pl_powerdes_bis_fact2_dt BIS_FACT2
                      left join ods.ods_pl_powerdes_bis_project_dt bis_project
                                on BIS_FACT2.BIS_PROJECT_ID = BIS_PROJECT.BIS_PROJECT_ID
             where BIS_FACT2.FACT_TYPE = '2'
               and IS_DELETE = '1'
               and bis_project.oper_status = '1' and is_business_project = '1'
               -- and BIS_FACT2.BIS_PROJECT_ID = '40282b8927a42dff0127a433647400aa'
               -- and to_char(FACT_DATE, 'YYYY') = '2021'
             group by BIS_FACT2.BIS_PROJECT_ID, BIS_PROJECT.ORG_CD, substr(BIS_FACT2.FACT_DATE, 0, 4),
                      substr(BIS_FACT2.FACT_DATE, 6, 2)
         ) table1
    group by table1.BIS_PROJECT_ID, table1.ORG_CD, table1.fact_date
) t2 on t1.BIS_PROJECT_ID = t2.BIS_PROJECT_ID and t1.budget_year = t2.fact_year;


-- 租费达成率
insert into table dws.dws_bis_attract_investment_deatil_big_dt partition (dt = '${hiveconf:nowdate}')
select t1.project_name,                                                           -- 项目名称
       t1.bis_project_id,                                                         -- 项目id
       '10' as business_type,                                                     -- 审批类型（(1：意向,2：商务,3：合同网批,4：合同签署,5：进场,6：开业,7：租金收缴率,8：租金达成率,9：物业费达成率,10：租费达成率,11：特商比例,12：品牌达成率,13：业态偏差率,14：网批驳回率,15：拆合铺,16：铺位信息变更）
       '4'  as scondition_type,                                                   -- '搜索条件类型（1：品牌 2：建筑面积 3：计租面积 4：非 1、2、3）'
       string(t1.budget_year),                                                    -- 审批完成时间（年）
       if(t1.budget_money_01 = 0, 0, round(t2.money_01 / t1.budget_money_01, 2)), -- 一月租费达成率
       if(t1.budget_money_02 = 0, 0, round(t2.money_2 / t1.budget_money_02, 2)),
       if(t1.budget_money_03 = 0, 0, round(t2.money_3 / t1.budget_money_03, 2)),
       if(t1.budget_money_04 = 0, 0, round(t2.money_4 / t1.budget_money_04, 2)),
       if(t1.budget_money_05 = 0, 0, round(t2.money_5 / t1.budget_money_05, 2)),
       if(t1.budget_money_06 = 0, 0, round(t2.money_6 / t1.budget_money_06, 2)),
       if(t1.budget_money_07 = 0, 0, round(t2.money_7 / t1.budget_money_07, 2)),
       if(t1.budget_money_08 = 0, 0, round(t2.money_8 / t1.budget_money_08, 2)),
       if(t1.budget_money_09 = 0, 0, round(t2.money_9 / t1.budget_money_09, 2)),
       if(t1.budget_money_10 = 0, 0, round(t2.money_10 / t1.budget_money_10, 2)),
       if(t1.budget_money_11 = 0, 0, round(t2.money_11 / t1.budget_money_11, 2)),
       if(t1.budget_money_12 = 0, 0, round(t2.money_12 / t1.budget_money_12, 2))
from (
         SELECT p.ORG_CD,
                p.BIS_PROJECT_ID,
                p.project_name,
                max(r.YEAR) as                         budget_year,
                sum(if(month = 1, r.budget_money, 0))  budget_money_01, -- 一月（租金 + 物管 ）预算
                sum(if(month = 2, r.budget_money, 0))  budget_money_02,
                sum(if(month = 3, r.budget_money, 0))  budget_money_03,
                sum(if(month = 4, r.budget_money, 0))  budget_money_04,
                sum(if(month = 5, r.budget_money, 0))  budget_money_05,
                sum(if(month = 6, r.budget_money, 0))  budget_money_06,
                sum(if(month = 7, r.budget_money, 0))  budget_money_07,
                sum(if(month = 8, r.budget_money, 0))  budget_money_08,
                sum(if(month = 9, r.budget_money, 0))  budget_money_09,
                sum(if(month = 10, r.budget_money, 0)) budget_money_10,
                sum(if(month = 11, r.budget_money, 0)) budget_money_11,
                sum(if(month = 12, r.budget_money, 0)) budget_money_12,
                sum(r.budget_money)
         FROM (
                  SELECT bis_project_id,
                         year,
                         month,
                         nvl(sum(budget_income), 0) as budget_money
                  FROM ods.ods_pl_powerdes_bis_report_rent_quick_premoney_dt BIS_REPORT_RENT_QUICK_PREMONEY
                  WHERE sequnce_no in (111, 112, 113, 120, 130, 211, 212, 213, 220, 230)
                    -- AND year = '2019' and BIS_PROJECT_ID = '40282b8927a42dff0127a435d5c30126'
                    and substr(string(sequnce_no), 1, 1) in ('1', '2')
                  group by BIS_PROJECT_ID, year, month
              ) r
                  LEFT JOIN
              ods.ods_pl_powerdes_bis_project_dt p ON r.bis_project_id = p.bis_project_id
         where p.oper_status = '1'
         GROUP BY p.BIS_PROJECT_ID, p.project_name, ORG_CD, r.YEAR
     ) t1
         left join (
    select table1.ORG_CD,
           table1.BIS_PROJECT_ID,
           nvl(max(table1.fact_date), 0) as fact_year, -- 实收日期（年）
           nvl(MAX(table1.money_01), 0)  AS money_01,  -- 一月（租金+物管）实收
           nvl(MAX(table1.money_2), 0)   AS money_2,
           nvl(MAX(table1.money_3), 0)   AS money_3,
           nvl(MAX(table1.money_4), 0)   AS money_4,
           nvl(MAX(table1.money_5), 0)   AS money_5,
           nvl(MAX(table1.money_6), 0)   AS money_6,
           nvl(MAX(table1.money_7), 0)   AS money_7,
           nvl(MAX(table1.money_8), 0)   AS money_8,
           nvl(MAX(table1.money_9), 0)   AS money_9,
           nvl(MAX(table1.money_10), 0)  AS money_10,
           nvl(MAX(table1.money_11), 0)  AS money_11,
           nvl(MAX(table1.money_12), 0)  AS money_12
    from (
             select bis_project.ORG_CD,
                    BIS_FACT2.BIS_PROJECT_ID,
                    substr(BIS_FACT2.FACT_DATE, 0, 4)                                                   fact_date, -- 实收日期(年)
                    case
                        when substr(BIS_FACT2.FACT_DATE, 6, 2) = '01' then sum(MONEY)
                        else null end                                                                as money_01,  -- 一月(租金+物管）实收
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '02' then sum(MONEY) else null end as money_2,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '03' then sum(MONEY) else null end as money_3,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '04' then sum(MONEY) else null end as money_4,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '05' then sum(MONEY) else null end as money_5,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '06' then sum(MONEY) else null end as money_6,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '07' then sum(MONEY) else null end as money_7,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '08' then sum(MONEY) else null end as money_8,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '09' then sum(MONEY) else null end as money_9,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '10' then sum(MONEY) else null end as money_10,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '11' then sum(MONEY) else null end as money_11,
                    case when substr(BIS_FACT2.FACT_DATE, 6, 2) = '12' then sum(MONEY) else null end as money_12
             from ods.ods_pl_powerdes_bis_fact2_dt BIS_FACT2
                      left join ods.ods_pl_powerdes_bis_project_dt bis_project
                                on BIS_FACT2.BIS_PROJECT_ID = BIS_PROJECT.BIS_PROJECT_ID
             where FACT_TYPE in ('1', '2')
               and IS_DELETE = '1'
               and bis_project.oper_status = '1' and is_business_project = '1'
               -- and BIS_FACT2.BIS_PROJECT_ID = '40282b8927a42dff0127a433647400aa'
               -- and to_char(FACT_DATE, 'YYYY') = '2021'
             group by BIS_FACT2.BIS_PROJECT_ID, BIS_PROJECT.ORG_CD, substr(BIS_FACT2.FACT_DATE, 0, 4),
                      substr(BIS_FACT2.FACT_DATE, 6, 2)
         ) table1
    group by table1.BIS_PROJECT_ID, table1.ORG_CD, table1.fact_date
) t2 on t2.BIS_PROJECT_ID = t1.BIS_PROJECT_ID and t1.budget_year = t2.fact_year;
