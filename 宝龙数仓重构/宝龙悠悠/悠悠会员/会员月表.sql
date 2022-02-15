select t5.project_id,         -- 项目id
       t5.year_month1,        -- 月份
       t1.member_count,       -- 月注册会员数,
       t2.index,              -- 会员拓卡指标
       t3.index,              -- 会员消费占比指标
       t4.pay_price,          -- 月消费金额
       t1.is_man_merber,      -- 是否是主会员
       t6.active_member_num,  -- 活跃会员数
       t4.consume_member_num, -- 消费会员数
       current_date           -- ETL时间
from (
         select distinct trim(bis_project.id) project_id,
                         bis_project.name,
                         year,
                         year_month1
         from dim.dim_pl_date,
              ods.ods_member_bl_basic_project_dt bis_project
         where year between '2010' and '2030'
     ) t5
         left join
     (
         select trim(t2.project_id)               project_id,     -- 项目id
                substring(mp.register_time, 0, 7) register_month, -- 会员注册月份
                count(*)                          member_count,   -- 月注册会员数
                is_man_merber
         from ods.ods_pl_bl_member_level_dt t2
                  LEFT JOIN ods.ods_pl_bl_member_profile_dt mp
                            ON mp.id = t2.uid -- and trim(t2.project_id) = trim(t2.project_id)
         where t2.is_member = '1'
         group by trim(t2.project_id), substring(mp.register_time, 0, 7), t2.is_man_merber
     ) t1 on t1.project_id = t5.project_id and t1.register_month = t5.year_month1
         left join ods.ods_pl_data_center_index_member_dt t2
                   on t5.project_id = trim(t2.bis_project_id) and t5.year_month1 = t2.query_date and
                      t2.query_type = '2' and t2.is_del = '0'
         left join ods.ods_pl_data_center_index_member_dt t3
                   on t5.project_id = trim(t3.bis_project_id) and t5.year = t3.query_year and
                      t3.query_type = '1' and t3.is_del = '0'
         left join

     (
         select trim(project_id)             project_id,
                substring(create_time, 0, 7) create_month,
                sum(pay_price)               pay_price,
                count(*)                     consume_member_num -- 消费会员数

         from ods.ods_pl_bl_order_base_dt
         group by trim(project_id), substring(create_time, 0, 7)
     ) t4 on t5.project_id = t4.project_id and t5.year_month1 = t4.create_month
         left join
     (
         select trim(t1.project_id)          project_id,       -- 项目id
                substr(t1.create_time, 0, 7) year_month,       -- 月份,
                count(distinct uid)          active_member_num -- 活跃会员数

         from (
                  select project_id, create_time, uid
                  from ods.ods_pl_bl_member_points_log_dt t1
                  where points_type in
                        (1, 3, 5, 6, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 22, 24, 25, 32, 33, 34, 35, 39, 47, 50)
              ) t1
         group by trim(t1.project_id), substr(t1.create_time, 0, 7)
     ) t6 on t5.project_id = t6.project_id and t5.year_month1 = t6.year_month;



