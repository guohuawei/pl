-- 实收
select distinct bis_project_id,
                store_type,
                qz_year_month,
                fact_type,
                round(sum(round(fact_money, 2))
                          over (partition by bis_project_id, store_type, qz_year_month,substr(qz_year_month, 0, 4), fact_type),
                      2) fact_money,
                round(sum(round(fact_money, 2))
                          over (partition by bis_project_id, substr(qz_year_month, 0, 4),store_type, fact_type order by bis_project_id,qz_year_month,store_type,fact_type),
                      2) total_qz_month_fact_money, -- 权责月月累计（月递归累计）实收金额
                round(sum(round(fact_money, 2))
                          over (partition by bis_project_id, substr(qz_year_month, 0, 4),store_type, fact_type order by bis_project_id, substr(qz_year_month, 0, 4),store_type, fact_type),
                      2) total_qz_year_fact_money   -- 权责月年累计实收金额
from dwd.dwd_bis_fact_basic_big_dt bis_fact_basic
where bis_fact_basic.fact_type in ('1', '2')
  and bis_fact_basic.dt = date_format(current_date, 'YYYYMMdd')
  and fact_date <= last_day(concat(qz_year_month, '-01'))-- 权责月最后一天
  and qz_year_month like '2021%'
  and bis_project_id = '6D7E1C7AFAFB43E986670A81CF441231';


-- 合同应收
select bis_project_id,
       must_year_month,
       sum(must_money),
       sum(total_cont_month_must_money),
       sum(total_cont_year_must_money)

from (
         select bis_project_id,
                substr(must_date, 0, 7)   must_year_month,
                store_type,
                must_type,
                round(sum(must_money), 2) must_money,
                round(sum(round(sum(must_money), 2))
                          over (partition by bis_project_id, substr(must_date, 0, 4),store_type, must_type order by bis_project_id,substr(must_date, 0, 7),store_type,must_type),
                      2)                  total_cont_month_must_money, -- 合同口径（应收月）月累计（月递归累计）应收金额
                round(sum(round(sum(must_money), 2))
                          over (partition by bis_project_id, substr(must_date, 0, 4),store_type, must_type order by bis_project_id, substr(must_date, 0, 4),store_type, must_type),
                      2)                  total_cont_year_must_money   -- 合同口径（应收月）年累计应收金额
         from dwd.dwd_bis_must_cont_basic_big_dt bis_must_basic
         where bis_must_basic.must_type in ('1', '2')
           and bis_must_basic.dt = date_format(current_date, 'YYYYMMdd')
           and bis_must_basic.bis_project_id = '6D7E1C7AFAFB43E986670A81CF444253'
           and substr(must_date, 0, 4) = '2021'
         group by bis_project_id, store_type, must_type, substr(must_date, 0, 7), substr(must_date, 0, 4)
     ) t
where t.must_type = '1'
group by t.bis_project_id, t.must_year_month;


-- 权责应收
select bis_project_id,
       qz_year_month,
       must_type,
       round(sum(must_money), 2) must_money
from dwd.dwd_bis_must_qz_basic_big_dt bis_must_basic
where bis_must_basic.must_type in ('1', '2')
  and bis_must_basic.dt = date_format(current_date, 'YYYYMMdd')
  and BIS_PROJECT_ID = '6D7E1C7AFAFB43E986670A81CF444253'
  and bis_must_basic.QZ_YEAR_MONTH like '2021%'
group by bis_project_id, qz_year_month, substr(qz_year_month, 0, 4), must_type;


-- 权责应收
select must.bis_project_id,
       must.qz_year_month,
       must.MUST_TYPE,
       sum(mustMoney),
       sum(nvl(adjMoney, 0)),
       sum(mustMoney) + sum(nvl(adjMoney, 0)) as mustMoney
from (
         -- 应收
         SELECT a.bis_project_id,
                a.bis_cont_id,
                a.qz_year_month,
                a.MUST_TYPE,
                sum(a.money) as mustMoney
         FROM ods.ods_pl_powerdes_bis_must2_dt a
         where a.must_type in ('1', '2')
           and a.is_show = 1
           and a.is_delete = 0
         group by a.bis_project_id, a.bis_cont_id, a.qz_year_month, a.MUST_TYPE
     ) must
         left join
     (
         -- 减免
         SELECT b.bis_cont_id,
                b.qz_year_month,
                b.FEE_TYPE,
                sum(b.adjust_money) as adjMoney
         FROM ods.ods_pl_powerdes_bis_mf_adjust_dt b
         where b.is_del = 1
           and b.fee_type in ('1', '2')
         group by b.bis_cont_id, b.qz_year_month, b.FEE_TYPE
     ) adj
     on must.bis_cont_id = adj.bis_cont_id and must.qz_year_month = adj.qz_year_month and
        must.MUST_TYPE = adj.FEE_TYPE
         left join
     (
         -- 实收
         SELECT c.bis_cont_id,
                c.qz_year_month,
                c.FACT_TYPE,
                sum(c.money) as factMoney
         FROM ods.ods_pl_powerdes_bis_fact2_dt c
         where c.is_delete = 1
           and c.fact_type in ('1', '2')
         group by c.bis_cont_id, c.qz_year_month, c.FACT_TYPE
     ) fact
     on must.bis_cont_id = fact.bis_cont_id and must.qz_year_month = fact.qz_year_month and
        must.MUST_TYPE = fact.FACT_TYPE
where BIS_PROJECT_ID = '6D7E1C7AFAFB43E986670A81CF444253'
  and must.QZ_YEAR_MONTH like '2021%'
  and must_type = '1'
group by must.bis_project_id, must.qz_year_month, must.MUST_TYPE;


-- 应收费用表
select t.bis_project_id, t.qz_year_month, t.must_type, sum(must_money) + sum(ADJUST_MONEY)
from (
         select t.bis_project_id,
                t.bis_cont_id,
                t.qz_year_month,
                t.must_type,
                t.fact_money,
                t.must_money,
                t.billing_period_begin,
                t.billing_period_end,
                t.must_date,
                t.rent_last_pay_date,
                t1.ADJUST_MONEY
         from (
                  select bis_must2.bis_project_id,
                         bis_must2.bis_cont_id,
                         bis_must2.qz_year_month,
                         bis_must2.must_type,
                         max(bis_must2.fact_money)           fact_money,
                         sum(bis_must2.money)                must_money,
                         max(bis_must2.billing_period_begin) billing_period_begin,
                         max(bis_must2.billing_period_end)   billing_period_end,
                         max(bis_must2.MUST_YEAR_MONTH)      must_date,
                         max(bis_must2.rent_last_pay_date)   rent_last_pay_date

                  from ods.ods_pl_powerdes_bis_must2_dt bis_must2
                  where (bis_must2.is_delete = '0' or bis_must2.is_delete is null)
                    and must_type is not null
                    and is_show = '1'
                  group by bis_must2.bis_project_id, bis_must2.bis_cont_id,
                           bis_must2.qz_year_month, bis_must2.must_type
              ) t
                  -- 应收关联减免：项目 合同 权责月 费项
                  left join
              (
                  select BIS_CONT_ID,
                         QZ_YEAR_MONTH,
                         FEE_TYPE,
                         sum(ADJUST_MONEY) ADJUST_MONEY
                  from ods.ods_pl_powerdes_bis_mf_adjust_dt bis_mf_adjust
                  where is_del = 1
                  group by BIS_CONT_ID, QZ_YEAR_MONTH, FEE_TYPE
              ) t1
              on t.bis_cont_id = t1.bis_cont_id
                  and t.qz_year_month = t1.qz_year_month
                  and t.must_type = t1.fee_type
     ) t
where BIS_PROJECT_ID = '6D7E1C7AFAFB43E986670A81CF444253'
  and QZ_YEAR_MONTH like '2021%'
  and must_type = '1'
group by t.bis_project_id, t.qz_year_month, t.must_type;



select distinct p.bis_project_id, -- 项目id
                p.short_name,     --项目名,
                a.cont_no,        --合同号,
                a.contract_no,    --合同文本号,
                a.attach_num,     --上传合同签署件数,
                a.created_date    --上传合同签署时间,
from (
         select t1.bis_cont_id,
                t1.bis_store_ids,
                t1.bis_store_nos,
                t1.bis_project_id,
                t1.contract_templet_info_id2,
                t2.contract_templet_info_id,
                t1.cont_no,
                t1.status_cd,
                t2.contract_no,
                t1.effect_flg,
                t1.cont_start_date,
                t1.cont_end_date,
                t1.cont_to_fail_date,
                t1.is_create_rent,
                t1.management_forms,
                b.attach_num,
                b.created_date,
                t1.cont_type_cd
         from ods.ods_pl_powerdes_bis_cont_dt t1
                  left join ods.ods_pl_powerdes_sc_contract_templet_info_dt t2
                            on t1.contract_templet_info_id2 = t2.contract_templet_info_id
                  left join
              (
                  select count(*)             attach_num,
                         t3.contract_templet_info_id,
                         t3.attach_type_cd,
                         max(t3.created_date) created_date
                  from ods.ods_pl_powerdes_sc_contract_info_attach_dt t3
                  where t3.attach_type_cd = '990'
                  group by t3.contract_templet_info_id, t3.attach_type_cd
              ) b
              on t2.contract_templet_info_id = b.contract_templet_info_id
         where t1.cont_type_cd in ('1', '2', '3')
           and t1.status_cd in ('0', '1', '2', '3', '5')
     ) a
         left join ods.ods_pl_powerdes_bis_store_dt bis_store
                   on bis_store.bis_store_id =
                      if(INSTR(a.bis_store_ids, ',') > 0, substr(a.bis_store_ids, 0, INSTR(a.bis_store_ids, ',') - 1),
                         a.bis_store_ids)
         left join ods.ods_pl_powerdes_bis_project_dt p on p.bis_project_id = a.bis_project_id
where a.cont_no is not null
  and p.bis_project_id = 'C0689QWERTY24123480EBEB52D6E002X'
order by p.short_name, bis_store.store_no;


-- 双签合同
select distinct p.bis_project_id,                                                     -- 项目id
                p.short_name,                                                         -- 项目名,
                a.cont_no,                                                            -- 合同号,
                a.bis_cont_id,                                                        -- 合同id
                a.contract_no,                                                        -- 合同文本号,
                a.attach_num,                                                         -- 上传合同签署件数,
                a.created_date,                                                       -- 上传合同签署时间,
                case a.cont_type_cd
                    when '1' then '自持'
                    when '1' then '2'
                    when '1' then '销售'
                    when '1' then '3'
                    when '1' then '多经'
                    else null end,                                                    --  合同类型,
                bs.store_no,                                                          --铺位号,
                bs.rent_square,                                                       -- 计租面积,
                -- decode(a.status_cd, 0, '未签约', 1, '已审核', 2, '已解约', 3, '未审核', 4, '无效合同', 5, '待处理'), -- 合同状态,
                --状态（0：未签约；1：已审核；2：已解约；3：未审核;4：无效合同（补充商务条件产生）;5、待处理） case a.effect_flg when 'Y' then '有效' else '无效' end                               --  合同状态,
                a.cont_start_date,                                                    -- 合同开始时间,
                a.cont_end_date,                                                      -- 合同结束时间,
                a.cont_to_fail_date,                                                  -- 合同解约时间,
                --case when (bs.rent_status='1' or bs.bis_store_id is null) then '已租' else '未租' end 租赁状态,
                case bs.is_assess when 'Y' then '考核' when 'N' then '不考核' else '' end, -- 是否考核, --是否考核
                bs.bis_store_id,
                a.bis_store_ids
from (
         select t1.bis_cont_id,
                t1.bis_store_ids,
                t1.bis_store_nos,
                t1.bis_project_id,
                t1.contract_templet_info_id2,
                t2.contract_templet_info_id,
                t1.cont_no,
                t1.status_cd,
                t2.contract_no,
                t1.effect_flg,
                t1.cont_start_date,
                t1.cont_end_date,
                t1.cont_to_fail_date,
                t1.is_create_rent,
                t1.management_forms,
                b.attach_num,
                b.created_date,
                t1.cont_type_cd
         from ods.ods_pl_powerdes_bis_cont_dt t1
                  left join ods.ods_pl_powerdes_sc_contract_templet_info_dt t2
                            on t1.contract_templet_info_id2 = t2.contract_templet_info_id
                  left join
              (
                  select count(*)             attach_num,
                         t3.contract_templet_info_id,
                         t3.attach_type_cd,
                         max(t3.created_date) created_date
                  from ods.ods_pl_powerdes_sc_contract_info_attach_dt t3
                  where t3.attach_type_cd = '990'
                  group by t3.contract_templet_info_id, t3.attach_type_cd
              ) b
              on t2.contract_templet_info_id = b.contract_templet_info_id
         where t1.cont_type_cd in ('1', '2', '3')
           and t1.status_cd in ('0', '1', '2', '3', '5')
         --and t1.EFFECT_FLG = 'Y'
         --and sysdate between t1.cont_start_date and t1.cont_end_date
         --and t1.bis_store_nos is not null
     ) a
         left join ods.ods_pl_powerdes_bis_store_dt bs
                   on bs.bis_store_id =
                      if(INSTR(a.bis_store_ids, ',') > 0, substr(a.bis_store_ids, 0, INSTR(a.bis_store_ids, ',') - 1),
                         a.bis_store_ids)
         left join ods.ods_pl_powerdes_bis_project_dt p on p.bis_project_id = a.bis_project_id
where --a.bis_project_id in ('C0689QWERTY24123480EBEB52D6E002X', '6D7E1C7AFAFB43E986670A81CF132446')
      --((a.contract_no is not null and a.attach_num > 0) or a.contract_no is null)
      --and bs.status_cd = '1'
      a.cont_no is not null
order by p.short_name, bs.store_no;


-- 合同欠费
select t.bis_project_id,                                                                                  -- 项目id
       t.MUST_TYPE,                                                                                       -- 费项（1：租金 2：物管）
       substr(t.rent_last_pay_date, 0, 7)                                                must_year_month, -- 应收月
       sum(nvl(t.mustMoney, 0)) + sum(nvl(t1.adjMoney, 0)) - sum(nvl(fact.factMoney, 0)) oweMoney         -- 欠费
from (
         -- 应收
         SELECT BIS_MUST2.bis_project_id,
                BIS_MUST2.bis_cont_id,
                BIS_MUST2.qz_year_month,
                BIS_MUST2.MUST_TYPE,
                BIS_MUST2.billing_period_end,
                BIS_MUST2.billing_period_begin,
                BIS_MUST2.bis_must_id,
                BIS_MUST2.rent_last_pay_date, -- 应收日期
                sum(nvl(BIS_MUST2.money, 0)) as mustMoney
         FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
         where BIS_MUST2.must_type in ('1', '2')
           and BIS_MUST2.is_show = 1
           and BIS_MUST2.is_delete = 0
         group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id, BIS_MUST2.bis_must_id,
                  BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE, BIS_MUST2.billing_period_end,
                  BIS_MUST2.billing_period_begin, BIS_MUST2.rent_last_pay_date
     ) t
         left join
     (
         -- 特殊情况：两条应收除了 应收id和应收日期不一样（权责 ，费项 账期开始、账期结束 都一样），这个时候减免没法知道和哪条应收对应
         -- 处理方法：取这两条应收中应收日期最小的一条应收和减免关联
         select must.bis_must_id, -- 应收id
                adj.adjMoney      -- 减免
         from (
                  -- 应收
                  SELECT BIS_MUST2.bis_project_id,
                         BIS_MUST2.bis_cont_id,
                         BIS_MUST2.qz_year_month,
                         BIS_MUST2.MUST_TYPE,
                         BIS_MUST2.billing_period_end,
                         BIS_MUST2.billing_period_begin,
                         min(BIS_MUST2.rent_last_pay_date) rent_last_pay_date, -- 应收日期
                         min(BIS_MUST2.bis_must_id)   as   bis_must_id,
                         sum(nvl(BIS_MUST2.money, 0)) as   mustMoney
                  FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                  where BIS_MUST2.must_type in ('1', '2')
                    and BIS_MUST2.is_show = 1
                    and BIS_MUST2.is_delete = 0
                  group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                           BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE, BIS_MUST2.billing_period_end,
                           BIS_MUST2.billing_period_begin
              ) must
                  left join
              (
                  -- 减免
                  SELECT bis_mf_adjust.bis_cont_id,
                         bis_mf_adjust.billing_period_end,
                         bis_mf_adjust.billing_period_begin,
                         bis_mf_adjust.qz_year_month,
                         bis_mf_adjust.FEE_TYPE,
                         sum(nvl(bis_mf_adjust.adjust_money, 0)) as adjMoney
                  FROM ods.ods_pl_powerdes_bis_mf_adjust_dt bis_mf_adjust
                  where bis_mf_adjust.is_del = 1
                    and bis_mf_adjust.fee_type in ('1', '2')
                  group by bis_mf_adjust.bis_cont_id,
                           bis_mf_adjust.qz_year_month,
                           bis_mf_adjust.FEE_TYPE,
                           bis_mf_adjust.billing_period_end,
                           bis_mf_adjust.billing_period_begin
              ) adj
                  -- 合同 权责 费项 账期开始  账期结束
              on must.bis_cont_id = adj.bis_cont_id and
                 must.qz_year_month = adj.qz_year_month and
                 must.MUST_TYPE = adj.FEE_TYPE and
                 must.billing_period_end = adj.billing_period_end and
                 must.billing_period_begin = adj.billing_period_begin
     ) t1 on t.bis_must_id = t1.bis_must_id
         left join
     (
         -- 实收
         SELECT bis_fact2.bis_cont_id,
                bis_fact2.bis_must_id,
                bis_fact2.fact_type,
                sum(nvl(bis_fact2.money, 0)) as factMoney
         FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
         where bis_fact2.is_delete = 1
           and bis_fact2.fact_type in ('1', '2')
         group by bis_fact2.bis_cont_id, bis_fact2.bis_must_id, bis_fact2.fact_type
     ) fact
     on t.bis_cont_id = fact.bis_cont_id and
        t.bis_must_id = fact.bis_must_id and
        t.MUST_TYPE = fact.FACT_TYPE
where bis_project_id = '6D7E1C7AFAFB43E986670A81CF444253'
  and substr(t.rent_last_pay_date, 0, 4) = '2021'
group by t.bis_project_id, substr(t.rent_last_pay_date, 0, 7), t.MUST_TYPE;



select BIS_CONT.bis_project_id,
       BIS_CONT.bis_cont_id,
       count(bis_cont_id)
from ods.ods_pl_powerdes_bis_cont_dt bis_cont
         left join ods.ods_pl_powerdes_bis_project_dt bis_project
                   on BIS_CONT.BIS_PROJECT_ID = BIS_PROJECT.BIS_PROJECT_ID
         left join ods.ods_pl_powerdes_bis_shop_dt bis_shop on BIS_CONT.BIS_SHOP_ID = BIS_SHOP.BIS_SHOP_ID
group by BIS_CONT.bis_project_id, BIS_CONT.bis_cont_id
having count(bis_cont_id) > 1;

select *
from (select t.bis_project_id,       -- 项目id
             t.bis_cont_id,          -- 合同id
             t.qz_year_month,        -- 权责月
             t.MUST_TYPE,            -- 费项
             t.billing_period_end,   -- 账期开始
             t.billing_period_begin, -- 账期结束
             t.bis_must_id,          -- 应收id
             t.rent_last_pay_date,   -- 应收日期
             t.mustMoney,            -- 应收金额
             t1.adjMoney             -- 减免金额

      from (
               -- 应收
               SELECT BIS_MUST2.bis_project_id,
                      BIS_MUST2.bis_cont_id,
                      BIS_MUST2.qz_year_month,
                      BIS_MUST2.MUST_TYPE,
                      BIS_MUST2.billing_period_end,
                      BIS_MUST2.billing_period_begin,
                      BIS_MUST2.bis_must_id,
                      BIS_MUST2.rent_last_pay_date, -- 应收日期
                      sum(nvl(BIS_MUST2.money, 0)) as mustMoney
               FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
               where BIS_MUST2.must_type in ('1', '2')
                 and BIS_MUST2.is_show = 1
                 and BIS_MUST2.is_delete = 0
               group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id, BIS_MUST2.bis_must_id,
                        BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE, BIS_MUST2.billing_period_end,
                        BIS_MUST2.billing_period_begin, BIS_MUST2.rent_last_pay_date
           ) t
               left join
           (
               -- 特殊情况：两条应收除了 应收id和应收日期不一样（权责 ，费项 账期开始、账期结束 都一样），这个时候减免没法知道和哪条应收对应
               -- 处理方法：取这两条应收中应收日期最小的一条应收和减免关联
               select must.bis_must_id, -- 应收id
                      adj.adjMoney      -- 减免
               from (
                        SELECT BIS_MUST2.bis_project_id,
                               BIS_MUST2.bis_cont_id,
                               BIS_MUST2.qz_year_month,
                               BIS_MUST2.MUST_TYPE,
                               BIS_MUST2.billing_period_end,
                               BIS_MUST2.billing_period_begin,
                               min(BIS_MUST2.rent_last_pay_date) rent_last_pay_date, -- 应收日期
                               min(BIS_MUST2.bis_must_id)   as   bis_must_id,
                               sum(nvl(BIS_MUST2.money, 0)) as   mustMoney
                        FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                        where BIS_MUST2.must_type in ('1', '2')
                          and BIS_MUST2.is_show = 1
                          and BIS_MUST2.is_delete = 0
                        group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                 BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE, BIS_MUST2.billing_period_end,
                                 BIS_MUST2.billing_period_begin
                    ) must
                        left join
                    (
                        -- 减免
                        SELECT bis_mf_adjust.bis_cont_id,
                               bis_mf_adjust.billing_period_end,
                               bis_mf_adjust.billing_period_begin,
                               bis_mf_adjust.qz_year_month,
                               bis_mf_adjust.FEE_TYPE,
                               sum(nvl(bis_mf_adjust.adjust_money, 0)) as adjMoney
                        FROM ods.ods_pl_powerdes_bis_mf_adjust_dt bis_mf_adjust
                        where bis_mf_adjust.is_del = 1
                          and bis_mf_adjust.fee_type in ('1', '2')
                        group by bis_mf_adjust.bis_cont_id,
                                 bis_mf_adjust.qz_year_month,
                                 bis_mf_adjust.FEE_TYPE,
                                 bis_mf_adjust.billing_period_end,
                                 bis_mf_adjust.billing_period_begin
                    ) adj
                        -- 合同 权责 费项 账期开始  账期结束
                    on must.bis_cont_id = adj.bis_cont_id and
                       must.qz_year_month = adj.qz_year_month and
                       must.MUST_TYPE = adj.FEE_TYPE and
                       must.billing_period_end = adj.billing_period_end and
                       must.billing_period_begin = adj.billing_period_begin
           ) t1 on t.bis_must_id = t1.bis_must_id
     ) t
group by bis_project_id, bis_cont_id;



select bis_project_id,
       substr(must_date, 0, 7)   must_year_month,
       round(sum(must_money), 2) must_money
from dwd.dwd_bis_must_cont_basic_big_dt bis_must_basic
where bis_must_basic.must_type in ('1')
  and bis_must_basic.dt = date_format(current_date, 'YYYYMMdd')
  and bis_must_basic.bis_project_id = '6D7E1C7AFAFB43E986670A81CF444253'
  and substr(must_date, 0, 4) = '2021'
group by bis_project_id, substr(must_date, 0, 7);


select bis_project_id, MUST_DATE, sum(fact_money)
from (
         select bis_project_id,
                store_type,
                substr(must_year_month, 0, 7) MUST_DATE,
                fact_type,
                round(sum(fact_money), 2)     fact_money
         from dwd.dwd_bis_fact_basic_big_dt bis_fact_basic
         where bis_fact_basic.fact_type in ('1', '2')
           and bis_fact_basic.dt = date_format(current_date, 'YYYYMMdd')
           and substr(fact_date, 0, 10) <= last_day(substr(must_year_month, 0, 10)) -- 权责月最后一天
           and bis_fact_basic.bis_project_id = '6D7E1C7AFAFB43E986670A81CF444253'
           and substr(must_year_month, 0, 4) = '2021'
         group by bis_project_id, store_type, substr(must_year_month, 0, 7), fact_type
     ) t
where t.fact_type = '1'
group by t.bis_project_id, t.MUST_DATE;


select bis_project_id,
       query_date,
       sum(total_qz_must_money),
       sum(total_qz_fact_money),
       sum(total_qz_month_fact_money),
       sum(total_qz_year_must_money),
       sum(total_qz_year_fact_money)
from dws.dws_bis_rent_mgr_multi_current_month_big_dt
where bis_project_id = '6D7E1C7AFAFB43E986670A81CF444253'
  and query_date like '2021%'
  and fee_type = '1'
  and dt = date_format(current_date, 'YYYYMMdd')
group by bis_project_id, query_date;


-- 合同口径应收/实收金额
select t4.bis_project_id,                                 -- 项目id
       t4.short_name,                                     -- 项目名称
       t4.year,                                           -- 年
       t4.month,                                          -- 月
       t4.year_month1,                                    -- 应收年月
       t6.must_type,                                      -- 费项（1：租金 2：物管）
       t6.store_type,                                     -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
       t6.mustMoney          must_money_cont,             -- 月应收金额
       t6.month_lj_mustMOney total_cont_month_must_money, -- 合同口径（应收月）月累计（月递归累计）应收金额
       t6.year_lj_mustMoney  total_cont_year_must_money,  -- 合同口径（应收月）年累计应收金额
       t6.factMoney          fact_money_cont,             -- 月实收金额
       t6.month_lj_factMoney total_cont_month_fact_money, -- 合同口径（应收月）月累计（月递归累计）实收金额
       t6.year_lf_factMoney  total_cont_year_fact_money   -- 合同口径（应收月）年累计实收金额

from (
         select distinct bis_project.bis_project_id,
                         bis_project.short_name,
                         year,
                         month,
                         year_month1
         from dim.dim_pl_date,
              ods.ods_pl_powerdes_bis_project_dt bis_project
         where year between '2010' and '2030'
           and bis_project.is_business_project = '1'
           and oper_status = '2'
     ) t4
         left join
     (
         select t2.BIS_PROJECT_ID,                                  -- 项目id
                t2.MUST_TYPE,                                       -- 费项
                t3.STORE_TYPE,                                      -- 物业类型
                t2.must_year_month,                                 -- 应收年月
                sum(nvl(t2.mustMoney, 0))       mustMoney,          -- 月应收
                sum(nvl(t2.factMoney, 0))       factMoney,          -- 月实收
                sum(nvl(month_lj_mustMOney, 0)) month_lj_mustMOney, -- 月累计应收
                sum(nvl(month_lj_factMoney, 0)) month_lj_factMoney, -- 月累计实收
                sum(nvl(year_lj_mustMoney, 0))  year_lj_mustMoney,  -- 年累计应收
                sum(nvl(year_lf_factMoney, 0))  year_lf_factMoney   -- 年累计实收
         from ods.ods_pl_powerdes_bis_cont_dt t3
                  inner join
              (
                  select t.bis_project_id,                                                                                                                                                                                                  -- 项目id
                         t.bis_cont_id,                                                                                                                                                                                                     -- 合同id
                         t.MUST_TYPE,                                                                                                                                                                                                       -- 费项（1：租金 2：物管）
                         substr(t.rent_last_pay_date, 0, 7)                                                                                                                                                             must_year_month,    -- 应收月
                         sum(nvl(t.mustMoney, 0) + nvl(t1.adjMoney, 0))                                                                                                                                                 mustMoney,          -- 月应收
                         sum(nvl(fact.factMoney, 0))                                                                                                                                                                    factMoney,          -- 月实收
                         sum(case
                                 when substr(fact.fact_date, 0, 7) <= substr(t.rent_last_pay_date, 0, 7)
                                     and substr(fact.must_rent_last_pay_date, 0, 4) = substr(t.rent_last_pay_date, 0, 4)
                                     then nvl(fact.factMoney, 0)
                                 else 0 end)                                                                                                                                                                            month_lj_factMoney, -- 月累计实收
                         sum(nvl(sum(case
                                         when substr(fact.fact_date, 0, 7) <= substr(t.rent_last_pay_date, 0, 7)
                                             and
                                              substr(fact.must_rent_last_pay_date, 0, 4) =
                                              substr(t.rent_last_pay_date, 0, 4)
                                             then nvl(fact.factMoney, 0)
                                         else 0 end), 0))
                             over (partition by t.BIS_PROJECT_ID, t.bis_cont_id,t.MUST_TYPE,substr(t.rent_last_pay_date, 0, 4)
                                 order by t.BIS_PROJECT_ID, t.bis_cont_id,t.MUST_TYPE,substr(t.rent_last_pay_date, 0, 7))                                                                                               year_lf_factMoney,  -- 年累计实收


                         sum(sum(nvl(t.mustMoney, 0) + nvl(t1.adjMoney, 0)))
                             over (partition by t.BIS_PROJECT_ID, t.bis_cont_id,t.MUST_TYPE,substr(t.rent_last_pay_date, 0, 4) order by t.BIS_PROJECT_ID, t.bis_cont_id,t.MUST_TYPE,substr(t.rent_last_pay_date, 0, 7)) month_lj_mustMOney, -- 月累计应收
                         sum(sum(nvl(t.mustMoney, 0) + nvl(t1.adjMoney, 0)))
                             over (partition by t.BIS_PROJECT_ID, t.bis_cont_id,t.MUST_TYPE,substr(t.rent_last_pay_date, 0, 4))                                                                                         year_lj_mustMoney   -- 年累计应收

                  from (
                           -- 应收
                           SELECT BIS_MUST2.bis_project_id,
                                  BIS_MUST2.bis_cont_id,
                                  BIS_MUST2.qz_year_month,
                                  BIS_MUST2.MUST_TYPE,
                                  BIS_MUST2.billing_period_end,
                                  BIS_MUST2.billing_period_begin,
                                  BIS_MUST2.bis_must_id,
                                  BIS_MUST2.rent_last_pay_date, -- 应收日期
                                  sum(nvl(BIS_MUST2.money, 0)) as mustMoney
                           FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                           where BIS_MUST2.must_type in ('1', '2')
                             and BIS_MUST2.is_show = 1
                             and BIS_MUST2.is_delete = 0
                           group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id, BIS_MUST2.bis_must_id,
                                    BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE, BIS_MUST2.billing_period_end,
                                    BIS_MUST2.billing_period_begin, BIS_MUST2.rent_last_pay_date
                       ) t
                           left join
                       (
                           select must.bis_must_id,
                                  adj.adjMoney
                           from (
                                    -- 应收
                                    SELECT BIS_MUST2.bis_project_id,
                                           BIS_MUST2.bis_cont_id,
                                           BIS_MUST2.qz_year_month,
                                           BIS_MUST2.MUST_TYPE,
                                           BIS_MUST2.billing_period_end,
                                           BIS_MUST2.billing_period_begin,
                                           min(BIS_MUST2.rent_last_pay_date) rent_last_pay_date, -- 应收日期
                                           min(BIS_MUST2.bis_must_id)   as   bis_must_id,
                                           sum(nvl(BIS_MUST2.money, 0)) as   mustMoney
                                    FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                    where BIS_MUST2.must_type in ('1', '2')
                                      and BIS_MUST2.is_show = 1
                                      and BIS_MUST2.is_delete = 0
                                    group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                             BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE, BIS_MUST2.billing_period_end,
                                             BIS_MUST2.billing_period_begin
                                ) must
                                    left join
                                (
                                    -- 减免
                                    SELECT bis_mf_adjust.bis_cont_id,
                                           bis_mf_adjust.billing_period_end,
                                           bis_mf_adjust.billing_period_begin,
                                           bis_mf_adjust.qz_year_month,
                                           bis_mf_adjust.FEE_TYPE,
                                           sum(nvl(bis_mf_adjust.adjust_money, 0)) as adjMoney
                                    FROM ods.ods_pl_powerdes_bis_mf_adjust_dt bis_mf_adjust
                                    where bis_mf_adjust.is_del = 1
                                      and bis_mf_adjust.fee_type in ('1', '2')
                                    group by bis_mf_adjust.bis_cont_id,
                                             bis_mf_adjust.qz_year_month,
                                             bis_mf_adjust.FEE_TYPE,
                                             bis_mf_adjust.billing_period_end,
                                             bis_mf_adjust.billing_period_begin
                                ) adj
                                    -- 合同 权责 费项 账期开始  账期结束
                                on must.bis_cont_id = adj.bis_cont_id and
                                   must.qz_year_month = adj.qz_year_month and
                                   must.MUST_TYPE = adj.FEE_TYPE and
                                   must.billing_period_end = adj.billing_period_end and
                                   must.billing_period_begin = adj.billing_period_begin
                       ) t1 on t.bis_must_id = t1.bis_must_id
                           left join
                       (
                           -- 实收
                           SELECT bis_fact2.bis_cont_id,
                                  bis_fact2.bis_must_id,
                                  bis_fact2.fact_type,
                                  bis_fact2.fact_date,
                                  bis_fact2.must_rent_last_pay_date,
                                  sum(nvl(bis_fact2.money, 0)) as factMoney
                           FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
                           where bis_fact2.is_delete = 1
                             and bis_fact2.fact_type in ('1', '2')
                             and bis_fact2.bis_must_id is not null
                           group by bis_fact2.bis_cont_id, bis_fact2.bis_must_id, bis_fact2.fact_type,
                                    bis_fact2.fact_date,
                                    bis_fact2.must_rent_last_pay_date
                       ) fact
                       on t.bis_cont_id = fact.bis_cont_id and
                          t.bis_must_id = fact.bis_must_id and
                          t.MUST_TYPE = fact.FACT_TYPE
                  group by t.bis_project_id, t.bis_cont_id, substr(t.rent_last_pay_date, 0, 4),
                           substr(t.rent_last_pay_date, 0, 7), t.MUST_TYPE
              ) t2 on t3.BIS_PROJECT_ID = t2.BIS_PROJECT_ID and t3.BIS_CONT_ID = t2.BIS_CONT_ID
-- where t2.must_year_month in ('2021-01', '2021-02')
--   and t2.BIS_PROJECT_ID = '6D7E1C7AFAFB43E986670A81CF444253'
         group by t2.BIS_PROJECT_ID,
                  t2.MUST_TYPE,
                  t3.STORE_TYPE,
                  t2.must_year_month
     ) t6
     on t4.bis_project_id = t6.bis_project_id and t4.year_month1 = t6.must_year_month;



select sales_date
from ods.ods_pl_powerdes_bis_sales_day_dt
where substr(sales_date, 0, 7) = '20201-03'
order by sales_date desc
limit 10;



select t.bis_project_id,
       t.query_date,
       sum(t.month_lj_mustMOney)


from (
         select t.bis_project_id,
                t.query_date,
                t.fee_type,
                t.store_type,
                sum(t.mustMoney)
                    over (partition by t.bis_project_id,substr(t.query_date, 0, 4) ,t.fee_type,t.store_type order by t.bis_project_id,t.query_date ,t.fee_type,t.store_type) month_lj_mustMOney -- 合同口径月累计应收

         from (
                  select t.bis_project_id,
                         t.query_date,
                         t.fee_type,
                         t.store_type,
                         sum(t.total_cont_must_money) mustMoney -- 合同口径月应收

                  from (
                           select t.bis_project_id,       -- 项目id
                                  t.query_date,           -- 应收月
                                  t.fee_type,
                                  t.store_type,
                                  t.total_cont_must_money -- 月应收
                           from dwd.dwd_bis_rent_mgr_multi_current_month_big_dt t
                           where t.dt = date_format(current_date, 'YYYYMMdd')
                       ) t
                  where t.bis_project_id = '6D7E1C7AFAFB43E986670A81CF444253'
                    and t.fee_type = '1'
                    and substr(t.query_date, 0, 4) = '2021'
                  group by t.bis_project_id, t.query_date, t.fee_type, t.store_type
              ) t
     ) t
group by bis_project_id, query_date;



select t.bis_project_id,
       t.query_year,
       sum(t.mustMoney) year_lj_mustMOney

from (
         select t.bis_project_id,
                t.query_year,
                t.fee_type,
                t.store_type,
                sum(t.total_cont_must_money) mustMoney -- 合同口径月应收

         from (
                  select t.bis_project_id,       -- 项目id
                         t.query_year,
                         t.fee_type,
                         t.store_type,
                         t.total_cont_must_money -- 月应收
                  from dwd.dwd_bis_rent_mgr_multi_current_month_big_dt t
                  where t.dt = date_format(current_date, 'YYYYMMdd')
              ) t
         where t.bis_project_id = '6D7E1C7AFAFB43E986670A81CF444253'
           and t.fee_type = '3'
           and t.query_year = '2021'
         group by t.bis_project_id, t.query_year, t.fee_type, t.store_type
     ) t
group by bis_project_id, query_year;



select t.bis_project_id, t.must_year_month, sum(mustMoney)
from (
         select t.bis_project_id,                                                  -- 项目id
                t.bis_cont_id,                                                     -- 合同id
                t.MUST_TYPE,                                                       -- 费项（1：租金 2：物管）
                substr(t.rent_last_pay_date, 0, 7)             must_year_month,    -- 应收月
                sum(nvl(t.mustMoney, 0) + nvl(t1.adjMoney, 0)) mustMoney,          -- 月应收
                null                                           month_lj_mustMOney, -- 月累计应收
                null                                           year_lj_mustMoney   -- 年累计应收

         from (
                  -- 应收
                  SELECT BIS_MUST2.bis_project_id,
                         BIS_MUST2.bis_cont_id,
                         BIS_MUST2.qz_year_month,
                         BIS_MUST2.MUST_TYPE,
                         BIS_MUST2.billing_period_end,
                         BIS_MUST2.billing_period_begin,
                         BIS_MUST2.bis_must_id,
                         BIS_MUST2.rent_last_pay_date, -- 应收日期
                         sum(nvl(BIS_MUST2.money, 0)) as mustMoney
                  FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                  where BIS_MUST2.must_type in ('34', '57')
                    and BIS_MUST2.is_show = 1
                    and BIS_MUST2.is_delete = 0
                  group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                           BIS_MUST2.bis_must_id,
                           BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                           BIS_MUST2.billing_period_end,
                           BIS_MUST2.billing_period_begin,
                           BIS_MUST2.rent_last_pay_date
              ) t
                  left join
              (
                  select must.bis_must_id,
                         adj.adjMoney
                  from (
                           -- 应收
                           SELECT BIS_MUST2.bis_project_id,
                                  BIS_MUST2.bis_cont_id,
                                  BIS_MUST2.qz_year_month,
                                  BIS_MUST2.MUST_TYPE,
                                  BIS_MUST2.billing_period_end,
                                  BIS_MUST2.billing_period_begin,
                                  min(BIS_MUST2.rent_last_pay_date) rent_last_pay_date, -- 应收日期
                                  min(BIS_MUST2.bis_must_id)   as   bis_must_id,
                                  sum(nvl(BIS_MUST2.money, 0)) as   mustMoney
                           FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                           where BIS_MUST2.must_type in ('34', '57')
                             and BIS_MUST2.is_show = 1
                             and BIS_MUST2.is_delete = 0
                           group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                    BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                                    BIS_MUST2.billing_period_end,
                                    BIS_MUST2.billing_period_begin
                       ) must
                           left join
                       (
                           -- 减免
                           SELECT bis_mf_adjust.bis_cont_id,
                                  bis_mf_adjust.billing_period_end,
                                  bis_mf_adjust.billing_period_begin,
                                  bis_mf_adjust.qz_year_month,
                                  bis_mf_adjust.FEE_TYPE,
                                  sum(nvl(bis_mf_adjust.adjust_money, 0)) as adjMoney
                           FROM ods.ods_pl_powerdes_bis_mf_adjust_dt bis_mf_adjust
                           where bis_mf_adjust.is_del = 1
                             and bis_mf_adjust.fee_type in ('34', '57')
                           group by bis_mf_adjust.bis_cont_id,
                                    bis_mf_adjust.qz_year_month,
                                    bis_mf_adjust.FEE_TYPE,
                                    bis_mf_adjust.billing_period_end,
                                    bis_mf_adjust.billing_period_begin
                       ) adj
                           -- 合同 权责 费项 账期开始  账期结束
                       on must.bis_cont_id = adj.bis_cont_id and
                          must.qz_year_month = adj.qz_year_month and
                          must.MUST_TYPE = adj.FEE_TYPE and
                          must.billing_period_end = adj.billing_period_end and
                          must.billing_period_begin = adj.billing_period_begin
              ) t1 on t.bis_must_id = t1.bis_must_id
         group by t.bis_project_id, t.bis_cont_id,
                  substr(t.rent_last_pay_date, 0, 4),
                  substr(t.rent_last_pay_date, 0, 7), t.MUST_TYPE
     ) t
where t.bis_project_id = '6D7E1C7AFAFB43E986670A81CF444253'
  and substr(t.must_year_month, 0, 4) = '2021'
group by t.bis_project_id, t.must_year_month;



select t.bis_project_id, t.year_month1, sum(must_money_cont), sum(fact_money_cont)
from (
         -- 多经合同口径应收实收
         select t4.bis_project_id,                                 -- 项目id
                t4.short_name,                                     -- 项目名称
                t4.year,                                           -- 年
                t4.month,                                          -- 月
                t4.year_month1,                                    -- 应收年月
                t6.must_type,                                      -- 费项（3：多经）
                t6.store_type,                                     -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                t6.mustMoney          must_money_cont,             -- 月应收金额
                t6.month_lj_mustMOney total_cont_month_must_money, -- 合同口径（应收月）月累计（月递归累计）应收金额
                t6.year_lj_mustMoney  total_cont_year_must_money,  -- 合同口径（应收月）年累计应收金额
                t6.factMoney          fact_money_cont,             -- 月实收金额
                t6.month_lj_factMoney total_cont_month_fact_money, -- 合同口径（应收月）月累计（月递归累计）实收金额
                t6.year_lf_factMoney  total_cont_year_fact_money   -- 合同口径（应收月）年累计实收金额

         from (
                  select distinct bis_project.bis_project_id,
                                  bis_project.short_name,
                                  year,
                                  month,
                                  year_month1
                  from dim.dim_pl_date,
                       ods.ods_pl_powerdes_bis_project_dt bis_project
                  where year between '2010' and '2030'
                    and bis_project.is_business_project = '1'
                    and oper_status = '2'
              ) t4
                  left join
              (
                  select t2.BIS_PROJECT_ID,                                  -- 项目id
                         '3' as                          MUST_TYPE,          -- 费项
                         t3.STORE_TYPE,                                      -- 物业类型
                         t2.must_year_month,                                 -- 应收年月
                         sum(nvl(t2.mustMoney, 0))       mustMoney,          -- 月应收
                         sum(nvl(t2.factMoney, 0))       factMoney,          -- 月实收
                         sum(nvl(month_lj_mustMOney, 0)) month_lj_mustMOney, -- 月累计应收
                         sum(nvl(month_lj_factMoney, 0)) month_lj_factMoney, -- 月累计实收
                         sum(nvl(year_lj_mustMoney, 0))  year_lj_mustMoney,  -- 年累计应收
                         sum(nvl(year_lf_factMoney, 0))  year_lf_factMoney   -- 年累计实收
                  from ods.ods_pl_powerdes_bis_cont_dt t3
                           inner join
                       (
                           select table1.bis_project_id,     -- 项目id
                                  table1.bis_cont_id,        -- 合同id
                                  table1.MUST_TYPE,          -- 费项（'34', '57'）
                                  table1.must_year_month,    -- 应收月
                                  table3.mustMoney,          -- 月应收
                                  table1.factMoney,          -- 月实收
                                  table1.month_lj_factMoney, -- 月累计实收
                                  table1.year_lf_factMoney,  -- 年累计实收
                                  table3.month_lj_mustMOney, -- 月累计应收
                                  table3.year_lj_mustMoney   -- 年累计应收
                           from (
                                    select t.bis_project_id,                                      -- 项目id
                                           t.bis_cont_id,                                         -- 合同id
                                           t.MUST_TYPE,                                           -- 费项（'34', '57'）
                                           substr(t.rent_last_pay_date, 0, 7) must_year_month,    -- 应收月
                                           null                               mustMoney,          -- 月应收
                                           sum(case
                                                   when substr(fact.fact_date, 0, 7) <= substr(t.rent_last_pay_date, 0, 7)
                                                       then nvl(fact.factMoney, 0)
                                                   else 0 end)                factMoney,          -- 月实收
                                           null                               month_lj_factMoney, -- 月累计实收

                                           null                               year_lf_factMoney   -- 年累计实收


                                    from (
                                             -- 应收
                                             SELECT BIS_MUST2.bis_project_id,
                                                    BIS_MUST2.bis_cont_id,
                                                    BIS_MUST2.qz_year_month,
                                                    BIS_MUST2.MUST_TYPE,
                                                    BIS_MUST2.billing_period_end,
                                                    BIS_MUST2.billing_period_begin,
                                                    BIS_MUST2.bis_must_id,
                                                    BIS_MUST2.rent_last_pay_date, -- 应收日期
                                                    sum(nvl(BIS_MUST2.money, 0)) as mustMoney
                                             FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                             where BIS_MUST2.must_type in ('34', '57')
                                               and BIS_MUST2.is_show = 1
                                               and BIS_MUST2.is_delete = 0
                                             group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                                      BIS_MUST2.bis_must_id,
                                                      BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                                                      BIS_MUST2.billing_period_end,
                                                      BIS_MUST2.billing_period_begin, BIS_MUST2.rent_last_pay_date
                                         ) t
                                             left join
                                         (
                                             select must.bis_must_id,
                                                    adj.adjMoney
                                             from (
                                                      -- 应收
                                                      SELECT BIS_MUST2.bis_project_id,
                                                             BIS_MUST2.bis_cont_id,
                                                             BIS_MUST2.qz_year_month,
                                                             BIS_MUST2.MUST_TYPE,
                                                             BIS_MUST2.billing_period_end,
                                                             BIS_MUST2.billing_period_begin,
                                                             min(BIS_MUST2.rent_last_pay_date) rent_last_pay_date, -- 应收日期
                                                             min(BIS_MUST2.bis_must_id)   as   bis_must_id,
                                                             sum(nvl(BIS_MUST2.money, 0)) as   mustMoney
                                                      FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                                      where BIS_MUST2.must_type in ('34', '57')
                                                        and BIS_MUST2.is_show = 1
                                                        and BIS_MUST2.is_delete = 0
                                                      group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                                               BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                                                               BIS_MUST2.billing_period_end,
                                                               BIS_MUST2.billing_period_begin
                                                  ) must
                                                      left join
                                                  (
                                                      -- 减免
                                                      SELECT bis_mf_adjust.bis_cont_id,
                                                             bis_mf_adjust.billing_period_end,
                                                             bis_mf_adjust.billing_period_begin,
                                                             bis_mf_adjust.qz_year_month,
                                                             bis_mf_adjust.FEE_TYPE,
                                                             sum(nvl(bis_mf_adjust.adjust_money, 0)) as adjMoney
                                                      FROM ods.ods_pl_powerdes_bis_mf_adjust_dt bis_mf_adjust
                                                      where bis_mf_adjust.is_del = 1
                                                        and bis_mf_adjust.fee_type in ('34', '57')
                                                      group by bis_mf_adjust.bis_cont_id,
                                                               bis_mf_adjust.qz_year_month,
                                                               bis_mf_adjust.FEE_TYPE,
                                                               bis_mf_adjust.billing_period_end,
                                                               bis_mf_adjust.billing_period_begin
                                                  ) adj
                                                      -- 合同 权责 费项 账期开始  账期结束
                                                  on must.bis_cont_id = adj.bis_cont_id and
                                                     must.qz_year_month = adj.qz_year_month and
                                                     must.MUST_TYPE = adj.FEE_TYPE and
                                                     must.billing_period_end = adj.billing_period_end and
                                                     must.billing_period_begin = adj.billing_period_begin
                                         ) t1 on t.bis_must_id = t1.bis_must_id
                                             left join
                                         (
                                             -- 实收
                                             SELECT bis_fact2.bis_cont_id,
                                                    bis_fact2.bis_must_id,
                                                    bis_fact2.fact_type,
                                                    bis_fact2.fact_date,
                                                    bis_fact2.must_rent_last_pay_date,
                                                    sum(nvl(bis_fact2.money, 0)) as factMoney
                                             FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
                                             where bis_fact2.is_delete = 1
                                               and bis_fact2.fact_type in ('34', '57')
                                               and bis_fact2.bis_must_id is not null
                                             group by bis_fact2.bis_cont_id, bis_fact2.bis_must_id, bis_fact2.fact_type,
                                                      bis_fact2.fact_date,
                                                      bis_fact2.must_rent_last_pay_date
                                         ) fact
                                         on t.bis_cont_id = fact.bis_cont_id and
                                            t.bis_must_id = fact.bis_must_id and
                                            t.MUST_TYPE = fact.FACT_TYPE
                                    group by t.bis_project_id, t.bis_cont_id, substr(t.rent_last_pay_date, 0, 4),
                                             substr(t.rent_last_pay_date, 0, 7), t.MUST_TYPE
                                ) table1
                                    left join
                                (
                                    select t.bis_project_id,                                                  -- 项目id
                                           t.bis_cont_id,                                                     -- 合同id
                                           t.MUST_TYPE,                                                       -- 费项（1：租金 2：物管）
                                           substr(t.rent_last_pay_date, 0, 7)             must_year_month,    -- 应收月
                                           sum(nvl(t.mustMoney, 0) + nvl(t1.adjMoney, 0)) mustMoney,          -- 月应收
                                           null                                           month_lj_mustMOney, -- 月累计应收
                                           null                                           year_lj_mustMoney   -- 年累计应收

                                    from (
                                             -- 应收
                                             SELECT BIS_MUST2.bis_project_id,
                                                    BIS_MUST2.bis_cont_id,
                                                    BIS_MUST2.qz_year_month,
                                                    BIS_MUST2.MUST_TYPE,
                                                    BIS_MUST2.billing_period_end,
                                                    BIS_MUST2.billing_period_begin,
                                                    BIS_MUST2.bis_must_id,
                                                    BIS_MUST2.rent_last_pay_date, -- 应收日期
                                                    sum(nvl(BIS_MUST2.money, 0)) as mustMoney
                                             FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                             where BIS_MUST2.must_type in ('34', '57')
                                               and BIS_MUST2.is_show = 1
                                               and BIS_MUST2.is_delete = 0
                                             group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                                      BIS_MUST2.bis_must_id,
                                                      BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                                                      BIS_MUST2.billing_period_end,
                                                      BIS_MUST2.billing_period_begin,
                                                      BIS_MUST2.rent_last_pay_date
                                         ) t
                                             left join
                                         (
                                             select must.bis_must_id,
                                                    adj.adjMoney
                                             from (
                                                      -- 应收
                                                      SELECT BIS_MUST2.bis_project_id,
                                                             BIS_MUST2.bis_cont_id,
                                                             BIS_MUST2.qz_year_month,
                                                             BIS_MUST2.MUST_TYPE,
                                                             BIS_MUST2.billing_period_end,
                                                             BIS_MUST2.billing_period_begin,
                                                             min(BIS_MUST2.rent_last_pay_date) rent_last_pay_date, -- 应收日期
                                                             min(BIS_MUST2.bis_must_id)   as   bis_must_id,
                                                             sum(nvl(BIS_MUST2.money, 0)) as   mustMoney
                                                      FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                                      where BIS_MUST2.must_type in ('34', '57')
                                                        and BIS_MUST2.is_show = 1
                                                        and BIS_MUST2.is_delete = 0
                                                      group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                                               BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                                                               BIS_MUST2.billing_period_end,
                                                               BIS_MUST2.billing_period_begin
                                                  ) must
                                                      left join
                                                  (
                                                      -- 减免
                                                      SELECT bis_mf_adjust.bis_cont_id,
                                                             bis_mf_adjust.billing_period_end,
                                                             bis_mf_adjust.billing_period_begin,
                                                             bis_mf_adjust.qz_year_month,
                                                             bis_mf_adjust.FEE_TYPE,
                                                             sum(nvl(bis_mf_adjust.adjust_money, 0)) as adjMoney
                                                      FROM ods.ods_pl_powerdes_bis_mf_adjust_dt bis_mf_adjust
                                                      where bis_mf_adjust.is_del = 1
                                                        and bis_mf_adjust.fee_type in ('34', '57')
                                                      group by bis_mf_adjust.bis_cont_id,
                                                               bis_mf_adjust.qz_year_month,
                                                               bis_mf_adjust.FEE_TYPE,
                                                               bis_mf_adjust.billing_period_end,
                                                               bis_mf_adjust.billing_period_begin
                                                  ) adj
                                                      -- 合同 权责 费项 账期开始  账期结束
                                                  on must.bis_cont_id = adj.bis_cont_id and
                                                     must.qz_year_month = adj.qz_year_month and
                                                     must.MUST_TYPE = adj.FEE_TYPE and
                                                     must.billing_period_end = adj.billing_period_end and
                                                     must.billing_period_begin = adj.billing_period_begin
                                         ) t1 on t.bis_must_id = t1.bis_must_id
                                    group by t.bis_project_id, t.bis_cont_id,
                                             substr(t.rent_last_pay_date, 0, 4),
                                             substr(t.rent_last_pay_date, 0, 7), t.MUST_TYPE
                                ) table3
                                on table1.bis_project_id = table3.bis_project_id
                                    and table1.bis_cont_id = table3.bis_cont_id
                                    and table1.must_type = table3.must_type
                                    and table1.must_year_month = table3.must_year_month
                       ) t2 on t3.BIS_PROJECT_ID = t2.BIS_PROJECT_ID and t3.BIS_CONT_ID = t2.BIS_CONT_ID
                  group by t2.BIS_PROJECT_ID,
                           t3.STORE_TYPE,
                           t2.must_year_month
              ) t6
              on t4.bis_project_id = t6.bis_project_id and t4.year_month1 = t6.must_year_month
     ) t
         left join
     (
         -- 多经 物管 租金 预算
         select t.bis_project_id,                                                                  -- 项目id
                t.annual,                                                                          -- 年
                substr(month_budget_money, 0, instr(month_budget_money, '-') - 1) as budget_month, -- 月
                t.charge_type,                                                                     -- 物业类型 （1购物中心2商业街3住宅4住宅底商5写字楼 6住宅公寓7住宅别墅）
                t.store_type,                                                                      -- 1 铺位  2 多径
                substr(month_budget_money, instr(month_budget_money, '-') + 1, 1) as fee_type,     -- 费项 (1:租金 2：物管 3： 多经)
                substr(month_budget_money, instr(month_budget_money, '=') + 1)    as budget_money  -- 租金 物管 多经 每个月的预算金额
         from (
                  SELECT out_mall_id as bis_project_id, -- 项目id
                         lease.annual,                  -- 年
                         lease.charge_type,             -- 物业类型 （1购物中心2商业街3住宅4住宅底商5写字楼 6住宅公寓7住宅别墅）
                         lease.store_type,              -- 1 铺位  2 多径
                         -- if(lease.store_type = '1 ：铺位','1：租金','3：多经')   -- if(lease.store_type = '1 ：铺位','1：物管','无效‘)
                         concat_ws(',',
                                   concat('1', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(jan_zj))),
                                   concat('2', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(feb_zj))),
                                   concat('3', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(march_zj))),
                                   concat('4', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(april_zj))),
                                   concat('5', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(may_zj))),
                                   concat('6', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(june_zj))),
                                   concat('7', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(july_zj))),
                                   concat('8', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(aug_zj))),
                                   concat('9', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(sep_zj))),
                                   concat('10', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(oct_zj))),
                                   concat('11', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(nov_zj))),
                                   concat('12', '-', if(lease.store_type = '1', '1', '3'), '=', string(sum(dece_zj))),
                                   if(string(sum(jan_wg)) is null, null,
                                      concat('1', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(jan_wg)))),
                                   if(string(sum(feb_wg)) is null, null,
                                      concat('2', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(feb_wg)))),
                                   if(string(sum(march_wg)) is null, null,
                                      concat('3', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(march_wg)))),
                                   if(string(sum(april_wg)) is null, null,
                                      concat('4', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(april_wg)))),
                                   if(string(sum(may_wg)) is null, null,
                                      concat('5', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(may_wg)))),
                                   if(string(sum(june_wg)) is null, null,
                                      concat('6', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(june_wg)))),
                                   if(string(sum(july_wg)) is null, null,
                                      concat('7', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(july_wg)))),
                                   if(string(sum(aug_wg)) is null, null,
                                      concat('8', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(aug_wg)))),
                                   if(string(sum(sep_wg)) is null, null,
                                      concat('9', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(sep_wg)))),
                                   if(string(sum(oct_wg)) is null, null,
                                      concat('10', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(oct_wg)))),
                                   if(string(sum(nov_wg)) is null, null,
                                      concat('11', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(nov_wg)))),
                                   if(string(sum(dece_wg)) is null, null,
                                      concat('12', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                             string(sum(dece_wg))))
                             )       as res

                  FROM ods.ods_pl_pms_bis_db_bs_mall_dt m
                           inner join ods.ods_pl_pms_budget_db_budget_instance_dt instance
                                      on m.id = instance.project_id
                           INNER JOIN ods.ods_pl_pms_budget_db_budget_instance_sheet_dt instanceSheet
                                      ON instance.id = instanceSheet.budget_instance_id
                                          AND instance.instance_type = 1
                                          AND instanceSheet.budget_sheet_id in (29, 52)
                           inner join ods.ods_pl_pms_budget_db_budget_instance_biz_lease_dt lease
                                      on instanceSheet.id = lease.budget_instance_sheet_id and
                                         lease.is_del = false and lease.store_type = '2'
                  where m.is_del = '0'
                    and m.stat = '2'
                  group by out_mall_id, lease.annual, lease.charge_type, lease.store_type
              ) t
                  lateral view
                      explode(split(t.res, ",")) t1 as month_budget_money
         where length(month_budget_money) > 0
     ) t3 on t.bis_project_id = t3.bis_project_id and t.year = t3.annual and
             t.month = t3.budget_month and t.must_type = t3.fee_type and
             t.store_type = t3.charge_type
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
     ) t2 on t.bis_project_id = t2.out_mall_id
where t.bis_project_id = '6D7E1C7AFAFB43E986670A81CF444253'
  and t.year = '2021'
group by t.bis_project_id, t.year_month1;



select t.bis_project_id, t.year_month1, sum(must_money_cont), sum(fact_money_cont)
from (
         select t2.area_name,                      -- 区域名称
                t2.id,                             -- 区域id
                t.bis_project_id,                  -- 项目id
                t.short_name,                      -- 项目名称
                t.year,                            -- 年
                t.month,                           -- 月
                t.year_month1,                     -- 年月 2021-12
                t.must_type,                       -- 费项（3：多经）
                t.store_type,                      -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                t.must_money_qz,                   -- 多经权责月应收金额
                t.fact_money_qz,                   -- 多经权责月实收金额
                t1.must_money_cont,                -- 多经每月合同口径应收金额
                t1.fact_money_cont,                -- 多经每月合同口径实收金额
                round(double(t3.budget_money), 2), -- 多经 预算金额
                t.total_qz_month_fact_money,       -- 权责月月累计（月递归累计）实收金额
                t.total_qz_year_fact_money,        -- 权责月年累计实收金额
                t.total_qz_month_must_money,       -- 权责月月累计（月递归累计）应收金额
                t.total_qz_year_must_money,        -- 权责月年累计应收金额
                t1.total_cont_month_fact_money,    -- 合同口径（应收月）月累计（月递归累计）实收金额
                t1.total_cont_year_fact_money,     -- 合同口径（应收月）年累计实收金额
                t1.total_cont_month_must_money,    -- 合同口径（应收月）月累计（月递归累计）应收金额
                t1.total_cont_year_must_money,     -- 合同口径（应收月）年累计应收金额
                CURRENT_TIMESTAMP                  -- ETL时间
         from (
                  -- 多经权责口径--每月应收 和 实收
                  select t.bis_project_id,                                                   -- 项目id
                         t.short_name,                                                       -- 项目名称
                         t.year,                                                             -- 年
                         t.month,                                                            -- 月
                         t.year_month1,                                                      -- 年月 2021-12
                         t.store_type,                                                       -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                         t.must_type,                                                        -- 费项（3：多经）
                         sum(nvl(t.must_money, 0))                must_money_qz,             -- 多经权责月应收金额
                         sum(nvl(t.fact_money, 0))                fact_money_qz,             -- 多经权责月实收金额
                         sum(nvl(t.total_qz_month_fact_money, 0)) total_qz_month_fact_money, -- 权责月月累计（月递归累计）实收金额
                         sum(nvl(t.total_qz_year_fact_money, 0))  total_qz_year_fact_money,  -- 权责月年累计实收金额
                         sum(nvl(t.total_qz_month_must_money, 0)) total_qz_month_must_money, -- 权责月月累计（月递归累计）应收金额
                         sum(nvl(t.total_qz_year_must_money, 0))  total_qz_year_must_money   -- -- 权责月年累计应收金额
                  from (
                           select t1.bis_project_id,                        -- 项目id
                                  t1.short_name,                            -- 项目名称
                                  t1.year,                                  -- 年
                                  t1.month,                                 -- 月
                                  t1.year_month1,                           -- 年月 2021-12
                                  '3' as must_type,                         -- 费项（3：多经）
                                  bis_must_basic.store_type,                -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                                  bis_must_basic.must_money,                -- 应收金额
                                  bis_must_basic.total_qz_month_must_money, -- 权责月月累计（月递归累计）应收金额
                                  bis_must_basic.total_qz_year_must_money,  -- -- 权责月年累计应收金额
                                  bis_fact_basic.fact_money,                -- 实收金额
                                  bis_fact_basic.total_qz_month_fact_money, -- 权责月月累计（月递归累计）实收金额
                                  bis_fact_basic.total_qz_year_fact_money   -- 权责月年累计实收金额


                           from (
                                    select distinct bis_project.bis_project_id,
                                                    bis_project.short_name,
                                                    year,
                                                    month,
                                                    year_month1
                                    from dim.dim_pl_date,
                                         ods.ods_pl_powerdes_bis_project_dt bis_project
                                    where year between '2010' and '2030'
                                      and bis_project.is_business_project = '1'
                                      and oper_status = '2'
                                ) t1
                                    left join
                                (
                                    -- 应收
                                    select bis_project_id,
                                           qz_year_month,
                                           store_type,
                                           must_type,
                                           round(sum(must_money), 2) must_money,
                                           round(sum(round(sum(must_money), 2))
                                                     over (partition by bis_project_id, substr(qz_year_month, 0, 4),store_type, must_type order by bis_project_id,qz_year_month,store_type,must_type),
                                                 2)                  total_qz_month_must_money, -- 权责月月累计（月递归累计）应收金额
                                           round(sum(round(sum(must_money), 2))
                                                     over (partition by bis_project_id, substr(qz_year_month, 0, 4),store_type, must_type order by bis_project_id, substr(qz_year_month, 0, 4),store_type, must_type),
                                                 2)                  total_qz_year_must_money   -- 权责月年累计应收金额
                                    from dwd.dwd_bis_must_qz_basic_big_dt bis_must_basic
                                    where bis_must_basic.must_type in ('34', '57') -- 34：场地使用费 57 ：展览展示费
                                      and bis_must_basic.dt = date_format(current_date, 'YYYYMMdd')
                                    group by bis_project_id, qz_year_month, substr(qz_year_month, 0, 4), store_type,
                                             must_type
                                ) bis_must_basic
                                on t1.bis_project_id = bis_must_basic.bis_project_id
                                    and t1.year_month1 = bis_must_basic.qz_year_month

                                    left join
                                (
                                    -- 实收
                                    select bis_project_id,
                                           store_type,
                                           qz_year_month,
                                           fact_type,
                                           round(sum(fact_money), 2) fact_money,
                                           null                      total_qz_month_fact_money, -- 权责月月累计（月递归累计）实收金额
                                           round(sum(round(sum(fact_money), 2))
                                                     over (partition by bis_project_id, substr(qz_year_month, 0, 4),store_type, fact_type order by bis_project_id, substr(qz_year_month, 0, 4),store_type, fact_type),
                                                 2)                  total_qz_year_fact_money   -- 权责月年累计实收金额
                                    from dwd.dwd_bis_fact_basic_big_dt bis_fact_basic
                                    where bis_fact_basic.fact_type in ('34', '57')                           -- 34：场地使用费 57 ：展览展示费
                                      and bis_fact_basic.dt = date_format(current_date, 'YYYYMMdd')
                                      and substr(fact_date, 0, 10) <= last_day(concat(qz_year_month, '-01')) -- 权责月最后一天
                                    group by bis_project_id, store_type, qz_year_month, substr(qz_year_month, 0, 4),
                                             fact_type
                                ) bis_fact_basic
                                on bis_must_basic.bis_project_id = bis_fact_basic.bis_project_id
                                    and bis_must_basic.qz_year_month = bis_fact_basic.qz_year_month
                                    and bis_must_basic.must_type = bis_fact_basic.fact_type
                                    and bis_must_basic.store_type = bis_fact_basic.store_type
                       ) t
                  group by t.bis_project_id, t.short_name, t.year_month1, t.year, t.month, t.store_type, t.must_type
              ) t
                  left join
              (
                  -- 多经合同口径应收实收
                  select t4.bis_project_id,                                 -- 项目id
                         t4.short_name,                                     -- 项目名称
                         t4.year,                                           -- 年
                         t4.month,                                          -- 月
                         t4.year_month1,                                    -- 应收年月
                         t6.must_type,                                      -- 费项（3：多经）
                         t6.store_type,                                     -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                         t6.mustMoney          must_money_cont,             -- 月应收金额
                         t6.month_lj_mustMOney total_cont_month_must_money, -- 合同口径（应收月）月累计（月递归累计）应收金额
                         t6.year_lj_mustMoney  total_cont_year_must_money,  -- 合同口径（应收月）年累计应收金额
                         t6.factMoney          fact_money_cont,             -- 月实收金额
                         t6.month_lj_factMoney total_cont_month_fact_money, -- 合同口径（应收月）月累计（月递归累计）实收金额
                         t6.year_lf_factMoney  total_cont_year_fact_money   -- 合同口径（应收月）年累计实收金额

                  from (
                           select distinct bis_project.bis_project_id,
                                           bis_project.short_name,
                                           year,
                                           month,
                                           year_month1
                           from dim.dim_pl_date,
                                ods.ods_pl_powerdes_bis_project_dt bis_project
                           where year between '2010' and '2030'
                             and bis_project.is_business_project = '1'
                             and oper_status = '2'
                       ) t4
                           left join
                       (
                           select t2.BIS_PROJECT_ID,                                  -- 项目id
                                  '3' as                          MUST_TYPE,          -- 费项
                                  t3.STORE_TYPE,                                      -- 物业类型
                                  t2.must_year_month,                                 -- 应收年月
                                  sum(nvl(t2.mustMoney, 0))       mustMoney,          -- 月应收
                                  sum(nvl(t2.factMoney, 0))       factMoney,          -- 月实收
                                  sum(nvl(month_lj_mustMOney, 0)) month_lj_mustMOney, -- 月累计应收
                                  sum(nvl(month_lj_factMoney, 0)) month_lj_factMoney, -- 月累计实收
                                  sum(nvl(year_lj_mustMoney, 0))  year_lj_mustMoney,  -- 年累计应收
                                  sum(nvl(year_lf_factMoney, 0))  year_lf_factMoney   -- 年累计实收
                           from ods.ods_pl_powerdes_bis_cont_dt t3
                                    inner join
                                (
                                    select table1.bis_project_id,     -- 项目id
                                           table1.bis_cont_id,        -- 合同id
                                           table1.MUST_TYPE,          -- 费项（'34', '57'）
                                           table1.must_year_month,    -- 应收月
                                           table3.mustMoney,          -- 月应收
                                           table1.factMoney,          -- 月实收
                                           table1.month_lj_factMoney, -- 月累计实收
                                           table1.year_lf_factMoney,  -- 年累计实收
                                           table3.month_lj_mustMOney, -- 月累计应收
                                           table3.year_lj_mustMoney   -- 年累计应收
                                    from (
                                             select t.bis_project_id,                                      -- 项目id
                                                    t.bis_cont_id,                                         -- 合同id
                                                    t.MUST_TYPE,                                           -- 费项（'34', '57'）
                                                    substr(t.rent_last_pay_date, 0, 7) must_year_month,    -- 应收月
                                                    null                               mustMoney,          -- 月应收
                                                    sum(case
                                                            when substr(fact.fact_date, 0, 7) <= substr(t.rent_last_pay_date, 0, 7)
                                                                then nvl(fact.factMoney, 0)
                                                            else 0 end)                factMoney,          -- 月实收
                                                    null                               month_lj_factMoney, -- 月累计实收

                                                    null                               year_lf_factMoney   -- 年累计实收


                                             from (
                                                      -- 应收
                                                      SELECT BIS_MUST2.bis_project_id,
                                                             BIS_MUST2.bis_cont_id,
                                                             BIS_MUST2.qz_year_month,
                                                             BIS_MUST2.MUST_TYPE,
                                                             BIS_MUST2.billing_period_end,
                                                             BIS_MUST2.billing_period_begin,
                                                             BIS_MUST2.bis_must_id,
                                                             BIS_MUST2.rent_last_pay_date, -- 应收日期
                                                             sum(nvl(BIS_MUST2.money, 0)) as mustMoney
                                                      FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                                      where BIS_MUST2.must_type in ('34', '57')
                                                        and BIS_MUST2.is_show = 1
                                                        and BIS_MUST2.is_delete = 0
                                                      group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                                               BIS_MUST2.bis_must_id,
                                                               BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                                                               BIS_MUST2.billing_period_end,
                                                               BIS_MUST2.billing_period_begin,
                                                               BIS_MUST2.rent_last_pay_date
                                                  ) t
                                                      left join
                                                  (
                                                      select must.bis_must_id,
                                                             adj.adjMoney
                                                      from (
                                                               -- 应收
                                                               SELECT BIS_MUST2.bis_project_id,
                                                                      BIS_MUST2.bis_cont_id,
                                                                      BIS_MUST2.qz_year_month,
                                                                      BIS_MUST2.MUST_TYPE,
                                                                      BIS_MUST2.billing_period_end,
                                                                      BIS_MUST2.billing_period_begin,
                                                                      min(BIS_MUST2.rent_last_pay_date) rent_last_pay_date, -- 应收日期
                                                                      min(BIS_MUST2.bis_must_id)   as   bis_must_id,
                                                                      sum(nvl(BIS_MUST2.money, 0)) as   mustMoney
                                                               FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                                               where BIS_MUST2.must_type in ('34', '57')
                                                                 and BIS_MUST2.is_show = 1
                                                                 and BIS_MUST2.is_delete = 0
                                                               group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                                                        BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                                                                        BIS_MUST2.billing_period_end,
                                                                        BIS_MUST2.billing_period_begin
                                                           ) must
                                                               left join
                                                           (
                                                               -- 减免
                                                               SELECT bis_mf_adjust.bis_cont_id,
                                                                      bis_mf_adjust.billing_period_end,
                                                                      bis_mf_adjust.billing_period_begin,
                                                                      bis_mf_adjust.qz_year_month,
                                                                      bis_mf_adjust.FEE_TYPE,
                                                                      sum(nvl(bis_mf_adjust.adjust_money, 0)) as adjMoney
                                                               FROM ods.ods_pl_powerdes_bis_mf_adjust_dt bis_mf_adjust
                                                               where bis_mf_adjust.is_del = 1
                                                                 and bis_mf_adjust.fee_type in ('34', '57')
                                                               group by bis_mf_adjust.bis_cont_id,
                                                                        bis_mf_adjust.qz_year_month,
                                                                        bis_mf_adjust.FEE_TYPE,
                                                                        bis_mf_adjust.billing_period_end,
                                                                        bis_mf_adjust.billing_period_begin
                                                           ) adj
                                                               -- 合同 权责 费项 账期开始  账期结束
                                                           on must.bis_cont_id = adj.bis_cont_id and
                                                              must.qz_year_month = adj.qz_year_month and
                                                              must.MUST_TYPE = adj.FEE_TYPE and
                                                              must.billing_period_end = adj.billing_period_end and
                                                              must.billing_period_begin = adj.billing_period_begin
                                                  ) t1 on t.bis_must_id = t1.bis_must_id
                                                      left join
                                                  (
                                                      -- 实收
                                                      SELECT bis_fact2.bis_cont_id,
                                                             bis_fact2.bis_must_id,
                                                             bis_fact2.fact_type,
                                                             bis_fact2.fact_date,
                                                             bis_fact2.must_rent_last_pay_date,
                                                             sum(nvl(bis_fact2.money, 0)) as factMoney
                                                      FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
                                                      where bis_fact2.is_delete = 1
                                                        and bis_fact2.fact_type in ('34', '57')
                                                        and bis_fact2.bis_must_id is not null
                                                      group by bis_fact2.bis_cont_id, bis_fact2.bis_must_id,
                                                               bis_fact2.fact_type,
                                                               bis_fact2.fact_date,
                                                               bis_fact2.must_rent_last_pay_date
                                                  ) fact
                                                  on t.bis_cont_id = fact.bis_cont_id and
                                                     t.bis_must_id = fact.bis_must_id and
                                                     t.MUST_TYPE = fact.FACT_TYPE
                                             group by t.bis_project_id, t.bis_cont_id,
                                                      substr(t.rent_last_pay_date, 0, 4),
                                                      substr(t.rent_last_pay_date, 0, 7), t.MUST_TYPE
                                         ) table1
                                             left join
                                         (
                                             select t.bis_project_id,                                                  -- 项目id
                                                    t.bis_cont_id,                                                     -- 合同id
                                                    t.MUST_TYPE,                                                       -- 费项（1：租金 2：物管）
                                                    substr(t.rent_last_pay_date, 0, 7)             must_year_month,    -- 应收月
                                                    sum(nvl(t.mustMoney, 0) + nvl(t1.adjMoney, 0)) mustMoney,          -- 月应收
                                                    null                                           month_lj_mustMOney, -- 月累计应收
                                                    null                                           year_lj_mustMoney   -- 年累计应收

                                             from (
                                                      -- 应收
                                                      SELECT BIS_MUST2.bis_project_id,
                                                             BIS_MUST2.bis_cont_id,
                                                             BIS_MUST2.qz_year_month,
                                                             BIS_MUST2.MUST_TYPE,
                                                             BIS_MUST2.billing_period_end,
                                                             BIS_MUST2.billing_period_begin,
                                                             BIS_MUST2.bis_must_id,
                                                             BIS_MUST2.rent_last_pay_date, -- 应收日期
                                                             sum(nvl(BIS_MUST2.money, 0)) as mustMoney
                                                      FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                                      where BIS_MUST2.must_type in ('34', '57')
                                                        and BIS_MUST2.is_show = 1
                                                        and BIS_MUST2.is_delete = 0
                                                      group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                                               BIS_MUST2.bis_must_id,
                                                               BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                                                               BIS_MUST2.billing_period_end,
                                                               BIS_MUST2.billing_period_begin,
                                                               BIS_MUST2.rent_last_pay_date
                                                  ) t
                                                      left join
                                                  (
                                                      select must.bis_must_id,
                                                             adj.adjMoney
                                                      from (
                                                               -- 应收
                                                               SELECT BIS_MUST2.bis_project_id,
                                                                      BIS_MUST2.bis_cont_id,
                                                                      BIS_MUST2.qz_year_month,
                                                                      BIS_MUST2.MUST_TYPE,
                                                                      BIS_MUST2.billing_period_end,
                                                                      BIS_MUST2.billing_period_begin,
                                                                      min(BIS_MUST2.rent_last_pay_date) rent_last_pay_date, -- 应收日期
                                                                      min(BIS_MUST2.bis_must_id)   as   bis_must_id,
                                                                      sum(nvl(BIS_MUST2.money, 0)) as   mustMoney
                                                               FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                                                               where BIS_MUST2.must_type in ('34', '57')
                                                                 and BIS_MUST2.is_show = 1
                                                                 and BIS_MUST2.is_delete = 0
                                                               group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                                                        BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                                                                        BIS_MUST2.billing_period_end,
                                                                        BIS_MUST2.billing_period_begin
                                                           ) must
                                                               left join
                                                           (
                                                               -- 减免
                                                               SELECT bis_mf_adjust.bis_cont_id,
                                                                      bis_mf_adjust.billing_period_end,
                                                                      bis_mf_adjust.billing_period_begin,
                                                                      bis_mf_adjust.qz_year_month,
                                                                      bis_mf_adjust.FEE_TYPE,
                                                                      sum(nvl(bis_mf_adjust.adjust_money, 0)) as adjMoney
                                                               FROM ods.ods_pl_powerdes_bis_mf_adjust_dt bis_mf_adjust
                                                               where bis_mf_adjust.is_del = 1
                                                                 and bis_mf_adjust.fee_type in ('34', '57')
                                                               group by bis_mf_adjust.bis_cont_id,
                                                                        bis_mf_adjust.qz_year_month,
                                                                        bis_mf_adjust.FEE_TYPE,
                                                                        bis_mf_adjust.billing_period_end,
                                                                        bis_mf_adjust.billing_period_begin
                                                           ) adj
                                                               -- 合同 权责 费项 账期开始  账期结束
                                                           on must.bis_cont_id = adj.bis_cont_id and
                                                              must.qz_year_month = adj.qz_year_month and
                                                              must.MUST_TYPE = adj.FEE_TYPE and
                                                              must.billing_period_end = adj.billing_period_end and
                                                              must.billing_period_begin = adj.billing_period_begin
                                                  ) t1 on t.bis_must_id = t1.bis_must_id
                                             group by t.bis_project_id, t.bis_cont_id,
                                                      substr(t.rent_last_pay_date, 0, 4),
                                                      substr(t.rent_last_pay_date, 0, 7), t.MUST_TYPE
                                         ) table3
                                         on table1.bis_project_id = table3.bis_project_id
                                             and table1.bis_cont_id = table3.bis_cont_id
                                             and table1.must_type = table3.must_type
                                             and table1.must_year_month = table3.must_year_month
                                ) t2 on t3.BIS_PROJECT_ID = t2.BIS_PROJECT_ID and t3.BIS_CONT_ID = t2.BIS_CONT_ID
                           group by t2.BIS_PROJECT_ID,
                                    t3.STORE_TYPE,
                                    t2.must_year_month
                       ) t6
                       on t4.bis_project_id = t6.bis_project_id and t4.year_month1 = t6.must_year_month
              ) t1 on t.bis_project_id = t1.bis_project_id and t.short_name = t1.short_name
                  and t.year_month1 = t1.year_month1 and t.store_type = t1.store_type and t.must_type = t1.must_type
                  left join
              (
                  -- 多经 物管 租金 预算
                  select t.bis_project_id,                                                                  -- 项目id
                         t.annual,                                                                          -- 年
                         substr(month_budget_money, 0, instr(month_budget_money, '-') - 1) as budget_month, -- 月
                         t.charge_type,                                                                     -- 物业类型 （1购物中心2商业街3住宅4住宅底商5写字楼 6住宅公寓7住宅别墅）
                         t.store_type,                                                                      -- 1 铺位  2 多径
                         substr(month_budget_money, instr(month_budget_money, '-') + 1, 1) as fee_type,     -- 费项 (1:租金 2：物管 3： 多经)
                         substr(month_budget_money, instr(month_budget_money, '=') + 1)    as budget_money  -- 租金 物管 多经 每个月的预算金额
                  from (
                           SELECT out_mall_id as bis_project_id, -- 项目id
                                  lease.annual,                  -- 年
                                  lease.charge_type,             -- 物业类型 （1购物中心2商业街3住宅4住宅底商5写字楼 6住宅公寓7住宅别墅）
                                  lease.store_type,              -- 1 铺位  2 多径
                                  -- if(lease.store_type = '1 ：铺位','1：租金','3：多经')   -- if(lease.store_type = '1 ：铺位','1：物管','无效‘)
                                  concat_ws(',',
                                            concat('1', '-', if(lease.store_type = '1', '1', '3'), '=',
                                                   string(sum(jan_zj))),
                                            concat('2', '-', if(lease.store_type = '1', '1', '3'), '=',
                                                   string(sum(feb_zj))),
                                            concat('3', '-', if(lease.store_type = '1', '1', '3'), '=',
                                                   string(sum(march_zj))),
                                            concat('4', '-', if(lease.store_type = '1', '1', '3'), '=',
                                                   string(sum(april_zj))),
                                            concat('5', '-', if(lease.store_type = '1', '1', '3'), '=',
                                                   string(sum(may_zj))),
                                            concat('6', '-', if(lease.store_type = '1', '1', '3'), '=',
                                                   string(sum(june_zj))),
                                            concat('7', '-', if(lease.store_type = '1', '1', '3'), '=',
                                                   string(sum(july_zj))),
                                            concat('8', '-', if(lease.store_type = '1', '1', '3'), '=',
                                                   string(sum(aug_zj))),
                                            concat('9', '-', if(lease.store_type = '1', '1', '3'), '=',
                                                   string(sum(sep_zj))),
                                            concat('10', '-', if(lease.store_type = '1', '1', '3'), '=',
                                                   string(sum(oct_zj))),
                                            concat('11', '-', if(lease.store_type = '1', '1', '3'), '=',
                                                   string(sum(nov_zj))),
                                            concat('12', '-', if(lease.store_type = '1', '1', '3'), '=',
                                                   string(sum(dece_zj))),
                                            if(string(sum(jan_wg)) is null, null,
                                               concat('1', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                                      string(sum(jan_wg)))),
                                            if(string(sum(feb_wg)) is null, null,
                                               concat('2', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                                      string(sum(feb_wg)))),
                                            if(string(sum(march_wg)) is null, null,
                                               concat('3', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                                      string(sum(march_wg)))),
                                            if(string(sum(april_wg)) is null, null,
                                               concat('4', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                                      string(sum(april_wg)))),
                                            if(string(sum(may_wg)) is null, null,
                                               concat('5', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                                      string(sum(may_wg)))),
                                            if(string(sum(june_wg)) is null, null,
                                               concat('6', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                                      string(sum(june_wg)))),
                                            if(string(sum(july_wg)) is null, null,
                                               concat('7', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                                      string(sum(july_wg)))),
                                            if(string(sum(aug_wg)) is null, null,
                                               concat('8', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                                      string(sum(aug_wg)))),
                                            if(string(sum(sep_wg)) is null, null,
                                               concat('9', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                                      string(sum(sep_wg)))),
                                            if(string(sum(oct_wg)) is null, null,
                                               concat('10', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                                      string(sum(oct_wg)))),
                                            if(string(sum(nov_wg)) is null, null,
                                               concat('11', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                                      string(sum(nov_wg)))),
                                            if(string(sum(dece_wg)) is null, null,
                                               concat('12', '-', if(lease.store_type = '1', '2', '无效'), '=',
                                                      string(sum(dece_wg))))
                                      )       as res

                           FROM ods.ods_pl_pms_bis_db_bs_mall_dt m
                                    inner join ods.ods_pl_pms_budget_db_budget_instance_dt instance
                                               on m.id = instance.project_id
                                    INNER JOIN ods.ods_pl_pms_budget_db_budget_instance_sheet_dt instanceSheet
                                               ON instance.id = instanceSheet.budget_instance_id
                                                   AND instance.instance_type = 1
                                                   AND instanceSheet.budget_sheet_id in (29, 52)
                                    inner join ods.ods_pl_pms_budget_db_budget_instance_biz_lease_dt lease
                                               on instanceSheet.id = lease.budget_instance_sheet_id and
                                                  lease.is_del = false and lease.store_type = '2'
                           where m.is_del = '0'
                             and m.stat = '2'
                           group by out_mall_id, lease.annual, lease.charge_type, lease.store_type
                       ) t
                           lateral view
                               explode(split(t.res, ",")) t1 as month_budget_money
                  where length(month_budget_money) > 0
              ) t3 on t1.bis_project_id = t3.bis_project_id and t1.year = t3.annual and
                      t1.month = t3.budget_month and t1.must_type = t3.fee_type and
                      t1.store_type = t3.charge_type
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
              ) t2 on t.bis_project_id = t2.out_mall_id
     ) t
where t.bis_project_id = '6D7E1C7AFAFB43E986670A81CF444253'
  and t.year = '2021'
group by t.bis_project_id, t.year_month1;



select bis_project_id,
       query_date,
       sum(total_cont_must_money),
       sum(total_cont_fact_money),
       sum(total_cont_month_fact_money),
       sum(total_cont_month_must_money),
       sum(total_cont_year_must_money),
       sum(total_cont_year_fact_money)
from dwd.dwd_bis_rent_mgr_multi_current_month_big_dt
where bis_project_id = '6D7E1C7AFAFB43E986670A81CF444253'
  and query_date like '2021%'
  and fee_type = '3'
group by bis_project_id, query_date;



select BIS_STORE_ID, IS_ASSESS
from dwd.dwd_bis_store_basics_big_dt
where bis_store_id in ('8a7b868b78cb6ed00178cdadf40b4978', '8a7b868b78cb6ed00178cdade8a14911');


select bis_project_id,
       qz_year_month,
       sum(nvl(rent_money, 0)),
       sum(nvl(small_store_rent_money, 0)),
       sum(nvl(main_store_rent_money, 0))
from (
         select bis_must_basic.bis_project_id,                    -- 项目id
                bis_must_basic.store_type,                        -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                bis_must_basic.qz_year_month,                     -- 租金应收权责月
                sum(must_money)        as rent_money,             -- 每个月的租金权责应收总金额(不区分小商铺和主力店)
                sum(case
                        when bis_store_basics.store_position > '3' then must_money
                        else null end) as small_store_rent_money, -- 月租金（小商铺）
                sum(case
                        when bis_store_basics.store_position <= '3' then must_money
                        else null end) as main_store_rent_money   -- 月租金（主力店）
         from dwd.dwd_bis_must_qz_basic_big_dt bis_must_basic
                  left join dwd.dwd_bis_cont_basics_big_dt bis_store_basics
                            on bis_must_basic.bis_project_id = bis_store_basics.bis_project_id and
                               bis_must_basic.bis_cont_id = bis_store_basics.bis_cont_id and
                               bis_store_basics.dt = date_format(current_date, 'YYYYMMdd')

         where bis_must_basic.dt = date_format(current_date, 'YYYYMMdd')
           and bis_must_basic.must_type = '1'
           -- and bis_store_basics.store_position is not null
         group by bis_must_basic.bis_project_id, bis_must_basic.store_type, bis_must_basic.qz_year_month
     ) t
WHERE store_type IN ('1', '2')
  AND bis_project_id = '6D7E1C7AFAFB43E986670A81CF444241'
  AND qz_year_month BETWEEN '2020-01'
    AND '2021-12'
GROUP BY bis_project_id, qz_year_month;



select bis_must_basic.bis_project_id, -- 项目id
       bis_must_basic.store_type,     -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
       bis_must_basic.qz_year_month,  -- 租金应收权责月
       bis_store_basics.store_position,
       bis_must_basic.must_money
from dwd.dwd_bis_must_qz_basic_big_dt bis_must_basic
         left join dwd.dwd_bis_cont_basics_big_dt bis_store_basics
                   on bis_must_basic.bis_project_id = bis_store_basics.bis_project_id and
                      bis_must_basic.bis_cont_id = bis_store_basics.bis_cont_id and
                      bis_store_basics.dt = date_format(current_date, 'YYYYMMdd')
where bis_must_basic.dt = date_format(current_date, 'YYYYMMdd')
  and bis_store_basics.store_position is not null
  and bis_must_basic.must_type = '1'
  and bis_must_basic.store_type IN ('1', '2')
  AND bis_must_basic.bis_project_id = '6D7E1C7AFAFB43E986670A81CF444241'
  AND bis_must_basic.qz_year_month BETWEEN '2020-01'
    AND '2021-12';



select must_year_month,
       sum(factMoney)
from (
         select t.bis_project_id,                                      -- 项目id
                t.bis_cont_id,                                         -- 合同id
                t.MUST_TYPE,                                           -- 费项（1：租金 2：物管）
                substr(t.rent_last_pay_date, 0, 7) must_year_month,    -- 应收月

                sum(case
                        when substr(fact.fact_date, 0, 7) <= substr(t.rent_last_pay_date, 0, 7)
                            then nvl(fact.factMoney, 0)
                        else 0 end)                factMoney,          -- 月实收

                null                               month_lj_factMoney, -- 月累计实收

                null                               year_lf_factMoney   -- 年累计实收


         from (
                  -- 应收
                  SELECT BIS_MUST2.bis_project_id,
                         BIS_MUST2.bis_cont_id,
                         BIS_MUST2.qz_year_month,
                         BIS_MUST2.MUST_TYPE,
                         BIS_MUST2.billing_period_end,
                         BIS_MUST2.billing_period_begin,
                         BIS_MUST2.bis_must_id,
                         BIS_MUST2.rent_last_pay_date, -- 应收日期
                         sum(nvl(BIS_MUST2.money, 0)) as mustMoney
                  FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                  where BIS_MUST2.must_type in ('1', '2')
                    and BIS_MUST2.is_show = 1
                    and BIS_MUST2.is_delete = 0
                  group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                           BIS_MUST2.bis_must_id,
                           BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                           BIS_MUST2.billing_period_end,
                           BIS_MUST2.billing_period_begin,
                           BIS_MUST2.rent_last_pay_date
              ) t
                  left join
              (
                  select must.bis_must_id,
                         adj.adjMoney
                  from (
                           -- 应收
                           SELECT BIS_MUST2.bis_project_id,
                                  BIS_MUST2.bis_cont_id,
                                  BIS_MUST2.qz_year_month,
                                  BIS_MUST2.MUST_TYPE,
                                  BIS_MUST2.billing_period_end,
                                  BIS_MUST2.billing_period_begin,
                                  min(BIS_MUST2.rent_last_pay_date) rent_last_pay_date, -- 应收日期
                                  min(BIS_MUST2.bis_must_id)   as   bis_must_id,
                                  sum(nvl(BIS_MUST2.money, 0)) as   mustMoney
                           FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                           where BIS_MUST2.must_type in ('1', '2')
                             and BIS_MUST2.is_show = 1
                             and BIS_MUST2.is_delete = 0
                           group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                                    BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                                    BIS_MUST2.billing_period_end,
                                    BIS_MUST2.billing_period_begin
                       ) must
                           left join
                       (
                           -- 减免
                           SELECT bis_mf_adjust.bis_cont_id,
                                  bis_mf_adjust.billing_period_end,
                                  bis_mf_adjust.billing_period_begin,
                                  bis_mf_adjust.qz_year_month,
                                  bis_mf_adjust.FEE_TYPE,
                                  sum(nvl(bis_mf_adjust.adjust_money, 0)) as adjMoney
                           FROM ods.ods_pl_powerdes_bis_mf_adjust_dt bis_mf_adjust
                           where bis_mf_adjust.is_del = 1
                             and bis_mf_adjust.fee_type in ('1', '2')
                           group by bis_mf_adjust.bis_cont_id,
                                    bis_mf_adjust.qz_year_month,
                                    bis_mf_adjust.FEE_TYPE,
                                    bis_mf_adjust.billing_period_end,
                                    bis_mf_adjust.billing_period_begin
                       ) adj
                           -- 合同 权责 费项 账期开始  账期结束
                       on must.bis_cont_id = adj.bis_cont_id and
                          must.qz_year_month = adj.qz_year_month and
                          must.MUST_TYPE = adj.FEE_TYPE and
                          must.billing_period_end = adj.billing_period_end and
                          must.billing_period_begin = adj.billing_period_begin
              ) t1 on t.bis_must_id = t1.bis_must_id
                  left join
              (
                  -- 实收
                  SELECT bis_fact2.bis_cont_id,
                         bis_fact2.bis_must_id,
                         bis_fact2.fact_type,
                         bis_fact2.fact_date,
                         bis_fact2.must_rent_last_pay_date,
                         sum(nvl(bis_fact2.money, 0)) as factMoney
                  FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
                  where bis_fact2.is_delete = 1
                    and bis_fact2.fact_type in ('1', '2')
                    and bis_fact2.bis_must_id is not null
                  group by bis_fact2.bis_cont_id, bis_fact2.bis_must_id,
                           bis_fact2.fact_type,
                           bis_fact2.fact_date,
                           bis_fact2.must_rent_last_pay_date
              ) fact
              on t.bis_cont_id = fact.bis_cont_id and
                 t.bis_must_id = fact.bis_must_id and
                 t.MUST_TYPE = fact.FACT_TYPE
         group by t.bis_project_id, t.bis_cont_id,
                  substr(t.rent_last_pay_date, 0, 4),
                  substr(t.rent_last_pay_date, 0, 7), t.MUST_TYPE
     ) t
where bis_project_id = '542gw502d453f27012d4ef49eb58o07F'
  and must_year_month like '2021%'
  and must_type = '1'
group by bis_project_id, must_year_month;



select fact.bis_project_id,
       substr(fact.must_rent_last_pay_date, 0, 7),
       sum(fact.factMoney)
from (
         -- 实收
         SELECT bis_fact2.bis_project_id,
                bis_fact2.bis_cont_id,
                bis_fact2.bis_must_id,
                bis_fact2.fact_type,
                bis_fact2.fact_date,
                bis_fact2.must_rent_last_pay_date,
                sum(nvl(bis_fact2.money, 0)) as factMoney
         FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
         where bis_fact2.is_delete = 1
           and bis_fact2.fact_type in ('1', '2')
           and bis_fact2.bis_must_id is not null
         group by bis_fact2.bis_cont_id, bis_fact2.bis_must_id,
                  bis_fact2.fact_type,
                  bis_fact2.fact_date,
                  bis_fact2.must_rent_last_pay_date,
                  bis_fact2.bis_project_id
     ) fact
where fact.bis_project_id = '542gw502d453f27012d4ef49eb58o07F'
  and fact.must_rent_last_pay_date like '2021%'
  and fact.fact_type = '1'
  and substr(fact.fact_date, 0, 7) <= substr(must_rent_last_pay_date, 0, 7)
group by fact.bis_project_id, substr(fact.must_rent_last_pay_date, 0, 7);



select t.bis_project_id,                                      -- 项目id
       t.bis_cont_id,                                         -- 合同id
       t.MUST_TYPE,                                           -- 费项（1：租金 2：物管）
       substr(t.rent_last_pay_date, 0, 7) must_year_month,    -- 应收月

       sum(case
               when substr(fact.fact_date, 0, 7) <= substr(t.rent_last_pay_date, 0, 7)
                   then nvl(fact.factMoney, 0)
               else 0 end)                factMoney,          -- 月实收

       null                               month_lj_factMoney, -- 月累计实收

       null                               year_lf_factMoney   -- 年累计实收


from (
         -- 应收
         SELECT BIS_MUST2.bis_project_id,
                BIS_MUST2.bis_cont_id,
                BIS_MUST2.qz_year_month,
                BIS_MUST2.MUST_TYPE,
                BIS_MUST2.billing_period_end,
                BIS_MUST2.billing_period_begin,
                BIS_MUST2.bis_must_id,
                BIS_MUST2.rent_last_pay_date, -- 应收日期
                sum(nvl(BIS_MUST2.money, 0)) as mustMoney
         FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
         where BIS_MUST2.must_type in ('1', '2')
           and BIS_MUST2.is_show = 1
           and BIS_MUST2.is_delete = 0
           and bis_must_id = '8a7b859c6e5fbe71016e690a65052ecd'
         group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                  BIS_MUST2.bis_must_id,
                  BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                  BIS_MUST2.billing_period_end,
                  BIS_MUST2.billing_period_begin,
                  BIS_MUST2.rent_last_pay_date
     ) t
         left join
     (
         select must.bis_must_id,
                adj.adjMoney
         from (
                  -- 应收
                  SELECT BIS_MUST2.bis_project_id,
                         BIS_MUST2.bis_cont_id,
                         BIS_MUST2.qz_year_month,
                         BIS_MUST2.MUST_TYPE,
                         BIS_MUST2.billing_period_end,
                         BIS_MUST2.billing_period_begin,
                         min(BIS_MUST2.rent_last_pay_date) rent_last_pay_date, -- 应收日期
                         min(BIS_MUST2.bis_must_id)   as   bis_must_id,
                         sum(nvl(BIS_MUST2.money, 0)) as   mustMoney
                  FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                  where BIS_MUST2.must_type in ('1', '2')
                    and BIS_MUST2.is_show = 1
                    and BIS_MUST2.is_delete = 0
                  group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                           BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                           BIS_MUST2.billing_period_end,
                           BIS_MUST2.billing_period_begin
              ) must
                  left join
              (
                  -- 减免
                  SELECT bis_mf_adjust.bis_cont_id,
                         bis_mf_adjust.billing_period_end,
                         bis_mf_adjust.billing_period_begin,
                         bis_mf_adjust.qz_year_month,
                         bis_mf_adjust.FEE_TYPE,
                         sum(nvl(bis_mf_adjust.adjust_money, 0)) as adjMoney
                  FROM ods.ods_pl_powerdes_bis_mf_adjust_dt bis_mf_adjust
                  where bis_mf_adjust.is_del = 1
                    and bis_mf_adjust.fee_type in ('1', '2')
                  group by bis_mf_adjust.bis_cont_id,
                           bis_mf_adjust.qz_year_month,
                           bis_mf_adjust.FEE_TYPE,
                           bis_mf_adjust.billing_period_end,
                           bis_mf_adjust.billing_period_begin
              ) adj
                  -- 合同 权责 费项 账期开始  账期结束
              on must.bis_cont_id = adj.bis_cont_id and
                 must.qz_year_month = adj.qz_year_month and
                 must.MUST_TYPE = adj.FEE_TYPE and
                 must.billing_period_end = adj.billing_period_end and
                 must.billing_period_begin = adj.billing_period_begin
     ) t1 on t.bis_must_id = t1.bis_must_id
         left join
     (
         -- 实收
         SELECT bis_fact2.bis_cont_id,
                bis_fact2.bis_must_id,
                bis_fact2.fact_type,
                bis_fact2.fact_date,
                bis_fact2.must_rent_last_pay_date,
                sum(nvl(bis_fact2.money, 0)) as factMoney
         FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
         where bis_fact2.is_delete = 1
           and bis_fact2.fact_type in ('1', '2')
           and bis_fact2.bis_must_id is not null
         group by bis_fact2.bis_cont_id, bis_fact2.bis_must_id,
                  bis_fact2.fact_type,
                  bis_fact2.fact_date,
                  bis_fact2.must_rent_last_pay_date
     ) fact
     on t.bis_cont_id = fact.bis_cont_id and
        t.bis_must_id = fact.bis_must_id and
        t.MUST_TYPE = fact.FACT_TYPE
where t.bis_must_id = '8a7b859c6e5fbe71016e690a65052ecd'
group by t.bis_project_id, t.bis_cont_id,
         substr(t.rent_last_pay_date, 0, 4),
         substr(t.rent_last_pay_date, 0, 7), t.MUST_TYPE;



select BIS_SHOP.BIS_SHOP_ID,                               --商家id
       max(case
               when BIS_SHOP_SORT.SORT_TYPE = 1 then BIS_SHOP_SORT.SORT_NAME
               else null end) as     primary_forms,        -- 一级业态
       max(BIS_SHOP.COMPANY_NAME)    company_name,         -- 品牌集团名称
       max(BIS_SHOP.NAME_CN)         cooperative_brand_CN, -- 合作品牌中文
       max(BIS_SHOP.name_en)         cooperative_brand_EN, -- 合作品牌英文
       max(BIS_SHOP.SHOP_LEVEL)      business_level,       -- 商家级别
       max(BIS_SHOP.BRAND_OWNERSHIP) BRAND_OWNERSHIP,      -- 品牌归属（1总部、2区域 3:其他品牌 4:黑名单）
       max(area_type)                area_type             -- 品牌所属区域
from ods.ods_pl_powerdes_bis_shop_dt BIS_SHOP
         left join ods.ods_pl_powerdes_bis_shop_sort_rel_dt BIS_SHOP_SORT_REL
                   on BIS_SHOP.BIS_SHOP_ID = BIS_SHOP_SORT_REL.BIS_SHOP_ID
         left join ods.ods_pl_powerdes_bis_shop_sort_dt BIS_SHOP_SORT
                   on BIS_SHOP_SORT.BIS_SHOP_SORT_ID = BIS_SHOP_SORT_REL.BIS_SHOP_SORT_ID
where BIS_SHOP.delete_bl <> '1'
  and (BIS_SHOP.IS_NEW is null or BIS_SHOP.IS_NEW = '0')
  and BIS_SHOP_SORT.SORT_TYPE <> '0'
  and BIS_SHOP_SORT.SORT_TYPE is not null
  and BIS_SHOP.BRAND_OWNERSHIP in ('1', '2')
  and BIS_SHOP.bis_shop_id = '0514BSH_16089A38282EF7E6E050007F010071EB'
group by BIS_SHOP.BIS_SHOP_ID;



select BIS_SHOP.BIS_SHOP_ID,                               --商家id
       max(case
               when BIS_SHOP_SORT_NEW.SORT_TYPE = 1 then BIS_SHOP_SORT_NEW.SORT_NAME
               else null end) as     primary_forms,        -- 一级业态
       max(BIS_SHOP.COMPANY_NAME)    company_name,         -- 品牌集团名称
       max(BIS_SHOP.NAME_CN)         cooperative_brand_CN, -- 合作品牌中文
       max(BIS_SHOP.name_en)         cooperative_brand_EN, -- 合作品牌英文
       max(BIS_SHOP.SHOP_LEVEL)      business_level,       -- 商家级别
       max(BIS_SHOP.BRAND_OWNERSHIP) BRAND_OWNERSHIP,      -- 品牌归属（1:总部、2区域 3:其他品牌 4:黑名单）
       max(area_type)                area_type             -- 品牌所属区域

from ods.ods_pl_powerdes_bis_shop_dt BIS_SHOP
         left join ods.ods_pl_powerdes_bis_shop_sort_new_rel_dt BIS_SHOP_SORT_NEW_REL
                   on BIS_SHOP.BIS_SHOP_ID = BIS_SHOP_SORT_NEW_REL.BIS_SHOP_ID
         left join ods.ods_pl_powerdes_bis_shop_sort_new_dt BIS_SHOP_SORT_NEW
                   on BIS_SHOP_SORT_NEW_REL.BIS_SHOP_SORT_ID = BIS_SHOP_SORT_NEW.BIS_SHOP_SORT_ID
where BIS_SHOP.delete_bl <> '1'
  and IS_NEW = '1'
  and BIS_SHOP.BRAND_OWNERSHIP in ('1', '2')
  and BIS_SHOP.bis_shop_id = '0514BSH_16089A38282EF7E6E050007F010071EB'
group by BIS_SHOP.BIS_SHOP_ID;



select table1.BIS_SHOP_ID,                                           -- 商家id
       table1.primary_forms,                                         -- 一级业态
       table1.company_name,                                          -- 品牌集团名称
       table1.cooperative_brand_CN,                                  -- 合作品牌中文
       table1.cooperative_brand_EN,                                  -- 合作品牌英文
       table1.business_level,                                        -- 商家级别(0：S；1：A；2：B；3：C；4：D；5：L)
       table1.BRAND_OWNERSHIP,                                       -- 品牌归属（1总部、2区域 3:其他品牌 4:黑名单）
       table1.area_type,                                             -- 品牌所属区域
       table2.project_number,                                        -- 项目合作数量
       table2.PROJECT_NAME,                                          -- 合作项目
       table2.total_rent_area,                                       -- 合作租赁面积
       table4.hap_2018,                                              -- 当前年往前二年的销售额
       table5.hap_2019,                                              -- 当前年往前一年的销售额
       table6.accumulated_turnover_2020,                             -- 当前年销售额
       table9.owe_qz,                                                -- 权责欠费
       table9.owe_cont,                                              -- 合同欠费
       case when table9.owe_qz > 0 then '1' else '2' end as owe_type -- 欠费情况(1: 欠费 2：不欠费)

from (
         select BIS_SHOP.BIS_SHOP_ID,                               --商家id
                max(case
                        when BIS_SHOP_SORT_NEW.SORT_TYPE = 1 then BIS_SHOP_SORT_NEW.SORT_NAME
                        else null end) as     primary_forms,        -- 一级业态
                max(BIS_SHOP.COMPANY_NAME)    company_name,         -- 品牌集团名称
                max(BIS_SHOP.NAME_CN)         cooperative_brand_CN, -- 合作品牌中文
                max(BIS_SHOP.name_en)         cooperative_brand_EN, -- 合作品牌英文
                max(BIS_SHOP.SHOP_LEVEL)      business_level,       -- 商家级别
                max(BIS_SHOP.BRAND_OWNERSHIP) BRAND_OWNERSHIP,      -- 品牌归属（1:总部、2区域 3:其他品牌 4:黑名单）
                max(area_type)                area_type             -- 品牌所属区域

         from ods.ods_pl_powerdes_bis_shop_dt BIS_SHOP
                  left join ods.ods_pl_powerdes_bis_shop_sort_new_rel_dt BIS_SHOP_SORT_NEW_REL
                            on BIS_SHOP.BIS_SHOP_ID = BIS_SHOP_SORT_NEW_REL.BIS_SHOP_ID
                  left join ods.ods_pl_powerdes_bis_shop_sort_new_dt BIS_SHOP_SORT_NEW
                            on BIS_SHOP_SORT_NEW_REL.BIS_SHOP_SORT_ID = BIS_SHOP_SORT_NEW.BIS_SHOP_SORT_ID
         where BIS_SHOP.delete_bl <> '1'
           and IS_NEW = '1'
           and BIS_SHOP.BRAND_OWNERSHIP in ('1', '2')
         group by BIS_SHOP.BIS_SHOP_ID
     ) table1
         left join
     (
         SELECT COUNT(BIS_CONT.BIS_PROJECT_ID)                         project_number,
                BIS_SHOP.BIS_SHOP_ID,
                concat_ws(",", collect_list(bis_project.project_name)) PROJECT_NAME,
                sum(BIS_CONT.RENT_SQUARE) as                           total_rent_area
         FROM ods.ods_pl_powerdes_bis_shop_dt BIS_SHOP
                  LEFT JOIN ods.ods_pl_powerdes_bis_cont_dt BIS_CONT ON BIS_SHOP.BIS_SHOP_ID = BIS_CONT.BIS_SHOP_ID
                  left join ods.ods_pl_powerdes_bis_project_dt bis_project
                            on BIS_CONT.BIS_PROJECT_ID = BIS_PROJECT.BIS_PROJECT_ID
         where bis_shop.delete_bl <> '1'
           and IS_NEW = '1'
           AND bis_cont.effect_flg = 'Y'
         GROUP BY BIS_SHOP.BIS_SHOP_ID
     ) table2 on table1.BIS_SHOP_ID = table2.BIS_SHOP_ID
         LEFT JOIN
     (
         select t.bis_cont_id,
                t.hap_2018,
                bis_cont.bis_shop_id
         from (
                  select SUM(SALES_MONEY) hap_2018,
                         BIS_CONT_ID
                  from ods.ods_pl_powerdes_bis_sales_day_dt BIS_SALES_DAY
                  where substr(SALES_DATE, 0, 4) = year(add_months(current_date, -12 * 2))
                  GROUP BY BIS_CONT_ID, substr(SALES_DATE, 0, 4)
              ) t
                  left join ods.ods_pl_powerdes_bis_cont_dt bis_cont on t.bis_cont_id = bis_cont.bis_cont_id
     ) table4 on table1.bis_shop_id = table4.bis_shop_id
         LEFT JOIN
     (
         select t.hap_2019,
                t.bis_cont_id,
                bis_cont.bis_shop_id
         from (
                  select SUM(SALES_MONEY) hap_2019,
                         BIS_CONT_ID
                  from ods.ods_pl_powerdes_bis_sales_day_dt BIS_SALES_DAY
                  where substr(SALES_DATE, 0, 4) = year(add_months(current_date, -12 * 1))
                  GROUP BY BIS_CONT_ID, substr(SALES_DATE, 0, 4)
              ) t
                  left join ods.ods_pl_powerdes_bis_cont_dt bis_cont on t.bis_cont_id = bis_cont.bis_cont_id
     ) table5 on table1.bis_shop_id = table5.bis_shop_id
         LEFT JOIN
     (
         select t.bis_cont_id,
                t.accumulated_turnover_2020,
                bis_cont.bis_shop_id
         from (
                  select SUM(SALES_MONEY) accumulated_turnover_2020,
                         BIS_CONT_ID
                  from ods.ods_pl_powerdes_bis_sales_day_dt BIS_SALES_DAY
                  where substr(SALES_DATE, 0, 4) = year(current_date)
                  GROUP BY BIS_CONT_ID, substr(SALES_DATE, 0, 4)
              ) t
                  left join ods.ods_pl_powerdes_bis_cont_dt bis_cont on t.bis_cont_id = bis_cont.bis_cont_id
     ) table6 on table1.bis_shop_id = table6.bis_shop_id
         left join
     (
         select bis_shop_id,                     -- 品牌id
                round(sum(owe_qz), 2)   owe_qz,  -- 权责欠费
                round(sum(owe_cont), 2) owe_cont -- 合同欠费
         from dws.dws_bis_rent_mgr_arrearage_big_dt
         where dt = date_format(current_date, 'YYYYMMdd')
         group by bis_shop_id
     ) table9 on table1.bis_shop_id = table9.bis_shop_id
where table1.bis_shop_id = '0514BSH_16089A38282EF7E6E050007F010071EB';



select OUT_CONT_ID                                                       BIS_CONT_ID,
       from_unixtime(unix_timestamp(SALE_YMD, 'yyyymmdd'), 'yyyy-mm-dd') SALES_DATE
from ods.ods_pl_pms_bis_db_mng_sale_amount_day_dt mng_sale_amount_day
where concat(OUT_CONT_ID, from_unixtime(unix_timestamp(SALE_YMD, 'yyyymmdd'), 'yyyy-mm-dd')) not in
      (select concat(bis_cont_id, substr(SALES_DATE, 0, 10)) from ods.ods_pl_powerdes_bis_sales_day_dt BIS_SALES_DAY);


select bis_cont_id,
       substr(SALES_DATE, 0, 10),
       concat(bis_cont_id, substr(SALES_DATE, 0, 10))
from ods.ods_pl_powerdes_bis_sales_day_dt BIS_SALES_DAY
where concat(bis_cont_id, substr(SALES_DATE, 0, 10)) not in
      (select concat(OUT_CONT_ID, from_unixtime(unix_timestamp(SALE_YMD, 'yyyymmdd'), 'yyyy-mm-dd'))
       from ods.ods_pl_pms_bis_db_mng_sale_amount_day_dt mng_sale_amount_day);









