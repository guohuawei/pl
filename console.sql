SELECT tmp.bis_cont_id,
       tmp.bis_store_ids,
       bis_store_id
FROM (
         SELECT bis_cont_id,
                bis_store_ids
         FROM ods.ods_pl_powerdes_bis_cont_dt
     ) tmp LATERAL VIEW explode(split(bis_store_ids, ',')) tmp1 AS bis_store_id;



SELECT max(bis_store_id) as                         store_id,
       concat_ws(',', collect_set(tmp.bis_cont_id)) bis_cont_ids,
       count(tmp.bis_cont_id)                       cont_count
FROM (
         SELECT bis_cont_id,
                bis_store_ids
         FROM ods.ods_pl_powerdes_bis_cont_dt
             /*2021 需要动态传值*/
         where '2021' between year(cont_start_date) and year(cont_end_date)
     ) tmp LATERAL VIEW explode(split(bis_store_ids, ',')) tmp1 AS bis_store_id
group by bis_store_id;


select table1.bis_cont_id,
       table1.must_type, -- 1:租金  2 物管
       MAX(table1.money_01)                                                      AS money_01,
       MAX(table1.money_2)                                                       AS money_2,
       MAX(table1.money_3)                                                       AS money_3,
       MAX(table1.money_4)                                                       AS money_4,
       MAX(table1.money_5)                                                       AS money_5,
       MAX(table1.money_6)                                                       AS money_6,
       MAX(table1.money_7)                                                       AS money_7,
       MAX(table1.money_8)                                                       AS money_8,
       MAX(table1.money_9)                                                       AS money_9,
       MAX(table1.money_10)                                                      AS money_10,
       MAX(table1.money_11)                                                      AS money_11,
       MAX(table1.money_12)                                                      AS money_12,
       (MAX(table1.money_01) + MAX(table1.money_2) + MAX(table1.money_3) + MAX(table1.money_4) + MAX(table1.money_5)
           + MAX(table1.money_6) + MAX(table1.money_7) + MAX(table1.money_8) + MAX(table1.money_9)
           + MAX(table1.money_10) + MAX(table1.money_11) + MAX(table1.money_12)) as total_money
from (
         select BIS_CONT_ID,
                must_type,
                case when QZ_YEAR_MONTH = '2021-01' then MONEY else null end as money_01,
                case when QZ_YEAR_MONTH = '2021-02' then MONEY else null end as money_2,
                case when QZ_YEAR_MONTH = '2021-03' then MONEY else null end as money_3,
                case when QZ_YEAR_MONTH = '2021-04' then MONEY else null end as money_4,
                case when QZ_YEAR_MONTH = '2021-05' then MONEY else null end as money_5,
                case when QZ_YEAR_MONTH = '2021-06' then MONEY else null end as money_6,
                case when QZ_YEAR_MONTH = '2021-07' then MONEY else null end as money_7,
                case when QZ_YEAR_MONTH = '2021-08' then MONEY else null end as money_8,
                case when QZ_YEAR_MONTH = '2021-09' then MONEY else null end as money_9,
                case when QZ_YEAR_MONTH = '2021-10' then MONEY else null end as money_10,
                case when QZ_YEAR_MONTH = '2021-11' then MONEY else null end as money_11,
                case when QZ_YEAR_MONTH = '2021-12' then MONEY else null end as money_12
         from ods.ods_pl_powerdes_bis_must2_dt bis_must2
         where substr(bis_must2.QZ_YEAR_MONTH, 0, 4) = '2021'
           and MUST_TYPE in ('1', '2')
     ) table1
group by table1.bis_cont_id, table1.must_type;


select t2.bis_cont_id,
       t2.bis_store_id,
       t1.total_own_money,                                          -- 合同欠费总金额
       t3.total_area,                                               -- 合同所对应的所有铺位面积之和
       bis_store.rent_square,                                       -- 单个店铺面积
       (bis_store.rent_square / t3.total_area) * t1.total_own_money -- 按照店铺面积拆分后的店铺欠费金额
from (
         -- 合同欠费总金额
         select bis_cont_id,
                sum(own_money) as total_own_money
         from ods.ods_pl_powerdes_pms_fin_arrearage_dt pms_fin_arrearage
         where FEE_TYPE in ('1', '2', '6', '76', '63', '71', '7', '8', '62', '72', '79', '88')
         group by bis_cont_id
     ) t1
         left join
     (
         -- 铺位打散
         SELECT tmp.bis_cont_id,
                tmp.bis_store_ids,
                bis_store_id
         FROM (
                  SELECT bis_cont_id,
                         bis_store_ids
                  FROM ods.ods_pl_powerdes_bis_cont_dt
              ) tmp LATERAL VIEW explode(split(bis_store_ids, ',')) tmp1 AS bis_store_id
     ) t2
     on t1.bis_cont_id = t2.bis_cont_id
         left join ods.ods_pl_powerdes_bis_store_dt bis_store on bis_store.bis_store_id = t2.bis_store_id
         left join
     (
         select t4.bis_cont_id,
                sum(bis_store.rent_square) total_area -- 合同下所有铺位面积之和
         from (
                  -- 铺位打散
                  SELECT tmp.bis_cont_id,
                         bis_store_id
                  FROM (
                           SELECT bis_cont_id,
                                  bis_store_ids
                           FROM ods.ods_pl_powerdes_bis_cont_dt
                       ) tmp LATERAL VIEW explode(split(bis_store_ids, ',')) tmp1 AS bis_store_id
              ) t4
                  left join ods.ods_pl_powerdes_bis_store_dt bis_store on bis_store.bis_store_id = t4.bis_store_id
         group by t4.bis_cont_id
     ) t3 on t1.bis_cont_id = t3.bis_cont_id;



select t2.BIS_CONT_ID,
       t2.MYyear,    -- 年份
       t2.UNIT_MONEY -- 历史租金单价
from (
         select row_number() over (partition by t1.BIS_CONT_ID order by t1.MYyear desc ) rn,
                t1.*
         from (
                  select bis_must_rent.bis_cont_id,
                         (year(table5.cont_start_date) - 1 + cast(bis_must_rent.YEAR as int)) as MYyear,
                         bis_must_rent.unit_money -- 历史租金单价
                  from (
                           select bis_cont_id,
                                  cont_start_date,
                                  cont_end_date
                           from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                           where '2021' between year(bis_cont.cont_start_date) and year(
                                   if(bis_cont.status_cd = '2', bis_cont.cont_to_fail_date, bis_cont.cont_end_date))
                             and bis_cont.cont_type_cd in ('1', '2')
                             and bis_cont.effect_flg <> 'D'
                             and bis_cont.CONT_NO is not null
                       ) table5
                           left join ods.ods_pl_powerdes_bis_must_rent_dt bis_must_rent
                                     on table5.bis_cont_id = bis_must_rent.bis_cont_id and
                                        bis_must_rent.unit_money is not null
                  where (year(table5.cont_start_date) - 1 + cast(bis_must_rent.YEAR as int)) < '2021'
              ) t1) t2
where t2.rn = 1;



select t2.BIS_CONT_ID,
       t2.MYyear,    -- 年份
       t2.UNIT_MONEY -- 历史物管单价
from (
         select row_number() over (partition by t1.BIS_CONT_ID order by t1.MYyear desc ) rn,
                t1.*
         from (
                  select bis_must2_prop.bis_cont_id,
                         (year(table5.cont_start_date) - 1 + cast(bis_must2_prop.YEAR as int)) as MYyear,
                         bis_must2_prop.unit_money -- 历史物管单价
                  from (
                           select bis_cont_id,
                                  cont_start_date,
                                  cont_end_date
                           from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                           where '2021' between year(bis_cont.cont_start_date) and year(
                                   if(bis_cont.status_cd = '2', bis_cont.cont_to_fail_date, bis_cont.cont_end_date))
                             and bis_cont.cont_type_cd in ('1', '2')
                             and bis_cont.effect_flg <> 'D'
                             and bis_cont.CONT_NO is not null
                       ) table5
                           left join ods.ods_pl_powerdes_bis_must2_prop_dt bis_must2_prop
                                     on table5.bis_cont_id = bis_must2_prop.bis_cont_id and
                                        bis_must2_prop.unit_money is not null
                  where (year(table5.cont_start_date) - 1 + cast(bis_must2_prop.YEAR as int)) < '2021'
              ) t1) t2
where t2.rn = 1;



select table7.BIS_CONT_ID,
       table7.BIS_STORE_IDS,
       table7.UNIT_MONEY,
       table8.UNIT_MONEY,
       case
           when table8.UNIT_MONEY = 0 then '0.00%'
           else concat(round((table7.unit_money - table8.unit_money) / table8.unit_money, 2) * 100, '%') end as increate
from (
         select bis_must_rent.unit_money, -- 租金单价
                bis_must_rent.bis_cont_id,
                bis_must_rent.free_rent_period,
                bis_must_rent.ROYALTY_RATIO,
                table5.bis_store_ids

         from (
                  select bis_cont_id,
                         cont_start_date,
                         cont_end_date,
                         bis_store_ids
                  from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                  where '2021' between year(bis_cont.cont_start_date) and year(
                          if(bis_cont.status_cd = '2', bis_cont.cont_to_fail_date, bis_cont.cont_end_date))
                    and bis_cont.cont_type_cd in ('1', '2')
                    and bis_cont.effect_flg <> 'D'
                    and bis_cont.CONT_NO is not null
              ) table5
                  left join ods.ods_pl_powerdes_bis_must_rent_dt bis_must_rent
                            on table5.bis_cont_id = bis_must_rent.bis_cont_id
         where (year(table5.cont_start_date) - 1 + cast(bis_must_rent.YEAR as int)) = '2021'
     ) table7
         left join
     (
         select t2.BIS_CONT_ID,
                t2.MYyear,    -- 年份
                t2.UNIT_MONEY -- 历史租金单价
         from (
                  select row_number() over (partition by t1.BIS_CONT_ID order by t1.MYyear desc ) rn,
                         t1.*
                  from (
                           select bis_must_rent.bis_cont_id,
                                  (year(table5.cont_start_date) - 1 + cast(bis_must_rent.YEAR as int)) as MYyear,
                                  bis_must_rent.unit_money -- 历史租金单价
                           from (
                                    select bis_cont_id,
                                           cont_start_date,
                                           cont_end_date
                                    from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                                    where '2021' between year(bis_cont.cont_start_date) and year(
                                            if(bis_cont.status_cd = '2', bis_cont.cont_to_fail_date,
                                               bis_cont.cont_end_date))
                                      and bis_cont.cont_type_cd in ('1', '2')
                                      and bis_cont.effect_flg <> 'D'
                                      and bis_cont.CONT_NO is not null
                                ) table5
                                    left join ods.ods_pl_powerdes_bis_must_rent_dt bis_must_rent
                                              on table5.bis_cont_id = bis_must_rent.bis_cont_id and
                                                 bis_must_rent.unit_money is not null
                           where (year(table5.cont_start_date) - 1 + cast(bis_must_rent.YEAR as int)) < '2021'
                       ) t1) t2
         where t2.rn = 1
     ) table8 on table7.bis_cont_id = table8.bis_cont_id;


-- 查询连续登陆3天以上的用户
select userid, count(*)
from (select *, date_sub(logdate, rank) as resDate
      from (select *, row_number() over (partition by userid order by logdate) as rank
            from (select distinct to_date(loggindate) as logdate,
                                  userid
                  from test.loggininfo) t1) t2) t3
group by userid, resDate
having count(*) >= 3;



select COUNT(t.BIS_PROJECT_ID)                      project_number,
       t.BIS_SHOP_ID,
       concat_ws(",", collect_list(t.project_name)) project_name
from (
         SELECT BIS_CONT.BIS_PROJECT_ID,
                BIS_SHOP.BIS_SHOP_ID,
                BIS_PROJECT.PROJECT_NAME,
                row_number() over (partition by BIS_PROJECT.PROJECT_NAME order by BIS_PROJECT.PROJECT_NAME) rank
         FROM ods.ods_pl_powerdes_bis_shop_dt BIS_SHOP
                  LEFT JOIN ods.ods_pl_powerdes_bis_cont_dt BIS_CONT ON BIS_SHOP.BIS_SHOP_ID = BIS_CONT.BIS_SHOP_ID
                  left join ods.ods_pl_powerdes_bis_project_dt bis_project
                            on BIS_CONT.BIS_PROJECT_ID = BIS_PROJECT.BIS_PROJECT_ID
         where bis_shop.delete_bl <> '1'
           AND bis_cont.effect_flg <> 'D'
           AND BIS_SHOP.BIS_SHOP_ID = '8a7b8596558343e2015584f44a240672'
     ) t
where t.rank = 1
GROUP BY t.BIS_SHOP_ID;


select MallName, SITEKEY, CountDate, InSum, OutSum
from dwd.dwd_mongodb_passenger_flow_hour_big_dt
where dt = 20210107
  and substr(CountDate, 0, 4) = '2021'
  and MallName = '七宝'
  and SITEKEY = 'P99998';

select sitename, SITEKEY, CountDate, InSum, OutSum
from ods.ods_sqlserver_intsummary_sixty_dt
where substr(CountDate, 0, 4) = '2019'
  and sitename = '七宝宝龙城市广场';


select *
from dws.dws_bis_passenger_flow_day_big_dt
where short_name like '七宝%'
  and substr(date_id, 0, 4) = '2021';


select *
from dws.dws_bis_passenger_flow_hour_big_dt
where DATEKEY like '20210106'
  and SITENAME = '奉贤宝龙城市广场'
  and SITENAME_LOC = 'B1F观光梯'
order by hourkey;



(select prj.sitename as prj_sitename,
        prj.sitekey  as prj_sitekey,
        prj.siteid   as prj_siteid,
        prj.parentid as prj_parentid,
        loc.siteid   as loc_siteid,
        loc.sitekey  as loc_sitekey,
        loc.sitename as loc_sitename,
        loc.parentid as loc_parentid,
        loc.sitetype as loc_sitetype
 from (
          select SiteId,
                 SiteKey,
                 ParentId,
                 SiteName
          from ods.ods_sqlserver_inttraffic_sites_dt
          where SiteType = '300'
      ) prj
          left outer join
      (
          select SiteId,
                 SiteKey,
                 ParentId,
                 SiteName,
                 SiteType
          from ods.ods_sqlserver_inttraffic_sites_dt
          where SiteType in ('700', '300')
      ) loc
      on prj.SiteId = loc.ParentId

 union

 select prj.sitename as prj_sitename,
        prj.sitekey  as prj_sitekey,
        prj.siteid   as prj_siteid,
        prj.parentid as prj_parentid,
        loc.siteid   as loc_siteid,
        loc.sitekey  as loc_sitekey,
        loc.sitename as loc_sitename,
        loc.parentid as loc_parentid,
        loc.sitetype as loc_sitetype
 from (
          select SiteId,
                 SiteKey,
                 ParentId,
                 SiteName
          from ods.ods_sqlserver_inttraffic_sites_dt
          where SiteType = '300'
      ) prj
          left outer join
      (
          select SiteId,
                 SiteKey,
                 ParentId,
                 SiteName,
                 SiteType
          from ods.ods_sqlserver_inttraffic_sites_dt
          where SiteType in ('700', '300')
      ) loc
      on prj.SiteId = loc.SiteId);



select SHORT_NAME, MONTH_ID, sum(ACTUAL_FLOW)
from dws.dws_bis_passenger_flow_day_big_dt
where MONTH_ID = '202101'
  and dt = 20210113
  and SHORT_NAME in ('绍兴袍江', '天津')
group by SHORT_NAME, MONTH_ID;

select *
from ods.ods_sqlserver_intsummary_day_dt
where sitename in ('绍兴袍江', '天津')
  and substr(COUNTDATE, 0, 7) like '2021-01%';


select *
from (select distinct sitename from ods.ods_sqlserver_intsummary_day_dt) t
where t.sitename = '天津';



select count(1)
from (
         -- 从sqlserver抽取的日客流
         select sitekey,
                sitename,
                sitetype,
                sitetypename,
                cityid,
                countdate,
                insum,
                outsum,
                modifytime,
                customercode,
                date_format(current_date(), 'yyyyMMdd') as Synchro_dt
         from ods.ods_sqlserver_intsummary_day_dt
         where sitetype = '300' -- 300是广场，  400是区域   500是楼层    600是店铺   700是通道

         union all

-- 从mongodb抽取的日客流
         select SITEKEY,
                SITENAME,
                SITETYPE,
                null                     as SITETYPENAME,
                null                     as CITYID,
                substr(COUNTDATE, 0, 10) as COUNTDATE,
                INSUM,
                OUTSUM,
                MODIFYTIME,
                null                     as CUSTOMERCODE,
                date_format(current_date(), 'yyyyMMdd')
         from (select a.*,
                      RANK()
                              OVER (PARTITION BY countdate, sitetype, sitename ORDER BY systime desc) systime_rank
               from dwd.dwd_mongodb_passenger_flow_day_big_dt a
               where dt = date_format(current_date(), 'yyyyMMdd')
              ) days
         where systime_rank = 1
     ) t
where countdate like '2021-01%';



select BIS_PROJECT_ID,
       store_type,
       max(t.yestoday_total_money)        yestoday_total_money,
       max(t.before_yestoday_total_money) before_yestoday_total_money
from (
         select BIS_SALES_DAY.BIS_PROJECT_ID,
                case
                    when substr(SALES_DATE, 0, 10) = date_sub(current_date, 1) then sum(SALES_MONEY)
                    else null end as      yestoday_total_money,
                case
                    when substr(SALES_DATE, 0, 10) = date_sub(current_date, 2) then sum(SALES_MONEY)
                    else null end as      before_yestoday_total_money,
                substr(SALES_DATE, 0, 10) SALES_DATE,
                bis_cont.store_type

         from ods.ods_pl_powerdes_bis_sales_day_dt BIS_SALES_DAY
                  left join ods.ods_pl_powerdes_bis_cont_dt BIS_CONT
                            on BIS_CONT.BIS_CONT_ID = BIS_SALES_DAY.BIS_CONT_ID and BIS_CONT.EFFECT_FLG <> 'D'
         where (substr(SALES_DATE, 0, 10) = date_sub(current_date, 1) or
                substr(SALES_DATE, 0, 10) = date_sub(current_date, 2))
           and BIS_SALES_DAY.BIS_PROJECT_ID = '16084DA3912DDAE4E050007F01005603'
           AND BIS_SALES_DAY.BIS_CONT_ID IN (
             -- 考核铺位
             SELECT BIS_CONT.BIS_CONT_ID
             FROM ods.ods_pl_powerdes_bis_store_dt bis_store
                      inner join ods.ods_pl_powerdes_bis_cont_dt bis_cont
                                 on BIS_STORE.BIS_STORE_ID = BIS_CONT.BIS_STORE_IDS and BIS_CONT.EFFECT_FLG <> 'D'
             where bis_store.is_delete = 'N'
               -- /*考核商铺*/
               and bis_store.IS_ASSESS = 'Y'
               and bis_store.status_cd = '1' -- 有效铺位
               and BIS_STORE.BIS_CONT_ID is not null
         )
         group by BIS_SALES_DAY.BIS_PROJECT_ID, substr(SALES_DATE, 0, 10), BIS_CONT.store_type
     ) t
group by t.BIS_PROJECT_ID, t.store_type;


SELECT DISTINCT effect_flg
FROM ODS.ods_pl_powerdes_bis_cont_dt
WHERE effect_flg = 'Y'
   or effect_flg is null;

select bs_area.id,
       bs_area.area_name,  -- 区域名称
       bs_mall.area,
       bs_mall.out_mall_id -- 项目id
from ods.ods_pl_pms_bis_db_bs_area_dt bs_area
         left join ods.ods_pl_pms_bis_db_bs_mall_dt bs_mall
                   on bs_area.id = bs_mall.area
where bs_mall.is_del = '0'
  and stat = '2'
;


select BIS_MUST_ID
from ods.ods_pl_powerdes_bis_fact2_dt
where BIS_MUST_ID is null;

select bis_cont_id, effect_flg
from ods_pl_powerdes_bis_cont_dt
where effect_flg is null;

select BIS_FACT_ID, BIS_MUST_ID
from ods.ods_pl_powerdes_bis_fact2_dt
where BIS_FACT_ID = '402834704aba25f8014abac896770f25'
  and BIS_MUST_ID is null;

select effect_flg, count(1)
from ods.ods_pl_powerdes_bis_cont_dt
group by effect_flg;


select bis_cont.bis_project_id,             -- 项目id
       bis_shop_primary_forms.primary_forms -- 项目下的一级业态
from ods.ods_pl_powerdes_bis_cont_dt bis_cont
         left join ods.ods_pl_powerdes_bis_project_dt bis_project
                   on bis_cont.bis_project_id = bis_project.bis_project_id
         left join dwd.dwd_bis_shop_primary_forms_big_dt bis_shop_primary_forms
                   on bis_cont.bis_shop_id = bis_shop_primary_forms.bis_shop_id
where bis_shop_primary_forms.dt = date_format(current_date, 'YYYYMMDD')
  and bis_cont.effect_flg <> 'D'
  and bis_project.is_business_project = '1'
  and bis_project.oper_status = '2'
group by bis_cont.bis_project_id, bis_shop_primary_forms.primary_forms;



SELECT bis_project_id,
       short_name,
       primary_forms,
       query_date,
       SUM(month_total_money)
from dws.dws_bis_sales_project_broken_line_big_dt
WHERE dt = date_format(current_date, 'YYYYMMDD')
  and query_year = '2020'
  and primary_forms is not NULL
  and primary_forms = '名品'
GROUP BY bis_project_id, short_name, primary_forms, query_date;


select *
from dws.dws_bis_rent_mgr_multi_current_month_big_dt
where total_qz_must_money > 0
   or total_qz_fact_money > 0
   or total_cont_fact_money > 0
   or total_cont_must_money > 0;



select bis_shop_id, bis_project_id, sum(owe_qz)
from dws.dws_bis_rent_mgr_arrearage_big_dt
where bis_shop_id = '8a7b868b636e2e6001638b6abfe905bd'
  and bis_project_id = 'qpeq_4028347027a51b8f0127b336b2a30002'
  and fee_type = '1'
group by bis_shop_id, bis_project_id;


-- 一条应收有可能对应多个减免
select BIS_PROJECT_ID,
       qz_year_month,                 -- 权责月
       fee_type,                      -- 费项
       sum(adjust_money) adjust_money -- 减免金额

from dwd.dwd_bis_derate_basics_big_dt bis_mf_adjust
where FEE_TYPE = '1'
  and QZ_YEAR_MONTH like '2021%'
  and BIS_PROJECT_ID = '6D7E1C7AFAFB43E986670A81CF441231'
  and bis_mf_adjust.dt = date_format(current_date, 'YYYYMMdd')
group by BIS_PROJECT_ID, bis_mf_adjust.qz_year_month, bis_mf_adjust.fee_type;



select bis_project_id,
       qz_year_month,
       fact_type,
       fact_money
from dwd.dwd_bis_fact_basic_big_dt
where bis_project_id = '6D7E1C7AFAFB43E986670A81CF441231'
  and qz_year_month = '2021-04'
  and fact_type = '1'
  and dt = date_format(current_date, 'YYYYMMdd')



select table1.bis_project_id,                     -- 项目id
       table1.PROJECT_NAME,                       -- 项目名称
       fct.bis_fact_id,                           -- 实收id
       fct.bis_must_id,                           -- 应收id
       table1.bis_cont_id,                        -- 合同id
       table1.BIS_SHOP_ID,                        -- 商家id(品牌)
       table1.NAME_CN,                            -- 商家名称
       table1.store_type,                         -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
       fct.qz_year_month,                         -- 实收权责月
       fct.fact_type,                             -- 费项类型
       PMS_FIN_CHARGE_ITEM_DICT.CHARGE_ITEM_NAME, -- 费项中文名称
       PMS_FIN_CHARGE_ITEM_DICT.CHARGE_ITEM_TYPE, -- 收费类型
       fct.fact_money,                            -- 实收金额
       fct.fact_date,                             -- 实收日期
       fct.MUST_YEAR_MONTH,                       -- 应收日期
       fct.billing_period_begin,                  -- 账期开始时间
       fct.billing_period_end                     -- 账期结束时间
from
    -- 合同表
    (
        select BIS_CONT.bis_project_id,
               BIS_CONT.bis_cont_id,
               BIS_CONT.bis_shop_id,
               bis_cont.STORE_TYPE,                            -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
               case
                   when bis_cont.bis_shop_name is null then bis_shop.NAME_CN
                   else bis_cont.bis_shop_name end as NAME_CN, -- 结果表取合同表中品牌名称，如合同表中品牌名称为空取品牌表中名称，  如全部为空  及为空
               bis_project.PROJECT_NAME
        from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                 left join ods.ods_pl_powerdes_bis_project_dt bis_project
                           on BIS_CONT.BIS_PROJECT_ID = BIS_PROJECT.BIS_PROJECT_ID
                 left join ods.ods_pl_powerdes_bis_shop_dt bis_shop on BIS_CONT.BIS_SHOP_ID = BIS_SHOP.BIS_SHOP_ID
    ) table1
        inner join
    -- 实收费用表
        (select bis_cont_id,
                bis_must_id,
                bis_fact_id,
                qz_year_month,
                fact_type,
                billing_period_begin,
                billing_period_end,
                money     fact_money,
                fact_date fact_date,
                must_year_month
         from ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
         where is_delete = '1' -- 实收表is_delete = '1'为有效
           and fact_type is not null
        ) fct
    on table1.bis_cont_id = fct.bis_cont_id
        left join ods.ods_pl_powerdes_pms_fin_charge_item_dict_dt PMS_FIN_CHARGE_ITEM_DICT
                  on fct.fact_type = PMS_FIN_CHARGE_ITEM_DICT.CHARGE_ITEM_CODE
where PMS_FIN_CHARGE_ITEM_DICT.IS_DEL = '0'
  and table1.BIS_PROJECT_ID = '6D7E1C7AFAFB43E986670A81CF441231'
  and fact_type = '1'
  and QZ_YEAR_MONTH = '2021-04';


-- 权责 租金、物管收入-当月
select t1.bis_project_id,     -- 项目id
       t1.year_month1,        -- 年月 2021-12
       sum(t1.must_money_qz), -- 权责月应收金额
       sum(t1.fact_money_qz)  -- 权责月实收金额

from (
         -- 权责口径--每月应收 和 实收
         select t.bis_project_id,                -- 项目id
                t.short_name,                    -- 项目名称
                t.year,                          -- 年
                t.month,                         -- 月
                t.year_month1,                   -- 年月 2021-12
                t.store_type,                    -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                t.must_type,                     -- 费项（1：租金 2：物管）
                sum(t.must_money) must_money_qz, -- 月权责应收金额
                sum(t.fact_money) fact_money_qz  -- 月权责实收金额
         from (
                  select t1.bis_project_id,         -- 项目id
                         t1.short_name,             -- 项目名称
                         t1.year,                   -- 年
                         t1.month,                  -- 月
                         t1.year_month1,            -- 年月 2021-12
                         bis_must_basic.must_type,  -- 费项（1：租金 2：物管）
                         bis_must_basic.store_type, -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
                         bis_must_basic.must_money, -- 应收金额
                         bis_fact_basic.fact_money  -- 实收金额


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
                             and bis_project_id = '6D7E1C7AFAFB43E986670A81CF441231'
                             and year = '2021'
                             and oper_status = '2'
                       ) t1
                           left join
                       (
                           -- 应收
                           select bis_project_id,
                                  qz_year_month,
                                  store_type,
                                  must_type,
                                  sum(must_money) must_money
                           from dwd.dwd_bis_must_basic_big_dt bis_must_basic
                           where bis_must_basic.must_type in ('1')
                             and bis_must_basic.dt = date_format(current_date, 'YYYYMMdd')
                           group by bis_project_id, qz_year_month, store_type, must_type
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
                                  sum(fact_money) fact_money
                           from dwd.dwd_bis_fact_basic_big_dt bis_fact_basic
                           where bis_fact_basic.fact_type in ('1')
                             and bis_fact_basic.dt = date_format(current_date, 'YYYYMMdd')
                           group by bis_project_id, store_type, qz_year_month, fact_type
                       ) bis_fact_basic
                       on bis_must_basic.bis_project_id = bis_fact_basic.bis_project_id
                           and bis_must_basic.qz_year_month = bis_fact_basic.qz_year_month
                           and bis_must_basic.must_type = bis_fact_basic.fact_type
                           and bis_must_basic.store_type = bis_fact_basic.store_type
              ) t
         group by t.bis_project_id, t.short_name, t.year_month1, t.year, t.month, t.store_type, t.must_type
     ) t1
group by t1.bis_project_id, t1.year_month1;


-- 权责实收
select bis_project_id,
       store_type,
       qz_year_month,
       fact_type,
       round(sum(fact_money), 2) fact_money,
       round(sum(round(sum(fact_money), 2))
                 over (partition by bis_project_id, substr(qz_year_month, 0, 4),store_type, fact_type order by bis_project_id,qz_year_month,store_type,fact_type),
             2),
       round(sum(round(sum(fact_money), 2))
                 over (partition by bis_project_id, substr(qz_year_month, 0, 4),store_type, fact_type order by bis_project_id, substr(qz_year_month, 0, 4),store_type, fact_type),
             2)
from dwd.dwd_bis_fact_basic_big_dt bis_fact_basic
where bis_fact_basic.fact_type in ('1', '2')
  and bis_fact_basic.dt = date_format(current_date, 'YYYYMMdd')
  and fact_date <= last_day(concat(qz_year_month, '-01')) -- 权责月最后一天
  and bis_project_id = '6D7E1C7AFAFB43E986670A81CF441231'
group by bis_project_id, store_type, qz_year_month, fact_type;


-- 权责应收
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
where bis_must_basic.must_type in ('1', '2')
  and bis_must_basic.dt = date_format(current_date, 'YYYYMMdd')
group by bis_project_id, qz_year_month, store_type, must_type;


-- 应收
select bis_project_id,
       qz_year_month,
       store_type,
       must_type,
       round(sum(must_money), 2) must_money
from dwd.dwd_bis_must_qz_basic_big_dt bis_must_basic
where bis_must_basic.must_type in ('1', '2')
  and bis_must_basic.dt = date_format(current_date, 'YYYYMMdd')
group by bis_project_id, qz_year_month, substr(qz_year_month, 0, 4), store_type, must_type;


-- 合同口径 实收
select bis_project_id,
       store_type,
       substr(must_year_month, 0, 7) MUST_DATE,
       fact_type,
       round(sum(fact_money), 2)     fact_money,
       round(sum(round(sum(fact_money), 2))
                 over (partition by bis_project_id, substr(must_year_month, 0, 4),store_type, fact_type order by bis_project_id,substr(must_year_month, 0, 7),store_type,fact_type),
             2)                      total_cont_month_fact_money,
       round(sum(round(sum(fact_money), 2))
                 over (partition by bis_project_id, substr(must_year_month, 0, 4),store_type, fact_type order by bis_project_id, substr(must_year_month, 0, 4),store_type, fact_type),
             2)                      total_cont_year_fact_money
from dwd.dwd_bis_fact_basic_big_dt bis_fact_basic
where bis_fact_basic.fact_type in ('1', '2')
  and bis_fact_basic.dt = date_format(current_date, 'YYYYMMdd')
  and fact_date <= last_day(must_year_month) -- 权责月最后一天
group by bis_project_id, store_type, substr(must_year_month, 0, 7), substr(must_year_month, 0, 4), fact_type;


-- 权责欠费（权责月）
select t1.bis_project_id,                                                   -- 项目id
       t1.short_name,                                                       -- 项目名称
       bis_must_basic.bis_cont_id,                                          -- 合同id
       bis_must_basic.must_type,                                            -- 费项（1：租金 2：物管）
       sum(bis_must_basic.must_money - bis_fact_basic.fact_money) as owe_qz -- 欠费（权责月）

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
           and bis_project_id = '6D7E1C7AFAFB43E986670A81CF441231'
     ) t1
         left join dwd.dwd_bis_must_qz_basic_big_dt bis_must_basic
                   on t1.bis_project_id = bis_must_basic.bis_project_id
                       and t1.year_month1 = bis_must_basic.qz_year_month
         left join dwd.dwd_bis_fact_basic_big_dt bis_fact_basic
                   on bis_must_basic.bis_project_id = bis_fact_basic.bis_project_id
                       and bis_must_basic.qz_year_month = bis_fact_basic.qz_year_month
                       and bis_must_basic.must_type = bis_fact_basic.fact_type
                       and bis_must_basic.bis_cont_id = bis_fact_basic.bis_cont_id
                       and bis_fact_basic.fact_type in ('1', '2')
where bis_must_basic.must_type in ('1', '2')
  and bis_must_basic.dt = date_format(current_date, 'YYYYMMdd')
  and bis_fact_basic.dt = date_format(current_date, 'YYYYMMdd')
group by t1.bis_project_id, t1.short_name, bis_must_basic.bis_cont_id,
         bis_must_basic.must_type;

select t.bis_project_id,
       t.bis_cont_id,
       t.bis_shop_id,
       primary_forms
from (
         select BIS_CONT.bis_project_id,
                BIS_CONT.bis_cont_id,
                BIS_CONT.bis_shop_id,
                case
                    when BIS_SHOP_SORT_NEW.SORT_TYPE = 1 then BIS_SHOP_SORT_NEW.SORT_NAME
                    else null end as primary_forms -- 一级业态
         from ods.ods_pl_powerdes_bis_cont_dt bis_cont
                  left join ods.ods_pl_powerdes_bis_shop_dt bis_shop
                            on BIS_CONT.BIS_SHOP_ID = BIS_SHOP.BIS_SHOP_ID
                  left join ods.ods_pl_powerdes_bis_shop_sort_new_rel_dt BIS_SHOP_SORT_NEW_REL
                            on BIS_SHOP.BIS_SHOP_ID = BIS_SHOP_SORT_NEW_REL.BIS_SHOP_ID
                  left join ods.ods_pl_powerdes_bis_shop_sort_new_dt BIS_SHOP_SORT_NEW
                            on BIS_SHOP_SORT_NEW_REL.BIS_SHOP_SORT_ID = BIS_SHOP_SORT_NEW.BIS_SHOP_SORT_ID
         where BIS_SHOP.delete_bl <> '1'
           and BIS_SHOP.IS_NEW = '1'
     ) t
where t.primary_forms is not null
group by t.bis_project_id, t.bis_cont_id, t.bis_shop_id, t.primary_forms
having count(t.bis_cont_id) > 1;



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
  and dt = '20210403'
group by bis_project_id, query_date;


select bis_project.project_name, -- 项目名称
       d.bis_project_id,         -- 项目id
       d.bis_cont_id,            -- 合同id
       d.MUST_TYPE,              -- 费项
       sum(d.oweMoney)           -- 欠费
from (
         select must.bis_project_id,
                must.bis_cont_id,
                must.qz_year_month,
                must.MUST_TYPE,
                sum(nvl(mustMoney, 0)) + sum(nvl(adjMoney, 0)) -
                sum(nvl(factMoney, 0)) as oweMoney
         from (
                  -- 应收
                  SELECT BIS_MUST2.bis_project_id,
                         BIS_MUST2.bis_cont_id,
                         BIS_MUST2.qz_year_month,
                         BIS_MUST2.MUST_TYPE,
                         sum(nvl(BIS_MUST2.money, 0)) as mustMoney
                  FROM ods.ods_pl_powerdes_bis_must2_dt BIS_MUST2
                  where BIS_MUST2.must_type in ('1', '2')
                    and BIS_MUST2.is_show = 1
                    and BIS_MUST2.is_delete = 0
                    and BIS_MUST2.qz_year_month <= substr(CURRENT_DATE, 0, 7)
                  group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                           BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE
              ) must
                  left join
              (
                  -- 减免
                  SELECT bis_mf_adjust.bis_cont_id,
                         bis_mf_adjust.qz_year_month,
                         bis_mf_adjust.FEE_TYPE,
                         sum(nvl(bis_mf_adjust.adjust_money, 0)) as adjMoney
                  FROM ods.ods_pl_powerdes_bis_mf_adjust_dt bis_mf_adjust
                  where bis_mf_adjust.is_del = 1
                    and bis_mf_adjust.fee_type in ('1', '2')
                  group by bis_mf_adjust.bis_cont_id,
                           bis_mf_adjust.qz_year_month,
                           bis_mf_adjust.FEE_TYPE
              ) adj
              on must.bis_cont_id = adj.bis_cont_id and
                 must.qz_year_month = adj.qz_year_month and
                 must.MUST_TYPE = adj.FEE_TYPE
                  left join
              (
                  -- 实收
                  SELECT bis_fact2.bis_cont_id,
                         bis_fact2.qz_year_month,
                         bis_fact2.FACT_TYPE,
                         sum(nvl(bis_fact2.money, 0)) as factMoney
                  FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
                  where bis_fact2.is_delete = 1
                    and bis_fact2.fact_type in ('1', '2')
                  group by bis_fact2.bis_cont_id, bis_fact2.qz_year_month,
                           bis_fact2.FACT_TYPE
              ) fact
              on must.bis_cont_id = fact.bis_cont_id and
                 must.qz_year_month = fact.qz_year_month and
                 must.MUST_TYPE = fact.FACT_TYPE
         group by must.bis_project_id, must.bis_cont_id, must.qz_year_month,
                  must.MUST_TYPE
     ) d
         left join ods.ods_pl_powerdes_bis_project_dt bis_project
                   on d.bis_project_id = bis_project.bis_project_id
where bis_cont_id = '8a7b868b6ad3172b016ad354ccc101cb'
group by bis_project.project_name, d.bis_project_id, d.bis_cont_id, d.MUST_TYPE;



select bis_cont_id, store_position
from dwd.dwd_bis_cont_basics_big_dt
where bis_project_id = 'f730d638659e436ead80f37ee77bb56a'



select bis_must_basic.bis_project_id,                            -- 项目id
       bis_must_basic.qz_year_month,                             -- 租金应收权责月
       sum(nvl(must_money, 0))        as rent_money,             -- 每个月的租金权责应收总金额(不区分小商铺和主力店)
       sum(nvl(case
                   when bis_store_basics.store_position > 3 then must_money
                   else null end, 0)) as small_store_rent_money, -- 月租金（小商铺）
       sum(nvl(case
                   when bis_store_basics.store_position <= 3 then must_money
                   else null end, 0)) as main_store_rent_money   -- 月租金（主力店）
from dwd.dwd_bis_must_qz_basic_big_dt bis_must_basic
         left join dwd.dwd_bis_cont_basics_big_dt bis_store_basics
                   on bis_must_basic.bis_project_id = bis_store_basics.bis_project_id and
                      bis_must_basic.bis_cont_id = bis_store_basics.bis_cont_id and
                      bis_store_basics.dt = date_format(current_date, 'YYYYMMdd')
where bis_must_basic.dt = date_format(current_date, 'YYYYMMdd')
  and bis_must_basic.must_type = '1'
  and bis_store_basics.store_position is not null
  and bis_store_basics.store_type IN ('1', '2')
  and bis_must_basic.bis_project_id = 'f730d638659e436ead80f37ee77bb56a'
group by bis_must_basic.bis_project_id, bis_must_basic.qz_year_month
order by bis_must_basic.qz_year_month;



select bis_must_basic.bis_project_id,                            -- 项目id
       bis_must_basic.qz_year_month,                             -- 租金应收权责月
       sum(nvl(must_money, 0))        as rent_money,             -- 每个月的租金权责应收总金额(不区分小商铺和主力店)
       sum(nvl(case
                   when bis_store_basics.store_position > 3 then must_money
                   else null end, 0)) as small_store_rent_money, -- 月租金（小商铺）
       sum(nvl(case
                   when bis_store_basics.store_position <= 3 then must_money
                   else null end, 0)) as main_store_rent_money   -- 月租金（主力店）
from dwd.dwd_bis_must_qz_basic_big_dt bis_must_basic
         left join
     (
         select *
         from dwd.dwd_bis_cont_basics_big_dt
         where store_position is not null
           and dt = date_format(current_date, 'YYYYMMdd')
     ) bis_store_basics
     on bis_must_basic.bis_project_id = bis_store_basics.bis_project_id and
        bis_must_basic.bis_cont_id = bis_store_basics.bis_cont_id
where bis_must_basic.dt = date_format(current_date, 'YYYYMMdd')
  and bis_must_basic.must_type = '1'
  and bis_store_basics.store_type IN ('1', '2')
  and bis_must_basic.bis_project_id = 'f730d638659e436ead80f37ee77bb56a'
group by bis_must_basic.bis_project_id, bis_must_basic.store_type, bis_must_basic.qz_year_month
order by bis_must_basic.qz_year_month;

select BIS_PROJECT_ID,
       store_type,
       max(t.yesterday_total_money)        yesterday_total_money,
       max(t.before_yesterday_total_money) before_yesterday_total_money
from (
         select BIS_SALES_DAY.BIS_PROJECT_ID,
                case
                    when substr(SALES_DATE, 0, 10) = date_sub(current_date, 1) then sum(SALES_MONEY)
                    else null end as      yesterday_total_money,
                case
                    when substr(SALES_DATE, 0, 10) = date_sub(current_date, 2) then sum(SALES_MONEY)
                    else null end as      before_yesterday_total_money,
                substr(SALES_DATE, 0, 10) SALES_DATE,
                bis_cont.store_type

         from ods.ods_pl_powerdes_bis_sales_day_dt BIS_SALES_DAY
                  left join ods.ods_pl_powerdes_bis_cont_dt BIS_CONT
                            on BIS_CONT.BIS_CONT_ID = BIS_SALES_DAY.BIS_CONT_ID and BIS_CONT.EFFECT_FLG <> 'D'
         where (substr(SALES_DATE, 0, 10) = date_sub(current_date, 1) or
                substr(SALES_DATE, 0, 10) = date_sub(current_date, 2))
           AND BIS_SALES_DAY.BIS_CONT_ID IN (
             -- 考核铺位
             SELECT BIS_CONT.BIS_CONT_ID
             FROM ods.ods_pl_powerdes_bis_store_dt bis_store
                      inner join ods.ods_pl_powerdes_bis_cont_dt bis_cont
                                 on BIS_STORE.BIS_STORE_ID = BIS_CONT.BIS_STORE_IDS and
                                    BIS_CONT.EFFECT_FLG <> 'D'
             where bis_store.is_delete = 'N'
               -- /*考核商铺*/
               and (bis_store.IS_ASSESS = 'Y' OR bis_store.is_assess is null)
               and bis_store.status_cd = '1' -- 有效铺位
         )
         group by BIS_SALES_DAY.BIS_PROJECT_ID, substr(SALES_DATE, 0, 10), BIS_CONT.store_type
     ) t
where t.bis_project_id = '40282b8927a42dff0127a432c1645678901'
group by t.BIS_PROJECT_ID, t.store_type;


select bis_project_id,
       SHORT_NAME,
       NUM_YEAR,             -- 年
       STR_DATE,             -- 日
       sum(IN_FLOW) day_flow -- 日总进客流
from ods.ods_pl_powerdes_bis_traffic_flow_dt BIS_TRAFFIC_FLOW
where (str_date = date_format(current_date(), 'yyyyMMdd') or
       str_date = replace(date_sub(date_format(current_date(), 'yyyy-MM-dd'), 1), '-', '')
    or str_date = replace(date_sub(date_format(current_date(), 'yyyy-MM-dd'), 2), '-', '')
          )
      -- and SHORT_NAME = '安溪'
group by bis_project_id, SHORT_NAME, NUM_YEAR, STR_DATE;



select BIS_SHOP.BIS_SHOP_ID,                       -- 商家id
       max(case
               when BIS_SHOP_SORT_NEW.SORT_TYPE = 1 then BIS_SHOP_SORT_NEW.SORT_NAME
               else null end) as primary_forms,    -- 一级业态
       max(BIS_SHOP.NAME_CN)     cooperative_brand -- 品牌名称
from ods.ods_pl_powerdes_bis_shop_dt BIS_SHOP
         left join ods.ods_pl_powerdes_bis_shop_sort_new_rel_dt BIS_SHOP_SORT_NEW_REL
                   on BIS_SHOP.BIS_SHOP_ID = BIS_SHOP_SORT_NEW_REL.BIS_SHOP_ID
         left join ods.ods_pl_powerdes_bis_shop_sort_new_dt BIS_SHOP_SORT_NEW
                   on BIS_SHOP_SORT_NEW_REL.BIS_SHOP_SORT_ID = BIS_SHOP_SORT_NEW.BIS_SHOP_SORT_ID
where BIS_SHOP.delete_bl <> '1'
  and BIS_SHOP.IS_NEW = '1'
  and BIS_SHOP.bis_shop_id = '8a7b868b6c1eefae016c217083a37082'
group by BIS_SHOP.BIS_SHOP_ID;


-- 老业态
select BIS_SHOP.BIS_SHOP_ID,                       --商家id
       max(case
               when BIS_SHOP_SORT.SORT_TYPE = 1 then BIS_SHOP_SORT.SORT_NAME
               else null end) as primary_forms,    -- 一级业态
       max(BIS_SHOP.NAME_CN)     cooperative_brand -- 品牌名称

from ods.ods_pl_powerdes_bis_shop_dt BIS_SHOP
         left join ods.ods_pl_powerdes_bis_shop_sort_rel_dt BIS_SHOP_SORT_REL
                   on BIS_SHOP.BIS_SHOP_ID = BIS_SHOP_SORT_REL.BIS_SHOP_ID
         left join ods.ods_pl_powerdes_bis_shop_sort_dt BIS_SHOP_SORT
                   on BIS_SHOP_SORT.BIS_SHOP_SORT_ID = BIS_SHOP_SORT_REL.BIS_SHOP_SORT_ID
where BIS_SHOP.delete_bl <> '1'
  and (BIS_SHOP.IS_NEW is null or BIS_SHOP.IS_NEW = '0')
  and BIS_SHOP_SORT.SORT_TYPE <> '0'
  and BIS_SHOP_SORT.SORT_TYPE is not null
  and BIS_SHOP.bis_shop_id = '8a7b868b6c1eefae016c217083a37082'
group by BIS_SHOP.BIS_SHOP_ID;



select bis_project_id,
       qz_year_month,
       sum(factMoney),
       sum(fact_money2)
from (

-- 实收
         select bis_project_id,

                qz_year_month,
                fact_type,
                sum(nvl(money, 0)) as factMoney,
                round(sum(money), 2)  fact_money2

         from ods.ods_pl_powerdes_bis_fact2_dt bis_fact_basic
         where bis_fact_basic.fact_type in ('1')
           and is_delete = '1'
           and bis_fact_basic.dt = date_format(current_date, 'YYYYMMdd')
           and substr(fact_date, 0, 10) <= last_day(concat(qz_year_month, '-01')) -- 权责月最后一天
         group by bis_project_id, qz_year_month, substr(qz_year_month, 0, 4), fact_type
     ) t
where bis_project_id = '402834702ec49066012ed7a1aac1225e'
  and qz_year_month like '2021%'
  and fact_type = '1'
group by bis_project_id, qz_year_month;



/*
申请租金

固定  实际单价-年租金||实际单价-年租金     RENT_TYPE=1

固定抽成 月销售额-扣率-年租金||月销售额-扣率-年租金   RENT_TYPE=2 && PUMPING_TYPE != 2

阶梯抽成 月销售额-[阶梯开始~阶梯结束 扣率,阶梯开始~阶梯结束 扣率]-年租金||月销售额-[阶梯开始~阶梯结束 扣率,阶梯开始~阶梯结束 扣率]-年租金    RENT_TYPE=2 && PUMPING_TYPE=2

两者取高 （新数据）月销售额-[阶梯开始~阶梯结束 扣率,阶梯开始~阶梯结束 扣率]-年租金-单价||月销售额-[阶梯开始~阶梯结束 扣率,阶梯开始~阶梯结束 扣率]-年租金-单价 RENT_TYPE=3


*/
-- 固定租金
select t1.res_approve_info_id,
       concat_ws(",", collect_list(cast(split(RENT_TYPE_1, '-')[0] as string))) rent_price -- 每期单价
from ods.ods_pl_powerdes_res_approve_rent_info_dt t1
         LATERAL VIEW explode(split(t1.PRICE_ACTUAL_INFO, '\\|\\|')) cfi as RENT_TYPE_1
where RENT_TYPE = 1 -- 租金方式：1,固定租金;2,提成租金;3,保底提成取高;4,其他方式
group by t1.res_approve_info_id

union all

-- 抽成：固定抽成
select t1.res_approve_info_id,
       concat_ws(",", collect_list(cast(split(RENT_TYPE_1, '-')[1] as string))) rent_price -- 每期扣率
from ods.ods_pl_powerdes_res_approve_rent_info_dt t1
         LATERAL VIEW explode(split(t1.PRICE_ACTUAL_INFO, '\\|\\|')) cfi as RENT_TYPE_1
where t1.RENT_TYPE = 2
  and nvl(t1.pumping_type, 1000) <> 1001 -- 租金方式：1,固定租金;2,提成租金;3,保底提成取高;4,其他方式
group by t1.res_approve_info_id


union all
-- 抽成：阶梯抽成


-- 两者取高 （旧数据）
select t1.res_approve_info_id,
       concat_ws(",", collect_list(cast(CONCAT_WS('/', split(RENT_TYPE_1, '-')[0],
                                                  concat(split(RENT_TYPE_1, '-')[1], '%')) as string))) rent_price --每期 单价/扣率
from ods.ods_pl_powerdes_res_approve_rent_info_dt t1
         LATERAL VIEW explode(split(t1.PRICE_ACTUAL_INFO, '\\|\\|')) cfi as RENT_TYPE_1
where t1.RENT_TYPE = 3
  and res_approve_info_id = '8a7b859c6bfe497f016bfe572c080397'-- 租金方式：1,固定租金;2,提成租金;3,保底提成取高;4,其他方式
group by t1.res_approve_info_id;

-- 两者取高 （新数据）


/*
    申请物管

    固定  实际单价-年物管||实际单价-年物管

*/


select t1.res_approve_info_id,
       concat_ws(",", collect_list(cast(split(RENT_TYPE_1, '-')[0] as string))) -- 每期扣率
from ods.ods_pl_powerdes_res_approve_rent_info_dt t1
         LATERAL VIEW explode(split(t1.PROP_PRICE_ACTUAL, '\\|\\|')) cfi as RENT_TYPE_1
where res_approve_info_id = '8a7b859e6e36b3ba016e39a975e26e81'
group by t1.res_approve_info_id;


select bis_cont.bis_cont_id, -- 合同id
       t.rent_price          -- 租金单价
from (
         -- 固定租金
         select t1.res_approve_info_id,
                concat_ws(",", collect_list(cast(split(RENT_TYPE_1, '-')[0] as string))) rent_price -- 每期单价
         from ods.ods_pl_powerdes_res_approve_rent_info_dt t1
                  LATERAL VIEW explode(split(t1.PRICE_ACTUAL_INFO, '\\|\\|')) cfi as RENT_TYPE_1
         where RENT_TYPE = 1
         group by t1.res_approve_info_id

         union all

         -- 抽成：固定抽成
         select t1.res_approve_info_id,
                concat_ws(",", collect_list(cast(split(RENT_TYPE_1, '-')[1] as string))) rent_price -- 每期扣率
         from ods.ods_pl_powerdes_res_approve_rent_info_dt t1
                  LATERAL VIEW explode(split(t1.PRICE_ACTUAL_INFO, '\\|\\|')) cfi as RENT_TYPE_1
         where t1.RENT_TYPE = 2
           and nvl(t1.pumping_type, 1000) = 1001
         group by t1.res_approve_info_id


         union all
         -- 抽成：阶梯抽成


         -- 两者取高 （旧数据）
         select t1.res_approve_info_id,
                concat_ws(",", collect_list(cast(CONCAT_WS('/', split(RENT_TYPE_1, '-')[0],
                                                           concat(split(RENT_TYPE_1, '-')[1], '%')) as string))) rent_price --每期 单价/扣率
         from ods.ods_pl_powerdes_res_approve_rent_info_dt t1
                  LATERAL VIEW explode(split(t1.PRICE_ACTUAL_INFO, '\\|\\|')) cfi as RENT_TYPE_1
         where t1.RENT_TYPE = 3
         group by t1.res_approve_info_id

         -- 两者取高 （新数据）
     ) t
         LEFT JOIN ods.ods_pl_powerdes_bis_cont_dt bis_cont
                   ON bis_cont.res_approve_id1 = t.res_approve_info_id and bis_cont.effect_flg = 'Y';


select *
from dws.dws_bis_headquarters_brand_monitoring_detail_big_dt
where bis_shop_id = '402834703319f14d01331a499ff5075c'
  and store_no = 'M1-L1-080';



select t3.bis_shop_id,
       t3.primary_forms,          -- 一级业态
       t3.secondary_formats,      -- 二级业态
       t3.thirdly_formats,        -- 三级业态
       t2.primary_forms_code,     -- 一级业态code
       t3.secondary_formats_code, -- 二级业态code
       t3.thirdly_formats_code,   -- 三级业态code
       t3.cooperative_brand,      -- 商家名称
       t3.company_name,           -- 品牌集团名称
       t3.bis_shop_status         -- 商家状态
from (
         select t1.bis_shop_id,
                case
                    when t1.primary_forms = '百货' then '次主力店'
                    when t1.primary_forms = '名品' then '服装'
                    when t1.primary_forms like '%配套-配套集合店%' then '零售配套'
                    when t1.primary_forms like '%配套-鞋包/珠宝配饰/化妆品/精品%' then '零售配套'
                    when t1.primary_forms like '%配套-文娱类配套-文具%' then '零售配套'
                    when t1.primary_forms like '%配套-文娱类配套-音像%' then '零售配套'
                    when t1.primary_forms like '%配套-文娱类配套-礼品精品%' then '零售配套'
                    when t1.primary_forms like '%配套-文娱类配套-艺术品%' then '零售配套'
                    when t1.primary_forms like '%配套-文娱类配套-书店%' then '生活配套'
                    when t1.primary_forms like '%配套-休闲娱乐%' then '生活配套'
                    when t1.primary_forms like '%配套-生活配套/服务%' then '生活配套'
                    else t1.primary_forms end as primary_forms, -- 一级业态
                t1.secondary_formats,                           -- 二级业态
                t1.thirdly_formats,                             -- 三级业态
                t1.primary_forms_code,                          -- 一级业态code
                t1.secondary_formats_code,                      -- 二级业态code
                t1.thirdly_formats_code,                        -- 三级业态code
                t1.cooperative_brand,                           -- 商家名称
                t1.company_name,                                -- 品牌集团名称
                t1.bis_shop_status                              -- 商家状态
         from (
                  -- 老业态
                  select bis_shop_id,
                         if(primary_forms = '配套' and
                            secondary_formats in ('配套集合店', '鞋包/珠宝配饰/化妆品/精品', '文娱类配套', '休闲娱乐', '生活配套/服务'),
                            concat_ws('-', primary_forms, secondary_formats, thirdly_formats),
                            primary_forms) primary_forms,
                         secondary_formats,
                         thirdly_formats,
                         primary_forms_code,
                         secondary_formats_code,
                         thirdly_formats_code,
                         cooperative_brand,
                         company_name,
                         bis_shop_status
                  from (-- 老业态
                           select BIS_SHOP.BIS_SHOP_ID,                             --商家id
                                  max(case
                                          when BIS_SHOP_SORT.SORT_TYPE = 1 then BIS_SHOP_SORT.SORT_NAME
                                          else null end) as primary_forms,          -- 一级业态
                                  max(case
                                          when BIS_SHOP_SORT.SORT_TYPE = 2 then BIS_SHOP_SORT.SORT_NAME
                                          else null end) as secondary_formats,      -- 二级业态
                                  max(case
                                          when BIS_SHOP_SORT.SORT_TYPE = 3 then BIS_SHOP_SORT.SORT_NAME
                                          else null end) as thirdly_formats,        -- 三级业态
                                  max(case
                                          when BIS_SHOP_SORT.SORT_TYPE = 1 then BIS_SHOP_SORT.layout_cd
                                          else null end) as primary_forms_code,     -- 一级业态code
                                  max(case
                                          when BIS_SHOP_SORT.SORT_TYPE = 2 then BIS_SHOP_SORT.layout_cd
                                          else null end) as secondary_formats_code, -- 二级业态code
                                  max(case
                                          when BIS_SHOP_SORT.SORT_TYPE = 3 then BIS_SHOP_SORT.layout_cd
                                          else null end) as thirdly_formats_code,   -- 三级业态code
                                  max(BIS_SHOP.NAME_CN)     cooperative_brand,      -- 商家名称
                                  max(COMPANY_NAME)         company_name,           -- 品牌集团名称
                                  max(case
                                          when bis_shop.is_new_shop = '1' then '有效'
                                          when bis_shop.is_new_shop = '0' then '无效'
                                          else null end)    bis_shop_status         -- 商家状态,

                           from ods.ods_pl_powerdes_bis_shop_dt BIS_SHOP
                                    left join ods.ods_pl_powerdes_bis_shop_sort_rel_dt BIS_SHOP_SORT_REL
                                              on BIS_SHOP.BIS_SHOP_ID = BIS_SHOP_SORT_REL.BIS_SHOP_ID
                                    left join ods.ods_pl_powerdes_bis_shop_sort_dt BIS_SHOP_SORT
                                              on BIS_SHOP_SORT.BIS_SHOP_SORT_ID = BIS_SHOP_SORT_REL.BIS_SHOP_SORT_ID
                           where (BIS_SHOP.IS_NEW is null or BIS_SHOP.IS_NEW = '0')
                             and BIS_SHOP_SORT.SORT_TYPE <> '0'
                             and BIS_SHOP_SORT.SORT_TYPE is not null
                           group by BIS_SHOP.BIS_SHOP_ID
                       ) t
              ) t1
     ) t3
         left join
     (
         -- 新业态
         select distinct primary_forms, primary_forms_code
         from (
                  select case
                             when BIS_SHOP_SORT_NEW.SORT_TYPE = 1 then BIS_SHOP_SORT_NEW.SORT_NAME
                             else null end as primary_forms,     -- 一级业态
                         case
                             when BIS_SHOP_SORT_NEW.SORT_TYPE = 1 then BIS_SHOP_SORT_NEW.layout_cd
                             else null end as primary_forms_code -- 一级业态code

                  from ods.ods_pl_powerdes_bis_shop_dt BIS_SHOP
                           left join ods.ods_pl_powerdes_bis_shop_sort_new_rel_dt BIS_SHOP_SORT_NEW_REL
                                     on BIS_SHOP.BIS_SHOP_ID = BIS_SHOP_SORT_NEW_REL.BIS_SHOP_ID
                           left join ods.ods_pl_powerdes_bis_shop_sort_new_dt BIS_SHOP_SORT_NEW
                                     on BIS_SHOP_SORT_NEW_REL.BIS_SHOP_SORT_ID = BIS_SHOP_SORT_NEW.BIS_SHOP_SORT_ID
                  where BIS_SHOP.IS_NEW = '1'
                    and BIS_SHOP_SORT_NEW.SORT_TYPE = 1
              ) t
     ) t2 on t3.primary_forms = t2.primary_forms;



select t1.bis_project_id,                        -- 项目id
       t1.short_name,                            -- 项目名称
       t1.year,                                  -- 年
       t1.month,                                 -- 月
       t1.year_month1,                           -- 年月 2021-12
       t1.must_type,                             -- 费项（1：租金 2：物管）
       t1.store_type,                            -- 物业类型（1：购物中心 2:商业街 3：住宅 4：住宅底商 5：写字楼）
       bis_must_basic.must_money,                -- 应收金额
       bis_must_basic.total_qz_month_must_money, -- 权责月月累计（月递归累计）应收金额
       bis_must_basic.total_qz_year_must_money   -- -- 权责月年累计应收金额

from (
         select distinct bis_project_id,
                         short_name,
                         year,
                         month,
                         year_month1,
                         store_type,
                         must_type
         from dwd.dwd_bis_cross_join_result_big_dt
         where dt = date_format(current_date(), 'yyyyMMdd')
     ) t1
         left join
     (
         -- 应收
         select bis_project_id,
                qz_year_month,
                store_type,
                must_type,
                round(sum(nvl(must_money, 0)), 2) must_money,
                round(sum(round(sum(nvl(must_money, 0)), 2))
                          over (partition by bis_project_id, substr(qz_year_month, 0, 4),store_type, must_type order by bis_project_id,qz_year_month,store_type,must_type),
                      2)                          total_qz_month_must_money, -- 权责月月累计（月递归累计）应收金额
                round(sum(round(sum(nvl(must_money, 0)), 2))
                          over (partition by bis_project_id, substr(qz_year_month, 0, 4),store_type, must_type order by bis_project_id, substr(qz_year_month, 0, 4),store_type, must_type),
                      2)                          total_qz_year_must_money   -- 权责月年累计应收金额
         from (
                  select t1.bis_project_id,
                         t1.year_month1 as     qz_year_month,
                         t1.store_type,
                         nvl(t2.must_money, 0) must_money,
                         t1.must_type
                  from (
                           select distinct bis_project_id,
                                           short_name,
                                           year,
                                           month,
                                           year_month1,
                                           store_type,
                                           must_type
                           from dwd.dwd_bis_cross_join_result_big_dt
                           where dt = date_format(current_date(), 'yyyyMMdd')
                       ) t1
                           left join dwd.dwd_bis_must_qz_basic_big_dt t2
                                     on t1.bis_project_id = t2.bis_project_id and t1.year_month1 = t2.qz_year_month
                                         and t1.store_type = t2.store_type and t1.must_type = t2.must_type and
                                        t2.must_type in ('1', '2')
                                         and t2.dt = date_format(current_date, 'YYYYMMdd')
              ) bis_must_basic
         group by bis_project_id, qz_year_month, substr(qz_year_month, 0, 4), store_type, must_type
     ) bis_must_basic
     on t1.bis_project_id = bis_must_basic.bis_project_id
         and t1.year_month1 = bis_must_basic.qz_year_month and
        t1.store_type = bis_must_basic.store_type and
        t1.must_type = bis_must_basic.must_type
where t1.bis_project_id = '6D7E1C7AFAFB43E986670A81CF444253'
  and t1.year = '2021'
  and t1.must_type = '1'
  and t1.store_type = '2';


-- 应收
select bis_project_id,
       qz_year_month,
       store_type,
       must_type,
       round(sum(nvl(must_money, 0)), 2) must_money,
       round(sum(round(sum(nvl(must_money, 0)), 2))
                 over (partition by bis_project_id, substr(qz_year_month, 0, 4),store_type, must_type order by bis_project_id,qz_year_month,store_type,must_type),
             2)                          total_qz_month_must_money, -- 权责月月累计（月递归累计）应收金额
       round(sum(round(sum(nvl(must_money, 0)), 2))
                 over (partition by bis_project_id, substr(qz_year_month, 0, 4),store_type, must_type order by bis_project_id, substr(qz_year_month, 0, 4),store_type, must_type),
             2)                          total_qz_year_must_money   -- 权责月年累计应收金额
from (
         select t1.bis_project_id,
                t1.year_month1 as     qz_year_month,
                t1.store_type,
                nvl(t2.must_money, 0) must_money,
                t1.must_type
         from (
                  select distinct bis_project_id,
                                  short_name,
                                  year,
                                  month,
                                  year_month1,
                                  store_type,
                                  must_type
                  from dwd.dwd_bis_cross_join_result_big_dt
                  where dt = date_format(current_date(), 'yyyyMMdd')
              ) t1
                  left join dwd.dwd_bis_must_qz_basic_big_dt t2
                            on t1.bis_project_id = t2.bis_project_id and t1.year_month1 = t2.qz_year_month
                                and t1.store_type = t2.store_type and t1.must_type = t2.must_type and
                               t2.must_type in ('1', '2')
                                and t2.dt = date_format(current_date, 'YYYYMMdd')
     ) bis_must_basic
where bis_project_id = '6D7E1C7AFAFB43E986670A81CF444253'
  and qz_year_month like '2021%'
  and must_type = '1'
  and store_type = '2'
group by bis_project_id, qz_year_month, substr(qz_year_month, 0, 4), store_type, must_type;


-- 实收
select bis_project_id,
       store_type,
       qz_year_month,
       fact_type,
       round(sum(nvl(fact_money, 0)), 2) fact_money,
       null                              total_qz_month_fact_money, -- 权责月月累计（月递归累计）实收金额
       round(sum(round(sum(nvl(fact_money, 0)), 2))
                 over (partition by bis_project_id, substr(qz_year_month, 0, 4),store_type, fact_type order by bis_project_id, substr(qz_year_month, 0, 4),store_type, fact_type),
             2)                          total_qz_year_fact_money   -- 权责月年累计实收金额
from (
         select t1.bis_project_id,
                t1.short_name,
                t1.store_type,
                t1.year_month1 qz_year_month,
                t1.must_type   fact_type,
                t2.fact_money
         from (
                  select distinct bis_project_id,
                                  short_name,
                                  year,
                                  month,
                                  year_month1,
                                  store_type,
                                  must_type
                  from dwd.dwd_bis_cross_join_result_big_dt
                  where dt = date_format(current_date(), 'yyyyMMdd')
              ) t1
                  left join dwd.dwd_bis_fact_basic_big_dt t2
                            on t1.bis_project_id = t2.bis_project_id and t1.year_month1 = t2.qz_year_month
                                and t1.store_type = t2.store_type and t1.must_type = t2.fact_type and
                               t2.fact_type in ('1', '2')
                                and t2.dt = date_format(current_date, 'YYYYMMdd')
                                and substr(t2.fact_date, 0, 10) <= last_day(concat(t2.qz_year_month, '-01')) -- 权责月最后一天
     ) bis_fact_basic
where bis_project_id = '6D7E1C7AFAFB43E986670A81CF444253'
  and qz_year_month like '2021%'
  and fact_type = '1'
  and store_type = '2'
group by bis_project_id, store_type, qz_year_month, substr(qz_year_month, 0, 4), fact_type;


-- 新业态
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
from ods_pl_powerdes_bis_cont_dt t1
         LEFT JOIN ods_pl_powerdes_bis_shop_sort_new_dt t2 ON t1.PROPORTION_IDS = t2.BIS_SHOP_SORT_ID
         left join ods_pl_powerdes_bis_shop_sort_new_dt t3 ON t2.PARENT_ID = t3.BIS_SHOP_SORT_ID
         left join ods_pl_powerdes_bis_shop_sort_new_dt t4 ON t3.PARENT_ID = t4.BIS_SHOP_SORT_ID
where PROPORTION_IDS is not null
  and t4.sort_name is not null
  and t3.sort_name is not null
  and t2.sort_name is not null
  and bis_cont_id = '8a7b859c734b85d301734bff909b187f'

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
                    when t1.PROPORTION_NAMES = '百货' then '次主力店'
                    when t1.PROPORTION_NAMES = '名品' then '服装'
                    when t1.PROPORTION_NAMES like '%配套-配套集合店%' then '零售配套'
                    when t1.PROPORTION_NAMES like '%配套-鞋包/珠宝配饰/化妆品/精品%' then '零售配套'
                    when t1.PROPORTION_NAMES like '%配套-文娱类配套-文具%' then '零售配套'
                    when t1.PROPORTION_NAMES like '%配套-文娱类配套-音像%' then '零售配套'
                    when t1.PROPORTION_NAMES like '%配套-文娱类配套-礼品精品%' then '零售配套'
                    when t1.PROPORTION_NAMES like '%配套-文娱类配套-艺术品%' then '零售配套'
                    when t1.PROPORTION_NAMES like '%配套-文娱类配套-书店%' then '生活配套'
                    when t1.PROPORTION_NAMES like '%配套-休闲娱乐%' then '生活配套'
                    when t1.PROPORTION_NAMES like '%配套-生活配套/服务%' then '生活配套'
                    else t1.PROPORTION_NAMES end as primary_forms           -- 一级业态
         from ods_pl_powerdes_bis_cont_dt t1
                  LEFT JOIN ods_pl_powerdes_bis_shop_sort_dt t2 ON t1.PROPORTION_IDS = t2.BIS_SHOP_SORT_ID
                  left join ods_pl_powerdes_bis_shop_sort_dt t3 ON t2.PARENT_ID = t3.BIS_SHOP_SORT_ID
                  left join ods_pl_powerdes_bis_shop_sort_dt t4 ON t3.PARENT_ID = t4.BIS_SHOP_SORT_ID
         where PROPORTION_IDS is not null
           and t4.sort_name is not null
           and t3.sort_name is not null
           and t2.sort_name is not null
           and bis_cont_id = '8a7b859c734b85d301734bff909b187f'
     ) t1
         left join
     (
         -- 新业态
         select distinct t4.LAYOUT_CD primary_forms_code, -- 一级业态code
                         t4.SORT_NAME primary_forms       -- 一级业态
         from ods_pl_powerdes_bis_cont_dt t1
                  LEFT JOIN ods_pl_powerdes_bis_shop_sort_new_dt t2 ON t1.PROPORTION_IDS = t2.BIS_SHOP_SORT_ID
                  left join ods_pl_powerdes_bis_shop_sort_new_dt t3 ON t2.PARENT_ID = t3.BIS_SHOP_SORT_ID
                  left join ods_pl_powerdes_bis_shop_sort_new_dt t4 ON t3.PARENT_ID = t4.BIS_SHOP_SORT_ID
         where PROPORTION_IDS is not null
           and t4.LAYOUT_CD is not null
           and t4.SORT_NAME is not null
     ) t2 on t1.primary_forms = t2.primary_forms;



select bis_project.bis_project_id,
       s.annual, -- 年
       t.*
from ods.ods_pl_pms_budget_db_budget_instance_collect_target_dt t
         LEFT JOIN ods.ods_pl_pms_budget_db_budget_instance_sheet_dt s
                   on t.budget_instance_sheet_id = s.id
         left join ods.ods_pl_powerdes_bis_project_dt bis_project on s.project_id = bis_project.mall_id
where s.annual = '2021'
  and s.is_del = '0'
  and s.project_id = '105'
;



select BIS_SHOP.BIS_SHOP_ID,                             --商家id
       max(case
               when BIS_SHOP_SORT.SORT_TYPE = 1 then BIS_SHOP_SORT.SORT_NAME
               else null end) as primary_forms,          -- 一级业态
       max(case
               when BIS_SHOP_SORT.SORT_TYPE = 2 then BIS_SHOP_SORT.SORT_NAME
               else null end) as secondary_formats,      -- 二级业态
       max(case
               when BIS_SHOP_SORT.SORT_TYPE = 3 then BIS_SHOP_SORT.SORT_NAME
               else null end) as thirdly_formats,        -- 三级业态
       max(case
               when BIS_SHOP_SORT.SORT_TYPE = 1 then BIS_SHOP_SORT.layout_cd
               else null end) as primary_forms_code,     -- 一级业态code
       max(case
               when BIS_SHOP_SORT.SORT_TYPE = 2 then BIS_SHOP_SORT.layout_cd
               else null end) as secondary_formats_code, -- 二级业态code
       max(case
               when BIS_SHOP_SORT.SORT_TYPE = 3 then BIS_SHOP_SORT.layout_cd
               else null end) as thirdly_formats_code,   -- 三级业态code
       max(BIS_SHOP.NAME_CN)     cooperative_brand,      -- 商家名称
       max(COMPANY_NAME)         company_name,           -- 品牌集团名称
       max(case
               when bis_shop.is_new_shop = '1' then '有效'
               when bis_shop.is_new_shop = '0' then '无效'
               else null end)    bis_shop_status         -- 商家状态,

from ods.ods_pl_powerdes_bis_shop_dt BIS_SHOP
         left join ods.ods_pl_powerdes_bis_shop_sort_rel_dt BIS_SHOP_SORT_REL
                   on BIS_SHOP.BIS_SHOP_ID = BIS_SHOP_SORT_REL.BIS_SHOP_ID
         left join ods.ods_pl_powerdes_bis_shop_sort_dt BIS_SHOP_SORT
                   on BIS_SHOP_SORT.BIS_SHOP_SORT_ID = BIS_SHOP_SORT_REL.BIS_SHOP_SORT_ID
where (BIS_SHOP.IS_NEW is null or BIS_SHOP.IS_NEW = '0')
  and BIS_SHOP_SORT.SORT_TYPE <> '0'
  and BIS_SHOP_SORT.SORT_TYPE is not null
group by BIS_SHOP.BIS_SHOP_ID;

-- 多径类型（1：固定点位；2：临时点位；
-- 3：宣传点位；4：ATM；5：其他;
-- 6:DP点位；7：外摆点位；8：仓库点位；9：车展点位；10：房展点位,11：营销点位）


select project_id
from ods.ods_pl_bl_member_level_dt
where locate(" ", project_id) > 0;



select t2.BIS_PROJECT_ID,                   -- 项目id
       t2.MUST_TYPE,                        -- 费项
       t3.STORE_TYPE,                       -- 物业类型
       t2.must_year_month,                  -- 应收年月
       sum(nvl(t2.mustMoney, 0)) mustMoney, -- 月应收
       sum(nvl(t2.factMoney, 0)) factMoney  -- 月实收

from ods.ods_pl_powerdes_bis_cont_dt t3
         inner join
     (
         select table1.bis_project_id,  -- 项目id
                table1.bis_cont_id,     -- 合同id
                table1.MUST_TYPE,       -- 费项（1：租金 2：物管）
                table1.must_year_month, -- 应收月
                table1.factMoney,       -- 月实收
                table3.mustMoney        -- 月应收

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
                  group by t.bis_project_id, t.bis_cont_id,
                           substr(t.rent_last_pay_date, 0, 4),
                           substr(t.rent_last_pay_date, 0, 7), t.MUST_TYPE
              ) table3
              on table1.bis_project_id = table3.bis_project_id
                  and table1.bis_cont_id = table3.bis_cont_id
                  and table1.must_type = table3.must_type
                  and table1.must_year_month = table3.must_year_month
     ) t2 on t3.BIS_PROJECT_ID = t2.BIS_PROJECT_ID and t3.BIS_CONT_ID = t2.BIS_CONT_ID
where t2.BIS_PROJECT_ID = '6D7E1C7AFAFB43E986670A81CF132446'
  and t2.must_year_month = '2021-06'
group by t2.BIS_PROJECT_ID,
         t2.MUST_TYPE,
         t3.STORE_TYPE,
         t2.must_year_month;



select table1.bis_project_id,  -- 项目id
       table1.bis_cont_id,     -- 合同id
       table1.MUST_TYPE,       -- 费项（1：租金 2：物管）
       table1.must_year_month, -- 应收月
       table1.factMoney        -- 月实收


from (
         select t.bis_project_id,                                   -- 项目id
                t.bis_cont_id,                                      -- 合同id
                t.MUST_TYPE,                                        -- 费项（1：租金 2：物管）
                substr(t.rent_last_pay_date, 0, 7) must_year_month, -- 应收月
                substr(fact.fact_date, 0, 7),
                fact.factMoney


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
                  -- 减免
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
         where t.BIS_PROJECT_ID = '6D7E1C7AFAFB43E986670A81CF132446'
           and substr(t.rent_last_pay_date, 0, 7) = '2021-06'
         group by t.bis_project_id, t.bis_cont_id,
                  substr(t.rent_last_pay_date, 0, 4),
                  substr(t.rent_last_pay_date, 0, 7), t.MUST_TYPE
     ) table1
where table1.BIS_PROJECT_ID = '6D7E1C7AFAFB43E986670A81CF132446'
  and table1.must_year_month = '2021-06'



SELECT bis_fact2.bis_cont_id,
       bis_fact2.bis_must_id,
       bis_fact2.fact_type,
       substr(bis_fact2.must_rent_last_pay_date, 0, 7),
       sum(nvl(bis_fact2.money, 0)) as factMoney
FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
where bis_fact2.is_delete = 1
  and bis_fact2.fact_type in ('1')
  and bis_fact2.bis_must_id is not null
  and bis_fact2.BIS_PROJECT_ID = '6D7E1C7AFAFB43E986670A81CF132446'
  and substr(bis_fact2.must_rent_last_pay_date, 0, 7) = '2021-06'

group by bis_fact2.bis_cont_id, bis_fact2.bis_must_id,
         bis_fact2.fact_type,
         substr(bis_fact2.must_rent_last_pay_date, 0, 7);


-- 实收 12232151.58  -- 1089
SELECT bis_fact2.bis_cont_id,
       bis_fact2.bis_must_id,
       bis_fact2.fact_type,
       bis_fact2.fact_date,
       bis_fact2.must_rent_last_pay_date,
       bis_fact2.money as factMoney
FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
where bis_fact2.is_delete = 1
  and bis_fact2.fact_type in ('1')
  and bis_fact2.bis_must_id is not null
  and bis_fact2.BIS_PROJECT_ID = '6D7E1C7AFAFB43E986670A81CF132446'
  and substr(bis_fact2.must_rent_last_pay_date, 0, 7) = '2021-06';



select substr(t.sales_date, 0, 10), sum(SALES_MONEY), sum(tt), sum(assess_SALES_MONEY + no_assess_SALES_MONEY)
from (
         select BIS_SALES_DAY.BIS_PROJECT_ID,                                                             -- 项目id
                BIS_SALES_DAY.SALES_DATE,                                                                 -- 销售日期
                sum(SALES_MONEY)                                                   tt,
                sum(case when is_assess = 'Y' THEN NVL(SALES_MONEY, 0) ELSE 0 END) assess_SALES_MONEY,    -- 考核销售额
                sum(case when is_assess = 'N' THEN NVL(SALES_MONEY, 0) ELSE 0 END) no_assess_SALES_MONEY, -- 不考核销售额
                sum(case
                        when is_assess in ('Y', 'N') THEN NVL(SALES_MONEY, 0)
                        ELSE 0 END)                                                SALES_MONEY            -- （考核 + 不考核）销售额
         from dwd.dwd_bis_sales_day_big_dt BIS_SALES_DAY

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
         group by BIS_SALES_DAY.BIS_PROJECT_ID,
                  BIS_SALES_DAY.SALES_DATE, t9.is_assess
     ) t
where t.bis_project_id = '40282b8927a42dff0127a4347f2b00fc'
  and substr(t.sales_date, 0, 10) like "2022%"
group by substr(t.sales_date, 0, 10);



select d.MUST_TYPE,    -- 费项
       sum(d.oweMoney) -- 欠费
from (
         select t.bis_project_id,                                   -- 项目id
                t.bis_cont_id,                                      -- 合同id
                t.MUST_TYPE,                                        -- 费项（1：租金 2：物管）
                substr(t.rent_last_pay_date, 0, 7) must_year_month, -- 应收月
                sum(nvl(t.mustMoney, 0)) + sum(nvl(t1.adjMoney, 0)) -
                sum(nvl(fact.factMoney, 0))        oweMoney         -- 欠费
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
                    AND qz_year_month = '2022-02'
                    and bis_cont_id = '8a7b868b7d5cad38017d6bc0851b6542'
                  group by BIS_MUST2.bis_project_id, BIS_MUST2.bis_cont_id,
                           BIS_MUST2.bis_must_id,
                           BIS_MUST2.qz_year_month, BIS_MUST2.MUST_TYPE,
                           BIS_MUST2.billing_period_end,
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
                             AND qz_year_month = '2022-02'
                             and bis_cont_id = '8a7b868b7d5cad38017d6bc0851b6542'
                           group by BIS_MUST2.bis_project_id,
                                    BIS_MUST2.bis_cont_id,
                                    BIS_MUST2.qz_year_month,
                                    BIS_MUST2.MUST_TYPE,
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
                             AND qz_year_month = '2022-02'
                             and bis_cont_id = '8a7b868b7d5cad38017d6bc0851b6542'
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
                  SELECT bis_fact2.bis_cont_id,
                         bis_fact2.bis_must_id,
                         bis_fact2.fact_type,
                         sum(nvl(bis_fact2.money, 0)) as factMoney
                  FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
                  where bis_fact2.is_delete = 1
                    and bis_fact2.fact_type in ('1', '2')
                    and bis_fact2.bis_must_id is not null
                    AND qz_year_month = '2022-02'
                    and bis_cont_id = '8a7b868b7d5cad38017d6bc0851b6542'
                  group by bis_fact2.bis_cont_id, bis_fact2.bis_must_id, bis_fact2.fact_type
              ) fact
              on t.bis_cont_id = fact.bis_cont_id and
                 t.bis_must_id = fact.bis_must_id and
                 t.MUST_TYPE = fact.FACT_TYPE
         group by t.bis_project_id, t.bis_cont_id, substr(t.rent_last_pay_date, 0, 7),
                  t.MUST_TYPE
     ) d
         left join ods.ods_pl_powerdes_bis_project_dt bis_project
                   on d.bis_project_id = bis_project.bis_project_id
group by d.MUST_TYPE;




select bis_project.project_name, -- 项目名称
       d.bis_project_id,         -- 项目id
       d.bis_cont_id,            -- 合同id
       d.must_year_month,        -- 应收月
       d.MUST_TYPE,              -- 费项
       d.oweMoney                -- 欠费
from (
         select t.bis_project_id,                                   -- 项目id
                t.bis_cont_id,                                      -- 合同id
                t.MUST_TYPE,                                        -- 费项（1：租金 2：物管）
                substr(t.rent_last_pay_date, 0, 7) must_year_month, -- 应收月
                sum(nvl(t.mustMoney, 0)) + sum(nvl(t1.adjMoney, 0)) -
                sum(nvl(fact.factMoney, 0))        oweMoney         -- 欠费
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
                           group by BIS_MUST2.bis_project_id,
                                    BIS_MUST2.bis_cont_id,
                                    BIS_MUST2.qz_year_month,
                                    BIS_MUST2.MUST_TYPE,
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
                         sum(nvl(bis_fact2.money, 0)) as factMoney
                  FROM ods.ods_pl_powerdes_bis_fact2_dt bis_fact2
                  where bis_fact2.is_delete = 1
                    and bis_fact2.fact_type in ('1', '2')
                    and bis_fact2.bis_must_id is not null
                  group by bis_fact2.bis_cont_id, bis_fact2.bis_must_id, bis_fact2.fact_type
              ) fact
              on t.bis_cont_id = fact.bis_cont_id and
                 t.bis_must_id = fact.bis_must_id and
                 t.MUST_TYPE = fact.FACT_TYPE
         group by t.bis_project_id, t.bis_cont_id, substr(t.rent_last_pay_date, 0, 7),
                  t.MUST_TYPE
     ) d
         left join ods.ods_pl_powerdes_bis_project_dt bis_project
                   on d.bis_project_id = bis_project.bis_project_id









