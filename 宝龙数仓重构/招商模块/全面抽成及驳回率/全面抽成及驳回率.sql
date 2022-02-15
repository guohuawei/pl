with tmp1 as (
    select bis_project_id,
           PROJECT_NAME,
           org_cd,
           month_id
    from ods.ods_pl_powerdes_bis_project_dt a,
         (
             select distinct month_id
             from dim.dim_date
             where month_id between '201010' and replace(substr(current_date, 1, 7), '-', '')
         ) b where a.oper_status = '1' and is_business_project = '1'
),
     tmp2 as (
         select t1.land_project_cd,
                replace(substr(start_date, 0, 7), '-', '')                               month_id,
                sum(case
                        when t2.RES_MODULE_ID in
                             ('402834e931835f03013184bc8a500074', 'E90421F27DC74C9DB886A2351D5E4F70') then 1
                        else 0 end)                                                      curmonth_business_fq_num,
                sum(case
                        when t2.RES_MODULE_ID in
                             ('402834e931835f03013184bc8a500074', 'E90421F27DC74C9DB886A2351D5E4F70') and
                             STATUS_CD = '3' then 1
                        else 0 end)                                                      curmonth_business_bh_num,
                sum(case
                        when t2.RES_MODULE_ID in
                             ('8a7b859c650f571d01650fb6c31c0715', 'ff8080814af18f95014af1a875e20007') then 1
                        else 0 end)                                                      curmonth_cont_fq_num,
                sum(case
                        when t2.RES_MODULE_ID in
                             ('8a7b859c650f571d01650fb6c31c0715', 'ff8080814af18f95014af1a875e20007') and
                             STATUS_CD = '3' then 1
                        else 0 end)                                                      curmonth_cont_bh_num,
                sum(case when t1.auth_type_cd = 'SYGS_ZSGL_ZHPPGL_12' then 1 else 0 end) curmonth_brand_fq_num,
                sum(case
                        when t1.auth_type_cd = 'SYGS_ZSGL_ZHPPGL_12' and STATUS_CD = '3' then 1
                        else 0 end)                                                      curmonth_brand_bh_num,
                sum(case
                        when (t2.RES_MODULE_ID in
                              ('8a7b859c650f571d01650fb6c31c0715', 'ff8080814af18f95014af1a875e20007',
                               '402834e931835f03013184bc8a500074', 'E90421F27DC74C9DB886A2351D5E4F70') or
                              t1.auth_type_cd = 'SYGS_ZSGL_ZHPPGL_12') then 1
                        else 0 end)                                                      fq_total,
                sum(case
                        when (t2.RES_MODULE_ID in
                              ('8a7b859c650f571d01650fb6c31c0715', 'ff8080814af18f95014af1a875e20007',
                               '402834e931835f03013184bc8a500074', 'E90421F27DC74C9DB886A2351D5E4F70') or
                              t1.auth_type_cd = 'SYGS_ZSGL_ZHPPGL_12') and STATUS_CD = '3' then 1
                        else 0 end)                                                      bh_total
         from ods.ods_pl_powerdes_res_approve_info_dt t1
                  left join ods.ods_pl_powerdes_res_auth_type_dt t2 on t1.auth_type_cd = t2.auth_type_cd
         group by t1.land_project_cd,
                  replace(substr(start_date, 0, 7), '-', '')
     ),
     tmp3 as (
         select tmp1.bis_project_id,
                tmp1.project_name,
                tmp1.month_id,
                nvl(lag(tmp2.curmonth_business_fq_num, 1, 0)
                        over (partition by tmp1.bis_project_id,tmp1.project_name order by tmp1.month_id),
                    0)                                lagmonth_business_fq_num,
                nvl(lag(tmp2.curmonth_business_bh_num, 1, 0)
                        over (partition by tmp1.bis_project_id,tmp1.project_name order by tmp1.month_id),
                    0)                                lagmonth_business_bh_num,

                round(1 - case
                              when nvl(lag(tmp2.curmonth_business_fq_num, 1, 0)
                                           over (partition by tmp1.bis_project_id,tmp1.project_name order by tmp1.month_id),
                                       0) = 0 then 1
                              else nvl(lag(tmp2.curmonth_business_bh_num, 1, 0)
                                           over (partition by tmp1.bis_project_id,tmp1.project_name order by tmp1.month_id),
                                       0) /
                                   nvl(lag(tmp2.curmonth_business_fq_num, 1, 0)
                                           over (partition by tmp1.bis_project_id,tmp1.project_name order by tmp1.month_id),
                                       0) end, 2)     lagmonth_business_zql,

                nvl(tmp2.curmonth_business_fq_num, 0) curmonth_business_fq_num,
                nvl(tmp2.curmonth_business_bh_num, 0) curmonth_business_bh_num,

                round(1 - case
                              when nvl(tmp2.curmonth_business_fq_num, 0) = 0 then 1
                              else
                                      nvl(tmp2.curmonth_business_bh_num, 0) / nvl(tmp2.curmonth_business_fq_num, 0) end,
                      2)                              curmonth_business_zql,

                nvl(lag(tmp2.curmonth_cont_fq_num, 1, 0)
                        over (partition by tmp1.bis_project_id,tmp1.project_name order by tmp1.month_id),
                    0)                                lagmonth_cont_fq_num,
                nvl(lag(tmp2.curmonth_cont_bh_num, 1, 0)
                        over (partition by tmp1.bis_project_id,tmp1.project_name order by tmp1.month_id),
                    0)                                lagmonth_cont_bh_num,

                round(1 - case
                              when nvl(lag(tmp2.curmonth_cont_fq_num, 1, 0)
                                           over (partition by tmp1.bis_project_id,tmp1.project_name order by tmp1.month_id),
                                       0) = 0 then 1
                              else nvl(lag(tmp2.curmonth_cont_bh_num, 1, 0)
                                           over (partition by tmp1.bis_project_id,tmp1.project_name order by tmp1.month_id),
                                       0) /
                                   nvl(lag(tmp2.curmonth_cont_fq_num, 1, 0)
                                           over (partition by tmp1.bis_project_id,tmp1.project_name order by tmp1.month_id),
                                       0) end, 2)     lagmonth_cont_zql,

                nvl(tmp2.curmonth_cont_fq_num, 0)     curmonth_cont_fq_num,
                nvl(tmp2.curmonth_cont_bh_num, 0)     curmonth_cont_bh_num,

                round(1 - case
                              when nvl(tmp2.curmonth_cont_fq_num, 0) = 0 then 1
                              else
                                      nvl(tmp2.curmonth_cont_bh_num, 0) / nvl(tmp2.curmonth_cont_fq_num, 0) end,
                      2)                              curmonth_cont_zql,

                nvl(lag(tmp2.curmonth_brand_fq_num, 1, 0)
                        over (partition by tmp1.bis_project_id,tmp1.project_name order by tmp1.month_id),
                    0)                                lagmonth_brand_fq_num,
                nvl(lag(tmp2.curmonth_brand_bh_num, 1, 0)
                        over (partition by tmp1.bis_project_id,tmp1.project_name order by tmp1.month_id),
                    0)                                lagmonth_brand_bh_num,

                round(1 - case
                              when nvl(lag(tmp2.curmonth_brand_fq_num, 1, 0)
                                           over (partition by tmp1.bis_project_id,tmp1.project_name order by tmp1.month_id),
                                       0) = 0 then 1
                              else
                                      nvl(lag(tmp2.curmonth_brand_bh_num, 1, 0)
                                              over (partition by tmp1.bis_project_id,tmp1.project_name order by tmp1.month_id),
                                          0) /
                                      nvl(lag(tmp2.curmonth_brand_fq_num, 1, 0)
                                              over (partition by tmp1.bis_project_id,tmp1.project_name order by tmp1.month_id),
                                          0) end, 2)  lagmonth_brand_zql,
                nvl(tmp2.curmonth_brand_fq_num, 0)    curmonth_brand_fq_num,
                nvl(tmp2.curmonth_brand_bh_num, 0)    curmonth_brand_bh_num,

                round(1 - case
                              when nvl(tmp2.curmonth_brand_fq_num, 0) = 0 then 1
                              else
                                      nvl(tmp2.curmonth_brand_bh_num, 0) / nvl(tmp2.curmonth_brand_fq_num, 0) end,
                      2)                              curmonth_pp_zql,

                nvl(tmp2.fq_total, 0)                 fq_total,
                nvl(tmp2.bh_total, 0)                 bh_total,

                round(1 -
                      case when nvl(tmp2.fq_total, 0) = 0 then 1 else nvl(tmp2.bh_total, 0) / nvl(tmp2.fq_total, 0) end,
                      2)                              zql_total
         from tmp1
                  left join tmp2 on tmp1.org_cd = tmp2.land_project_cd and tmp1.month_id = tmp2.month_id
     ),
     tmp4 as (
         select bis_project_id,
                month_id,
                nvl(sum(case
                            when PAY_WAY = '2' and
                                 month_id between replace(substr(CONT_START_DATE, 1, 7), '-', '') and replace(substr(CONT_end_DATE, 1, 7), '-', '')
                                then 1
                            else 0 end), 0) ck_num,
                nvl(sum(case
                            when PAY_WAY = '1' and
                                 month_id between replace(substr(CONT_START_DATE, 1, 7), '-', '') and replace(substr(CONT_end_DATE, 1, 7), '-', '')
                                then 1
                            else 0 end), 0) bd_num,
                nvl(sum(case
                            when PAY_WAY = '3' and
                                 month_id between replace(substr(CONT_START_DATE, 1, 7), '-', '') and replace(substr(CONT_end_DATE, 1, 7), '-', '')
                                then 1
                            else 0 end), 0) qg_num,
                nvl(sum(case
                            when PAY_WAY in ('1', '2', '3') and
                                 month_id between replace(substr(CONT_START_DATE, 1, 7), '-', '') and replace(substr(CONT_end_DATE, 1, 7), '-', '')
                                then 1
                            else 0 end), 0) total_num
         from ods.ods_pl_powerdes_bis_cont_dt a1,
              (
                  select distinct month_id
                  from dim.dim_date
                  where month_id between '201010' and replace(substr(current_date, 1, 7), '-', '')
              ) b1
         where EFFECT_FLG <> 'D'
         group by bis_project_id, month_id
     )
insert OVERWRITE table dws.dws_bis_commission_reject_rate_big_dt partition (dt='${hiveconf:nowdate}')
select tmp3.bis_project_id,
       tmp3.project_name,
       tmp3.month_id,
       tmp3.lagmonth_brand_fq_num,
       tmp3.lagmonth_brand_bh_num,
       tmp3.lagmonth_brand_zql,
       tmp3.curmonth_brand_fq_num,
       tmp3.curmonth_brand_bh_num,
       tmp3.curmonth_pp_zql,
       tmp3.lagmonth_business_fq_num,
       tmp3.lagmonth_business_bh_num,
       tmp3.lagmonth_business_zql,
       tmp3.curmonth_business_fq_num,
       tmp3.curmonth_business_bh_num,
       tmp3.curmonth_business_zql,
       tmp3.lagmonth_cont_fq_num,
       tmp3.lagmonth_cont_bh_num,
       tmp3.lagmonth_cont_zql,
       tmp3.curmonth_cont_fq_num,
       tmp3.curmonth_cont_bh_num,
       tmp3.curmonth_cont_zql,
       tmp3.fq_total,
       tmp3.bh_total,
       tmp3.zql_total,
       tmp4.total_num,
       tmp4.ck_num,
       tmp4.bd_num,
       tmp4.qg_num
from tmp3
         left join tmp4 on tmp3.bis_project_id = tmp4.bis_project_id and tmp3.month_id = tmp4.month_id
;