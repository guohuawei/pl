select bis_shop.bis_shop_id,                                              -- 商家id
       bis_project.project_name,                                          -- 项目名称
       bis_cont.cont_no,                                                  -- 合同号
       case
           when BIS_CONT.CONT_TYPE_CD = '1' then '自持商铺合同'
           when BIS_CONT.CONT_TYPE_CD = '2' then '已售商铺合同'
           when BIS_CONT.CONT_TYPE_CD = '3' then '多经合同'
           when BIS_CONT.CONT_TYPE_CD = '4' then '广告位合同'
           else null end,                                                 -- 合同类型,
       if(bis_cont.cont_end_date = bis_cont.cont_to_fail_date, '是', '否'), -- 是否正常解约
       case
           when bis_cont.STATUS_CD = '0' then '未签约'
           when bis_cont.STATUS_CD = '1' then '已审核'
           when bis_cont.STATUS_CD = '2' then '已解约'
           when bis_cont.STATUS_CD = '3' then '未审核'
           when bis_cont.STATUS_CD = '4' then '无效合同'
           when bis_cont.STATUS_CD = '5' then '待处理'
           else null end,                                                 -- 合同状态,
       bis_cont.cont_end_date,                                            -- 到期日
       bis_cont.effect_flg,                                               -- 合同是否有效
       bis_cont.rent_square                                               -- 计租面积


from ods.ods_pl_powerdes_bis_shop_dt bis_shop
         left join ods.ods_pl_powerdes_bis_cont_dt bis_cont on bis_shop.bis_shop_id = bis_cont.bis_shop_id
         left join ods.ods_pl_powerdes_bis_project_dt bis_project
                   on bis_cont.bis_project_id = bis_project.bis_project_id
where bis_shop.DELETE_BL = '0'
  and bis_cont.effect_flg <> 'D';
