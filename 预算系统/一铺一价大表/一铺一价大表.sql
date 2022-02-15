/*
一铺一价大表
dws_bis_store_price_big
*/
select
    bis_store_price.id , -- 一铺一价id,
    bis_store.bis_store_id,-- 店铺id
    bis_store.store_no, -- 铺位号
    bis_store_price.rent_price, -- 租金标准单价
    bis_store_price.mgr_price, -- 物管标准单价
    '/' as bhz_standard, -- 保证金
    '/' as lowestMoney, -- 最低营业额
    bis_store_price.free_period, -- 免租期标准
    '/' as discount, -- 扣率
    '/' as approvalStatus, -- 审批状态
    concat(cast ((year(bis_store.effect_date)-1 + cast(bis_store_price.seq_year as int)) as string),substr(bis_store.effect_date,5,6)) as start_date, -- 开始时间
    date_sub(concat(cast ((year(bis_store.effect_date) + cast(bis_store_price.seq_year as int)) as string),substr(bis_store.effect_date,5,6)),1) as end_date, -- 结束时间
    bis_store.is_delete, -- 是否删除
    bis_store.updated_date, -- 更新时间
    bis_store.updator, -- 更新人员
    bis_store.created_date, -- 创建时间
    bis_store.creator, -- 创建人
    "oracle" as source -- 数据来源
from ods.ods_pl_powerdes_bis_store_dt bis_store
         left join ods.ods_pl_powerdes_bis_store_price_dt bis_store_price
                   on bis_store.bis_store_id = bis_store_price.bis_store_id
where bis_store.is_delete = 'N' and bis_store.status_cd = '1'