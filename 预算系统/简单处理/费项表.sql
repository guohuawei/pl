
/**
 费项表: PMS_FIN_CHARGE_ITEM_DICT_BIG
 */
select
    PARENT_ID,
    CHARGE_ITEM_CODE,
    THIRD_PARTY_CODE,
    CHARGE_ITEM_NAME,
    CHARGE_ITEM_TYPE ,
    ITEM_TYPE_CODE ,
    PRICE ,
    MEASUREMENT ,
    FINANCE_NAME,
    FINANCE_CODE,
    CASE WHEN IS_DEL = 0 THEN 'N' WHEN IS_DEL = 1 THEN 'Y' END AS IS_DEL, -- '是否删除(N：否,Y:是)'
    UPDATED_DATE,
    UPDATER,
    CREATED_DATE,
    CREATER,
    "oracle" as source,
     ID
from ods.ods_pl_powerdes_pms_fin_charge_item_dict_dt;