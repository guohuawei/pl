select BIS_SHOP.BIS_SHOP_ID,          -- 商家id
       BIS_SHOP_CONN.PART_NAME,       -- 经销商--公司名称
       BIS_PROJECT.PROJECT_NAME,      -- 经销商--所属项目
       BIS_SHOP_CONN.PRINCIPAL_PHONE, --经销商--电话
       BIS_SHOP_CONN.PRINCIPAL        -- 经销商--法人
from ods.ods_pl_powerdes_bis_shop_dt BIS_SHOP
         left join ods.ods_pl_powerdes_bis_shop_conn_rel_dt BIS_SHOP_CONN_REL on BIS_SHOP.BIS_SHOP_ID = BIS_SHOP_CONN_REL.BIS_SHOP_ID
         left join ods.ods_pl_powerdes_bis_shop_conn_dt BIS_SHOP_CONN on BIS_SHOP_CONN_REL.BIS_SHOP_CONN_ID = BIS_SHOP_CONN.BIS_SHOP_CONN_ID
         left join ods.ods_pl_powerdes_bis_project_dt bis_project on BIS_SHOP_CONN.BIS_PROJECT_ID = BIS_SHOP_CONN.BIS_PROJECT_ID
where  BIS_SHOP.DELETE_BL <> '1';