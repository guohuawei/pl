SELECT BIS_PROJECT_ID,
       PROJECT_NAME,        -- 项目名称
       PINYINCODE,-- 项目code
       SHORT_NAME,
       stage,
       AREA_CD,
       PROVINCE,
       PROVINCE_CD,
       CITY,
       cast(IS_BUSINESS_PROJECT as int), --是否商业项目
       PROJECT_ADSCRIPTION, -- 项目归属
       MAP_PROVINCE,        --  省份
       MAP_COORDINATE,      --  项目坐标
       TOTAL_SQUARE,        -- 建筑面积
       FLAT_SQUARE,         -- 公寓面积
       FLAT_NUM,            -- 公寓数量
       PARK_SQUARE,         -- 停车场面积
       PARK_NUM,            -- 停车场数量
       OPEN_DATE,           -- 开业时间
       REMARK,              --备注
       ADDRESS,             -- 地址
       IMG_URL,             -- 缩列图
       OPER_STATUS,         -- 项目状态（1：在建；2：在营）
       OPERATE_AREA,        -- 区域
       MALL_ID,             -- pms项目id
       'N'         IS_DEL,  -- 'Y:生效 N:失效 D:删除'
       UPDATED_DATE,
       UPDATOR,
       CREATED_DATE,
       CREATOR,
       'oracle' as SOURCE,
       current_date         -- ETL时间
from ods.ods_pl_powerdes_bis_project_dt;