/**
  部门表：dws_bis_department_big
 */
SELECT
    T.PLAS_ORG_ID, -- 部门ID
    T1.PARENT_ID, -- 父部门
    T2.PLAS_ORG_DIME_ID, -- 部门类别
    T.ORG_CD, -- 部门CODE
    T.ORG_NAME, -- 部门名称
    T.ACTIVE_BL,
    T.ORG_TYPE_CD, -- 机构类型，0；未知；1:部门；2：中心；3:分管；4：集团；5：组
    T.FINANCE_CHARGE_ID,  -- 'PLAS机构管理中机构编辑界面增加，财务负责人系统职位字段',
    T.ENGINEER_CHARGE_ID,  -- 'PLAS机构管理中机构编辑界面增加，工程负责人系统职位字段',
    T.BUSINESS_CHARGE_ID,  -- 'PLAS机构管理中机构编辑界面增加，招商负责人系统职位字段',
    T.OPERATION_CHARGE_ID,  -- 'PLAS机构管理中机构编辑界面增加，营运负责人系统职位字段',
    T.DESIGN_CHARGE_ID,  -- 'PLAS机构管理中机构编辑界面增加，设计负责人系统职位字段',
    T.EBUSINESS_CHARGE_ID,  -- 'PLAS机构管理中机构编辑界面增加，电商负责人系统职位字段',
    T.FINANCE_ID,  -- 'PLAS机构管理中机构编辑界面增加，财务负责人 ',
    T.ENGINEER_ID,  -- 'PLAS机构管理中机构编辑界面增加，工程负责人',
    T.BUSINESS_ID,  -- 'PLAS机构管理中机构编辑界面增加，招商负责人',
    T.OPERATION_ID,  -- 'PLAS机构管理中机构编辑界面增加，营运负责人',
    T.DESIGN_ID,  -- 'PLAS机构管理中机构编辑界面增加，设计负责人',
    T.EBUSINESS_ID,  -- 'PLAS机构管理中机构编辑界面增加，电商负责人',
    T.ORG_MGR_ID,   -- '管理人',
    T.SEQUENCE_NO, -- '序列号',
    T.REMARK, -- '备注',
    T.SHORT_CN_NAME, -- '中文简称',
    T2.DIME_CD, -- '部门类别CD',
    T2.DIME_NAME, -- '部门类别',
    CASE WHEN T.ACTIVE_BL = 0 THEN 'Y' WHEN T.ACTIVE_BL = 1 THEN 'N' END AS IS_DEL, -- '是否删除(N：否,Y:是)',
    T.UPDATED_DATE UPDATED_DATE, -- '更新时间',
    T.UPDATOR UPDATER, -- '更新人',
    T.CREATED_DATE CREATED_DATE, --'创建时间',
    T.CREATOR CREATER, -- '创建人'
    'ORACLE' AS SOURCE
FROM ods.ods_pl_powerdes_plas_org_dt T
         LEFT JOIN ods.ods_pl_powerdes_plas_dime_org_rel_dt T1 ON T.PLAS_ORG_ID= T1.PLAS_ORG_ID
         LEFT JOIN ods.ods_pl_powerdes_plas_org_dime_dt T2 ON T1.PLAS_ORG_DIME_ID=T2.PLAS_ORG_DIME_ID
WHERE T2.DIME_CD='1';
