
drop  table  if exists dim.dim_date;
CREATE TABLE `dim.dim_date`(
                           `date_id` string,
                           `day_id` string,
                           `day_short_desc` string,
                           `day_long_desc` string,
                           `week_id` string,
                           `week_long_desc` string,
                           `week_cn` string,
                           `week_num` string,
                           `week_flg_1` string,
                           `month_id` string,
                           `month_short_desc` string,
                           `month_long_desc` string,
                           `quarter_id` string,
                           `quarter_long_desc` string,
                           `year_id` string,
                           `year_long_desc` string,
                           `is_holiday` string)
    row format delimited fields terminated by ',';


drop table if exists dim.pl_date;
CREATE TABLE dim.pl_date
(
    yw_date string COMMENT 'yyyy-mm'
)
    row format delimited fields terminated by ',';


load data inpath '/mywork/dim.db/dim_date/*' into table dim.dim_date;


drop table if exists dim.dim_pl_date;
create table dim.dim_pl_date
(
    year_month_day1        string comment ' 年月日  YYYY-MM-DD',
    year_month_day2        string comment ' 年月日 YYYYMMDD',
    year_month_day3        string comment ' 年月日 YYYY/MM/DD',
    year_month_day4        string comment ' 年月日 2020年12月01日',
    year_month1            string comment ' 年月 2020-12',
    year_month2            string comment ' 年月 2020/12',
    year_month3            string comment ' 年月 2020年12月',
    year_month4            string comment ' 年月 202012',
    year                   string comment ' 年 YYYY',
    year_desc              string comment ' 年 2020年',
    month                  string comment ' 月  12',
    month_desc             string comment ' 月 12月',
    day                    string comment ' 日 dd',
    weekday_eg             string comment ' 周几(英文)',
    weekday_cn             string comment ' 周几(中文)',
    day_number_of_week     string comment ' 一周中的第几天',
    week_of_year           string comment ' 一年中的第几周',
    week_long_desc         string comment ' 一年中的第几周中文描述',
    week_of_month          string comment ' 一个月中的第几周',
    day_number_of_year     string comment ' 一年中的第几天',
    quarter_number_of_year string comment ' 季度 数字',
    quarter_long_desc      string comment ' 季度 中文'

)
    row format delimited fields terminated by '\001';


load data inpath '/mywork/dim.db/dim_date/*' into table dim.dim_date;


load data inpath '/mywork/dim.db/dim_pl_date/*' into table dim.dim_pl_date;
load data inpath '/mywork/dim.db/pl_date/*' into table dim.pl_date;



insert overwrite table dim.dim_pl_date
SELECT `date`,                                                                -- 年月日  YYYY-MM-DD
       date_ds,                                                               -- 年月日 YYYYMMDD
       date_ds2,                                                              -- 年月日 YYYY/MM/DD
       date_long_desc,                                                        -- 年月日 2020年12月01日
       month_id,                                                              -- 年月 2020-12
       regexp_replace(month_id, '-', '/'),                                    -- 年月 2020/12
       month_long_desc,                                                       -- 年月 2020年12月
       regexp_replace(month_id, '-', ''),                                     -- 年月 202012
       year,                                                                  -- 年 YYYY
       year_desc,                                                             -- 年 2020年
       month,                                                                 -- 月 MM
       concat(month, '月'),                                                    -- 月 12月
       day,                                                                   -- 日 dd
       weekday_eg,                                                            -- 周几(英文)
       case
           when weekday_eg = 'Sunday' then '星期日'
           when weekday_eg = 'Monday' then '星期一'
           when weekday_eg = 'Tuesday' then '星期二'
           when weekday_eg = 'Wednesday' then '星期三'
           when weekday_eg = 'Thursday' then '星期四'
           when weekday_eg = 'Friday' then '星期五'
           when weekday_eg = 'Saturday' then '星期六'
           else pmod(datediff(`date`, '1990-01-01'), 7) end as weekday_cn,    -- 周几(中文)
       daynumber_of_week,                                                     -- 一周中的第几天
       weekofyear(`date`)                                   as week_of_year,  -- 一年中的第几周
       week_long_desc,                                                        -- 一年中的第几周中文描述
       CAST((day(theMonday) - 1) / 7 + 1 AS BIGINT)         as week_of_month, -- 一个月中的第几周
       daynumber_of_year,                                                     -- 一年中的第几天
       floor(substr(`date`, 6, 2) / 3.1) + 1,                                 -- 季度 数字
       case
           when floor(substr(`date`, 6, 2) / 3.1) + 1 = '1' then '第一季度'
           when floor(substr(`date`, 6, 2) / 3.1) + 1 = '2' then '第二季度'
           when floor(substr(`date`, 6, 2) / 3.1) + 1 = '3' then '第三季度'
           when floor(substr(`date`, 6, 2) / 3.1) + 1 = '4' then '第四季度'
           else null end                                                      -- 季度 中文
FROM (
         SELECT `date`,
                regexp_replace(`date`, '-', '')                                    as date_ds,
                regexp_replace(`date`, '-', '/')                                   as date_ds2,
                year(`date`)                                                       as year,
                month(`date`)                                                      as month,
                day(`date`)                                                        as day,
                from_unixtime(unix_timestamp(`date`, 'yyyy-MM-dd'), 'EEEE')        as weekday_eg,        -- 周几(英文)
                date_sub(next_day(`date`, 'Mon'), 7)                               as theMonday,
                from_unixtime(unix_timestamp(`date`, "yyyy-MM-dd"), "u")           as daynumber_of_week,
                from_unixtime(unix_timestamp(`date`, 'yyyy-MM-dd'), 'yyyy年第w周')       week_long_desc,    -- 一年中的第几周中文描述
                from_unixtime(unix_timestamp(`date`, 'yyyy-MM-dd'), 'D')           as daynumber_of_year, -- 今年的第几天
                from_unixtime(unix_timestamp(`date`, 'yyyy-MM-dd'), 'yyyy年MM月dd日') as date_long_desc,
                from_unixtime(unix_timestamp(`date`, 'yyyy-MM-dd'), 'Y年')          as year_desc,
                from_unixtime(unix_timestamp(`date`, 'yyyy-MM-dd'), 'yyyy-MM')     as month_id,
                from_unixtime(unix_timestamp(`date`, 'yyyy-MM-dd'), 'yyyy年MM月')       month_long_desc
         FROM (
                  SELECT date_add(`start_date`, pos) AS `date`
                  from (
                           SELECT '1990-01-01' as start_date
                       ) t
                           lateral view posexplode(split(repeat(", ", 15006), ",")) tf AS pos, val --  14600为时间跨度
              ) dates
     ) dates_expanded
    SORT BY `date`;