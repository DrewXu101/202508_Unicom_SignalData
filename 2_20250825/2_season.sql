-- create subsample table of moving 
DROP TABLE IF EXISTS move_month_subsample; 
CREATE TABLE move_month_subsample AS SELECT
        m.uid,
        m.move_id,
        m.stime,
        m.etime,
        m.mode,
        m.start_grid_id,
        m.end_grid_id,
        m.distance,
        m.`time`,
-- 保留反引号，避免与关键字冲突
  m.moi_id,
        m.is_core,
        m.start_ptype,
        m.end_ptype,
        m.province,
        m.city,
        m.`date`,
        /* 本地标记：is_core=1 且 province='011' */
  CASE   
            WHEN m.is_core = '1'   
            AND m.province = '011' THEN 1   
            ELSE 0   
        END AS is_local,
        /* 用户属性 */
  ua.gender,
        ua.age,
        /* 聚合后的年龄组 */
  CASE   
            WHEN ua.age IN ('01',
            '02',
            '03',
            '04') THEN 1   
            WHEN ua.age IN ('05',
            '06',
            '07') THEN 2   
            WHEN ua.age IN ('08',
            '09',
            '10') THEN 3   
            WHEN ua.age IN ('11',
            '12',
            '13',
            '14',
            '15') THEN 4   
            ELSE NULL   
        END AS age_group   
    FROM
        move_month m   
    LEFT JOIN
        (
SELECT
                DISTINCT uid,
                gender,
                age   
            FROM
                user_attribute   
        ) ua   
            ON m.uid = ua.uid   
    WHERE
        m.city = 'V0110000'   
        AND (
            (
                m.`date` BETWEEN '20220801' AND '20220831'   
            )   
            OR (
                m.`date` BETWEEN '20231101' AND '20231130'   
            )   
        ); 
-- is_local 
SELECT
        substr(`date`,
        1,
        6) AS stat_month,
-- 抽取年月
  is_local,
        COUNT(*) AS n_obs,
        ROUND( COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION   
    BY
        substr(`date`,
        1,
        6)),
        2 ) AS pct   
    FROM
        move_month_subsample   
    GROUP BY
        substr(`date`,
        1,
        6),
        is_local   
    ORDER BY
        stat_month,
        is_local; 
-- is_core
SELECT
        substr(`date`, 1, 6) AS stat_month,
        is_core,
        COUNT(*) AS n_obs,
        COUNT(DISTINCT uid) AS n_uid,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION 
    BY
        substr(`date`, 1, 6)), 2) AS pct,
        AVG(distance) AS avg_distance,
        AVG(`time`) AS avg_time 
    FROM
        move_month_subsample 
    GROUP BY
        is_core,
        substr(`date`,
        1,
        6) 
    ORDER BY
        stat_month,
        n_obs DESC; 
-- create subsample table of staying 
DROP TABLE IF EXISTS stay_poi_subsample; 
CREATE TABLE stay_poi_subsample AS SELECT
        s.uid,
        s.poi_id,
        s.ptype,
        s.final_grid_id,
        s.stay_fre,
        s.weighted_centroid_lat,
        s.weighted_centroid_lon,
        s.weekday_day_time,
        s.weekday_eve_time,
        s.weekend_day_time,
        s.weekend_eve_time,
        s.is_core,
        s.province,
        s.city,
        s.`date`,
-- 本地标记（沿用你当前口径）
  CASE   
            WHEN s.is_core = '1'   
            AND s.province = '011' THEN 1   
            ELSE 0   
        END AS is_local,
-- 用户属性
  ua.gender,
        ua.age,
-- 年龄分组
  CASE   
            WHEN ua.age IN ('01',
            '02',
            '03',
            '04') THEN 1   
            WHEN ua.age IN ('05',
            '06',
            '07') THEN 2   
            WHEN ua.age IN ('08',
            '09',
            '10') THEN 3   
            WHEN ua.age IN ('11',
            '12',
            '13',
            '14',
            '15') THEN 4   
            ELSE NULL   
        END AS age_group   
    FROM
        stay_poi s   
    LEFT JOIN
        (
SELECT
                DISTINCT uid,
                gender,
                age   
            FROM
                user_attribute   
        ) ua   
            ON s.uid = ua.uid   
    WHERE
        s.city = 'V0110000'   
        AND (
            (
                s.`date` BETWEEN '20220801' AND '20220831'  
            )   
            OR (
                s.`date` BETWEEN '20231101' AND '20231130'  
            )   
        ); 
