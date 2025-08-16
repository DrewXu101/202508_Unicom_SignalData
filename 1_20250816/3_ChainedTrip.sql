-- 1. Creating tables 
-- August, household
-- 如需重跑先：DROP TABLE xmu_hh_members_202208;
DROP TABLE xmu_hh_members_202208; 
CREATE TABLE xmu_hh_members_202208 AS SELECT
        '20220801' AS stat_date,
        'V0110000' AS city,
        DENSE_RANK() OVER (  
    ORDER BY
        g.final_grid_id) AS household_id,
        g.final_grid_id,
        u.uid   
    FROM
        ( /* 先筛出 HH≥2 的居住网格（distinct uid 计数） */
SELECT
            d.final_grid_id   
        FROM
            ( SELECT
                DISTINCT sp.final_grid_id,
                sp.uid   
            FROM
                stay_poi sp   
            WHERE
                sp.city = 'V0110000'   
                AND sp.date = '20220801'   
                AND sp.ptype = 1 ) d   
        GROUP BY
            d.final_grid_id   
        HAVING
            COUNT(*) >= 2   
        ) g /* 再把这些网格里的去重成员 uid 拉平 */
    
    JOIN
        (
SELECT
                DISTINCT sp.final_grid_id,
                sp.uid   
            FROM
                stay_poi sp   
            WHERE
                sp.city = 'V0110000'   
                AND sp.date = '20220801'   
                AND sp.ptype = 1   
        ) u   
            ON u.final_grid_id = g.final_grid_id; 
DROP TABLE xmu_hh_members_202311; 
CREATE TABLE xmu_hh_members_202311 AS SELECT
        '20231101' AS stat_date,
        'V0110000' AS city,
        DENSE_RANK() OVER (  
    ORDER BY
        g.final_grid_id) AS household_id,
        g.final_grid_id,
        u.uid   
    FROM
        ( /* 先筛出 HH≥2 的居住网格（distinct uid 计数） */
SELECT
            d.final_grid_id   
        FROM
            ( SELECT
                DISTINCT sp.final_grid_id,
                sp.uid   
            FROM
                stay_poi sp   
            WHERE
                sp.city = 'V0110000'   
                AND sp.date = '20231101'   
                AND sp.ptype = 1 ) d   
        GROUP BY
            d.final_grid_id   
        HAVING
            COUNT(*) >= 2   
        ) g /* 再把这些网格里的去重成员 uid 拉平 */
    
    JOIN
        (
SELECT
                DISTINCT sp.final_grid_id,
                sp.uid   
            FROM
                stay_poi sp   
            WHERE
                sp.city = 'V0110000'   
                AND sp.date = '20231101'   
                AND sp.ptype = 1   
        ) u   
            ON u.final_grid_id = g.final_grid_id; 
CREATE TABLE xmu_hh_members_202208 AS SELECT
        '20220801' AS stat_date,
        'V0110000' AS city,
        DENSE_RANK() OVER (  
    ORDER BY
        g.final_grid_id) AS household_id,
        g.final_grid_id,
        u.uid   
    FROM
        ( /* 先筛出 HH≥2 的居住网格（distinct uid 计数） */
SELECT
            d.final_grid_id   
        FROM
            ( SELECT
                DISTINCT sp.final_grid_id,
                sp.uid   
            FROM
                stay_poi sp   
            WHERE
                sp.city = 'V0110000'   
                AND sp.date = '20220801'   
                AND sp.ptype = 1 ) d   
        GROUP BY
            d.final_grid_id   
        HAVING
            COUNT(*) >= 2   
        ) g /* 再把这些网格里的去重成员 uid 拉平 */
    
    JOIN
        (
SELECT
                DISTINCT sp.final_grid_id,
                sp.uid   
            FROM
                stay_poi sp   
            WHERE
                sp.city = 'V0110000'   
                AND sp.date = '20220801'   
                AND sp.ptype = 1   
        ) u   
            ON u.final_grid_id = g.final_grid_id; 
-- November, household
-- 如需重跑可先: DROP TABLE xmu_hh_subsample_grids_202311;
CREATE TABLE xmu_hh_subsample_grids_202311 AS SELECT
        '20231101' AS stat_date,
        'V0110000' AS city,
        DENSE_RANK() OVER (   
    ORDER BY
        g.final_grid_id) AS household_id,
        g.final_grid_id   
    FROM
        ( /* 先去重到“人-格子”层，避免同一人多行 */
SELECT
            d.final_grid_id   
        FROM
            ( SELECT
                DISTINCT sp.final_grid_id,
                sp.uid   
            FROM
                stay_poi sp   
            WHERE
                sp.city = 'V0110000'   
                AND sp.date = '20231101'   
                AND sp.ptype = 1 ) d   
        GROUP BY
            d.final_grid_id   
        HAVING
            COUNT(*) >= 2 -- 等价于“每格 distinct 人数 >= 2”
    
        ) g; 
-- 2. Demographic composition 
-- 2.1 gender
-- Gender, August
SELECT
        '2022-08' AS month,
        ua.gender,
        CASE   
            WHEN COUNT(DISTINCT ua.uid) < 15 THEN '≤15人'   
            ELSE CAST(COUNT(DISTINCT ua.uid) AS STRING)   
        END AS freq   
    FROM
        user_attribute ua   
    JOIN
        (
            /* 用 subsample 网格清单筛出目标用户（uid 仅用于过滤，不出现在结果） */
SELECT
                DISTINCT sp.uid   
            FROM
                stay_poi sp   
            JOIN
                xmu_hh_subsample_grids_202208 g   
                    ON sp.final_grid_id = g.final_grid_id   
            WHERE
                sp.city = 'V0110000'   
                AND sp.date = '20220801'   
                AND sp.ptype = 1   
        ) hh   
            ON ua.uid = hh.uid   
            AND ua.city = 'V0110000'   
            AND ua.date = 20220801   
    GROUP BY
        ua.gender; 
-- Gender, November 
SELECT
        '2023-11' AS month,
        ua.gender,
        CASE   
            WHEN COUNT(DISTINCT ua.uid) < 15 THEN '≤15人'   
            ELSE CAST(COUNT(DISTINCT ua.uid) AS STRING)   
        END AS freq   
    FROM
        user_attribute ua   
    JOIN
        (
            /* 用 subsample 网格清单筛出目标用户（uid 仅用于过滤，不出现在结果） */
SELECT
                DISTINCT sp.uid   
            FROM
                stay_poi sp   
            JOIN
                xmu_hh_subsample_grids_202311 g   
                    ON sp.final_grid_id = g.final_grid_id   
            WHERE
                sp.city = 'V0110000'   
                AND sp.date = '20231101'   
                AND sp.ptype = 1   
        ) hh   
            ON ua.uid = hh.uid   
            AND ua.city = 'V0110000'   
            AND ua.date = 20231101   
    GROUP BY
        ua.gender; 
-- 2.2 age
-- Age, August
SELECT
        '2022-08' AS month,
        ua.age,
        CASE   
            WHEN COUNT(DISTINCT ua.uid) < 15 THEN '≤15人'   
            ELSE CAST(COUNT(DISTINCT ua.uid) AS STRING)   
        END AS freq   
    FROM
        user_attribute ua   
    JOIN
        (
            /* 用 subsample 网格清单筛出目标用户（uid 仅用于过滤，不出现在结果） */
SELECT
                DISTINCT sp.uid   
            FROM
                stay_poi sp   
            JOIN
                xmu_hh_subsample_grids_202208 g   
                    ON sp.final_grid_id = g.final_grid_id   
            WHERE
                sp.city = 'V0110000'   
                AND sp.date = '20220801'   
                AND sp.ptype = 1   
        ) hh   
            ON ua.uid = hh.uid   
            AND ua.city = 'V0110000'   
            AND ua.date = 20220801   
    GROUP BY
        ua.age; 
-- Age, November
SELECT
        '2023-11' AS month,
        ua.age,
        CASE   
            WHEN COUNT(DISTINCT ua.uid) < 15 THEN '≤15人'   
            ELSE CAST(COUNT(DISTINCT ua.uid) AS STRING)   
        END AS freq   
    FROM
        user_attribute ua   
    JOIN
        (
            /* 用 subsample 网格清单筛出目标用户（uid 仅用于过滤，不出现在结果） */
SELECT
                DISTINCT sp.uid   
            FROM
                stay_poi sp   
            JOIN
                xmu_hh_subsample_grids_202311 g   
                    ON sp.final_grid_id = g.final_grid_id   
            WHERE
                sp.city = 'V0110000'   
                AND sp.date = '20231101'   
                AND sp.ptype = 1   
        ) hh   
            ON ua.uid = hh.uid   
            AND ua.city = 'V0110000'   
            AND ua.date = 20231101   
    GROUP BY
        ua.age; 
-- 2.3 is_local, 
-- is_local, August 
SELECT
        '2022-08' AS month,
        ua.is_local,
        CASE   
            WHEN COUNT(DISTINCT ua.uid) < 15 THEN '≤15人'   
            ELSE CAST(COUNT(DISTINCT ua.uid) AS STRING)   
        END AS freq   
    FROM
        user_attribute ua   
    JOIN
        (
            /* 用 subsample 网格清单筛出目标用户（uid 仅用于过滤，不出现在结果） */
SELECT
                DISTINCT sp.uid   
            FROM
                stay_poi sp   
            JOIN
                xmu_hh_subsample_grids_202208 g   
                    ON sp.final_grid_id = g.final_grid_id   
            WHERE
                sp.city = 'V0110000'   
                AND sp.date = '20231101'   
                AND sp.ptype = 1   
        ) hh   
            ON ua.uid = hh.uid   
            AND ua.city = 'V0110000'   
            AND ua.date = 20220801   
    GROUP BY
        ua.is_local; 
-- is_local, November
SELECT
        '2023-11' AS month,
        ua.is_local,
        CASE   
            WHEN COUNT(DISTINCT ua.uid) < 15 THEN '≤15人'   
            ELSE CAST(COUNT(DISTINCT ua.uid) AS STRING)   
        END AS freq   
    FROM
        user_attribute ua   
    JOIN
        (
            /* 用 subsample 网格清单筛出目标用户（uid 仅用于过滤，不出现在结果） */
SELECT
                DISTINCT sp.uid   
            FROM
                stay_poi sp   
            JOIN
                xmu_hh_subsample_grids_202311 g   
                    ON sp.final_grid_id = g.final_grid_id   
            WHERE
                sp.city = 'V0110000'   
                AND sp.date = '20231101'   
                AND sp.ptype = 1   
        ) hh   
            ON ua.uid = hh.uid   
            AND ua.city = 'V0110000'   
            AND ua.date = 20231101   
    GROUP BY
        ua.is_local; 
-- 3. Stay patterns
-- 3.1 Gender
-- Gender, August
SELECT
        '20220801' AS stat_date,
        'V0110000' AS city,
        t.gender,
        CASE   
            WHEN t.group_n < 15 THEN '≤15人'   
            ELSE CAST(t.group_n AS STRING)   
        END AS group_n_display,
        CASE   
            WHEN t.wk_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.wk_p0 / t.wk_total,
            2)   
        END AS weekday_p0_share_pct,
        CASE   
            WHEN t.wk_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.wk_p1 / t.wk_total,
            2)   
        END AS weekday_p1_share_pct,
        CASE   
            WHEN t.wk_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.wk_p2 / t.wk_total,
            2)   
        END AS weekday_p2_share_pct,
        CASE   
            WHEN t.we_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.we_p0 / t.we_total,
            2)   
        END AS weekend_p0_share_pct,
        CASE   
            WHEN t.we_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.we_p1 / t.we_total,
            2)   
        END AS weekend_p1_share_pct,
        CASE   
            WHEN t.we_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.we_p2 / t.we_total,
            2)   
        END AS weekend_p2_share_pct   
    FROM
        ( SELECT
            ua.gender,
            COUNT(DISTINCT u.uid) AS group_n,
            AVG(COALESCE(s0.p0_wk,
            0)) AS wk_p0,
            AVG(COALESCE(s1.p1_wk,
            0)) AS wk_p1,
            AVG(COALESCE(s2.p2_wk,
            0)) AS wk_p2,
            AVG(COALESCE(s0.p0_we,
            0)) AS we_p0,
            AVG(COALESCE(s1.p1_we,
            0)) AS we_p1,
            AVG(COALESCE(s2.p2_we,
            0)) AS we_p2,
            (AVG(COALESCE(s0.p0_wk,
            0)) + AVG(COALESCE(s1.p1_wk,
            0)) + AVG(COALESCE(s2.p2_wk,
            0))) AS wk_total,
            (AVG(COALESCE(s0.p0_we,
            0)) + AVG(COALESCE(s1.p1_we,
            0)) + AVG(COALESCE(s2.p2_we,
            0))) AS we_total   
        FROM
            ( SELECT
                DISTINCT sp.uid   
            FROM
                stay_poi sp   
            JOIN
                xmu_hh_subsample_grids_202208 g   
                    ON sp.final_grid_id = g.final_grid_id   
            WHERE
                sp.city = 'V0110000'   
                AND sp.date = '20220801'   
                AND sp.ptype = 1 ) u   
        LEFT JOIN
            (
SELECT
                    uid,
                    SUM(weekday_day_time + weekday_eve_time) AS p0_wk,
                    SUM(weekend_day_time + weekend_eve_time) AS p0_we   
                FROM
                    stay_poi   
                WHERE
                    city = 'V0110000'   
                    AND date = '20220801'   
                    AND ptype = 0   
                GROUP BY
                    uid   
            ) s0   
                ON u.uid = s0.uid   
        LEFT JOIN
            (
SELECT
                    uid,
                    SUM(weekday_day_time + weekday_eve_time) AS p1_wk,
                    SUM(weekend_day_time + weekend_eve_time) AS p1_we   
                FROM
                    stay_poi   
                WHERE
                    city = 'V0110000'   
                    AND date = '20220801'   
                    AND ptype = 1   
                GROUP BY
                    uid   
            ) s1   
                ON u.uid = s1.uid   
        LEFT JOIN
            (
SELECT
                    uid,
                    SUM(weekday_day_time + weekday_eve_time) AS p2_wk,
                    SUM(weekend_day_time + weekend_eve_time) AS p2_we   
                FROM
                    stay_poi   
                WHERE
                    city = 'V0110000'   
                    AND date = '20220801'   
                    AND ptype = 2   
                GROUP BY
                    uid   
            ) s2   
                ON u.uid = s2.uid   
        JOIN
            user_attribute ua   
                ON ua.uid = u.uid   
                AND ua.city = 'V0110000'   
                AND ua.date = 20220801   
        GROUP BY
            ua.gender ) t; 
-- Gender, November 
SELECT
        '20231101' AS stat_date,
        'V0110000' AS city,
        t.gender,
        CASE   
            WHEN t.group_n < 15 THEN '≤15人'   
            ELSE CAST(t.group_n AS STRING)   
        END AS group_n_display,
        CASE   
            WHEN t.wk_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.wk_p0 / t.wk_total,
            2)   
        END AS weekday_p0_share_pct,
        CASE   
            WHEN t.wk_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.wk_p1 / t.wk_total,
            2)   
        END AS weekday_p1_share_pct,
        CASE   
            WHEN t.wk_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.wk_p2 / t.wk_total,
            2)   
        END AS weekday_p2_share_pct,
        CASE   
            WHEN t.we_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.we_p0 / t.we_total,
            2)   
        END AS weekend_p0_share_pct,
        CASE   
            WHEN t.we_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.we_p1 / t.we_total,
            2)   
        END AS weekend_p1_share_pct,
        CASE   
            WHEN t.we_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.we_p2 / t.we_total,
            2)   
        END AS weekend_p2_share_pct   
    FROM
        ( SELECT
            ua.gender,
            COUNT(DISTINCT u.uid) AS group_n,
            AVG(COALESCE(s0.p0_wk,
            0)) AS wk_p0,
            AVG(COALESCE(s1.p1_wk,
            0)) AS wk_p1,
            AVG(COALESCE(s2.p2_wk,
            0)) AS wk_p2,
            AVG(COALESCE(s0.p0_we,
            0)) AS we_p0,
            AVG(COALESCE(s1.p1_we,
            0)) AS we_p1,
            AVG(COALESCE(s2.p2_we,
            0)) AS we_p2,
            (AVG(COALESCE(s0.p0_wk,
            0)) + AVG(COALESCE(s1.p1_wk,
            0)) + AVG(COALESCE(s2.p2_wk,
            0))) AS wk_total,
            (AVG(COALESCE(s0.p0_we,
            0)) + AVG(COALESCE(s1.p1_we,
            0)) + AVG(COALESCE(s2.p2_we,
            0))) AS we_total   
        FROM
            ( SELECT
                DISTINCT sp.uid   
            FROM
                stay_poi sp   
            JOIN
                xmu_hh_subsample_grids_202311 g   
                    ON sp.final_grid_id = g.final_grid_id   
            WHERE
                sp.city = 'V0110000'   
                AND sp.date = '20231101'   
                AND sp.ptype = 1 ) u   
        LEFT JOIN
            (
SELECT
                    uid,
                    SUM(weekday_day_time + weekday_eve_time) AS p0_wk,
                    SUM(weekend_day_time + weekend_eve_time) AS p0_we   
                FROM
                    stay_poi   
                WHERE
                    city = 'V0110000'   
                    AND date = '20231101'   
                    AND ptype = 0   
                GROUP BY
                    uid   
            ) s0   
                ON u.uid = s0.uid   
        LEFT JOIN
            (
SELECT
                    uid,
                    SUM(weekday_day_time + weekday_eve_time) AS p1_wk,
                    SUM(weekend_day_time + weekend_eve_time) AS p1_we   
                FROM
                    stay_poi   
                WHERE
                    city = 'V0110000'   
                    AND date = '20231101'   
                    AND ptype = 1   
                GROUP BY
                    uid   
            ) s1   
                ON u.uid = s1.uid   
        LEFT JOIN
            (
SELECT
                    uid,
                    SUM(weekday_day_time + weekday_eve_time) AS p2_wk,
                    SUM(weekend_day_time + weekend_eve_time) AS p2_we   
                FROM
                    stay_poi   
                WHERE
                    city = 'V0110000'   
                    AND date = '20231101'   
                    AND ptype = 2   
                GROUP BY
                    uid   
            ) s2   
                ON u.uid = s2.uid   
        JOIN
            user_attribute ua   
                ON ua.uid = u.uid   
                AND ua.city = 'V0110000'   
                AND ua.date = 20231101   
        GROUP BY
            ua.gender ) t; 
-- 3.2 Age 
-- Age, August
SELECT
        '20220801' AS stat_date,
        'V0110000' AS city,
        t.age,
        CASE   
            WHEN t.group_n < 15 THEN '≤15人'   
            ELSE CAST(t.group_n AS STRING)   
        END AS group_n_display,
        /* 工作日占比 */
  CASE   
            WHEN t.wk_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.wk_p0 / t.wk_total,
            2)   
        END AS weekday_p0_share_pct,
        CASE   
            WHEN t.wk_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.wk_p1 / t.wk_total,
            2)   
        END AS weekday_p1_share_pct,
        CASE   
            WHEN t.wk_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.wk_p2 / t.wk_total,
            2)   
        END AS weekday_p2_share_pct,
        /* 周末占比 */
  CASE   
            WHEN t.we_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.we_p0 / t.we_total,
            2)   
        END AS weekend_p0_share_pct,
        CASE   
            WHEN t.we_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.we_p1 / t.we_total,
            2)   
        END AS weekend_p1_share_pct,
        CASE   
            WHEN t.we_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.we_p2 / t.we_total,
            2)   
        END AS weekend_p2_share_pct   
    FROM
        ( SELECT
            ua.age,
            COUNT(DISTINCT u.uid) AS group_n,
            /* 先在 uid 层聚，再在组内取均值 */
  AVG(COALESCE(s0.p0_wk,
            0)) AS wk_p0,
            AVG(COALESCE(s1.p1_wk,
            0)) AS wk_p1,
            AVG(COALESCE(s2.p2_wk,
            0)) AS wk_p2,
            AVG(COALESCE(s0.p0_we,
            0)) AS we_p0,
            AVG(COALESCE(s1.p1_we,
            0)) AS we_p1,
            AVG(COALESCE(s2.p2_we,
            0)) AS we_p2,
            /* 分母：工作日/周末总时长（均值后再求和） */
  (AVG(COALESCE(s0.p0_wk,
            0)) + AVG(COALESCE(s1.p1_wk,
            0)) + AVG(COALESCE(s2.p2_wk,
            0))) AS wk_total,
            (AVG(COALESCE(s0.p0_we,
            0)) + AVG(COALESCE(s1.p1_we,
            0)) + AVG(COALESCE(s2.p2_we,
            0))) AS we_total   
        FROM
            ( SELECT
                DISTINCT sp.uid   
            FROM
                stay_poi sp   
            JOIN
                xmu_hh_subsample_grids_202208 g   
                    ON sp.final_grid_id = g.final_grid_id   
            WHERE
                sp.city = 'V0110000'   
                AND sp.date = '20220801'   
                AND sp.ptype = 1 ) u   
        LEFT JOIN
            (
SELECT
                    uid,
                    SUM(weekday_day_time + weekday_eve_time) AS p0_wk,
                    SUM(weekend_day_time + weekend_eve_time) AS p0_we   
                FROM
                    stay_poi   
                WHERE
                    city = 'V0110000'   
                    AND date = '20220801'   
                    AND ptype = 0   
                GROUP BY
                    uid   
            ) s0   
                ON u.uid = s0.uid   
        LEFT JOIN
            (
SELECT
                    uid,
                    SUM(weekday_day_time + weekday_eve_time) AS p1_wk,
                    SUM(weekend_day_time + weekend_eve_time) AS p1_we   
                FROM
                    stay_poi   
                WHERE
                    city = 'V0110000'   
                    AND date = '20220801'   
                    AND ptype = 1   
                GROUP BY
                    uid   
            ) s1   
                ON u.uid = s1.uid   
        LEFT JOIN
            (
SELECT
                    uid,
                    SUM(weekday_day_time + weekday_eve_time) AS p2_wk,
                    SUM(weekend_day_time + weekend_eve_time) AS p2_we   
                FROM
                    stay_poi   
                WHERE
                    city = 'V0110000'   
                    AND date = '20220801'   
                    AND ptype = 2   
                GROUP BY
                    uid   
            ) s2   
                ON u.uid = s2.uid   
        JOIN
            user_attribute ua   
                ON ua.uid = u.uid   
                AND ua.city = 'V0110000'   
                AND ua.date = 20220801   
        GROUP BY
            ua.age ) t; 
-- Age, November
SELECT
        '20231101' AS stat_date,
        'V0110000' AS city,
        t.age,
        CASE   
            WHEN t.group_n < 15 THEN '≤15人'   
            ELSE CAST(t.group_n AS STRING)   
        END AS group_n_display,
        /* 工作日占比 */
  CASE   
            WHEN t.wk_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.wk_p0 / t.wk_total,
            2)   
        END AS weekday_p0_share_pct,
        CASE   
            WHEN t.wk_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.wk_p1 / t.wk_total,
            2)   
        END AS weekday_p1_share_pct,
        CASE   
            WHEN t.wk_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.wk_p2 / t.wk_total,
            2)   
        END AS weekday_p2_share_pct,
        /* 周末占比 */
  CASE   
            WHEN t.we_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.we_p0 / t.we_total,
            2)   
        END AS weekend_p0_share_pct,
        CASE   
            WHEN t.we_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.we_p1 / t.we_total,
            2)   
        END AS weekend_p1_share_pct,
        CASE   
            WHEN t.we_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.we_p2 / t.we_total,
            2)   
        END AS weekend_p2_share_pct   
    FROM
        ( SELECT
            ua.age,
            COUNT(DISTINCT u.uid) AS group_n,
            /* 先在 uid 层聚，再在组内取均值 */
  AVG(COALESCE(s0.p0_wk,
            0)) AS wk_p0,
            AVG(COALESCE(s1.p1_wk,
            0)) AS wk_p1,
            AVG(COALESCE(s2.p2_wk,
            0)) AS wk_p2,
            AVG(COALESCE(s0.p0_we,
            0)) AS we_p0,
            AVG(COALESCE(s1.p1_we,
            0)) AS we_p1,
            AVG(COALESCE(s2.p2_we,
            0)) AS we_p2,
            /* 分母：工作日/周末总时长（均值后再求和） */
  (AVG(COALESCE(s0.p0_wk,
            0)) + AVG(COALESCE(s1.p1_wk,
            0)) + AVG(COALESCE(s2.p2_wk,
            0))) AS wk_total,
            (AVG(COALESCE(s0.p0_we,
            0)) + AVG(COALESCE(s1.p1_we,
            0)) + AVG(COALESCE(s2.p2_we,
            0))) AS we_total   
        FROM
            ( SELECT
                DISTINCT sp.uid   
            FROM
                stay_poi sp   
            JOIN
                xmu_hh_subsample_grids_202311 g   
                    ON sp.final_grid_id = g.final_grid_id   
            WHERE
                sp.city = 'V0110000'   
                AND sp.date = '20231101'   
                AND sp.ptype = 1 ) u   
        LEFT JOIN
            (
SELECT
                    uid,
                    SUM(weekday_day_time + weekday_eve_time) AS p0_wk,
                    SUM(weekend_day_time + weekend_eve_time) AS p0_we   
                FROM
                    stay_poi   
                WHERE
                    city = 'V0110000'   
                    AND date = '20231101'   
                    AND ptype = 0   
                GROUP BY
                    uid   
            ) s0   
                ON u.uid = s0.uid   
        LEFT JOIN
            (
SELECT
                    uid,
                    SUM(weekday_day_time + weekday_eve_time) AS p1_wk,
                    SUM(weekend_day_time + weekend_eve_time) AS p1_we   
                FROM
                    stay_poi   
                WHERE
                    city = 'V0110000'   
                    AND date = '20231101'   
                    AND ptype = 1   
                GROUP BY
                    uid   
            ) s1   
                ON u.uid = s1.uid   
        LEFT JOIN
            (
SELECT
                    uid,
                    SUM(weekday_day_time + weekday_eve_time) AS p2_wk,
                    SUM(weekend_day_time + weekend_eve_time) AS p2_we   
                FROM
                    stay_poi   
                WHERE
                    city = 'V0110000'   
                    AND date = '20231101'   
                    AND ptype = 2   
                GROUP BY
                    uid   
            ) s2   
                ON u.uid = s2.uid   
        JOIN
            user_attribute ua   
                ON ua.uid = u.uid   
                AND ua.city = 'V0110000'   
                AND ua.date = 20231101   
        GROUP BY
            ua.age ) t; 
-- 3.3 is_local 
-- is_local, August
SELECT
        '20220801' AS stat_date,
        'V0110000' AS city,
        t.is_local,
        CASE   
            WHEN t.group_n < 15 THEN '≤15人'   
            ELSE CAST(t.group_n AS STRING)   
        END AS group_n_display,
        CASE   
            WHEN t.wk_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.wk_p0 / t.wk_total,
            2)   
        END AS weekday_p0_share_pct,
        CASE   
            WHEN t.wk_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.wk_p1 / t.wk_total,
            2)   
        END AS weekday_p1_share_pct,
        CASE   
            WHEN t.wk_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.wk_p2 / t.wk_total,
            2)   
        END AS weekday_p2_share_pct,
        CASE   
            WHEN t.we_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.we_p0 / t.we_total,
            2)   
        END AS weekend_p0_share_pct,
        CASE   
            WHEN t.we_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.we_p1 / t.we_total,
            2)   
        END AS weekend_p1_share_pct,
        CASE   
            WHEN t.we_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.we_p2 / t.we_total,
            2)   
        END AS weekend_p2_share_pct   
    FROM
        ( SELECT
            ua.is_local,
            COUNT(DISTINCT u.uid) AS group_n,
            AVG(COALESCE(s0.p0_wk,
            0)) AS wk_p0,
            AVG(COALESCE(s1.p1_wk,
            0)) AS wk_p1,
            AVG(COALESCE(s2.p2_wk,
            0)) AS wk_p2,
            AVG(COALESCE(s0.p0_we,
            0)) AS we_p0,
            AVG(COALESCE(s1.p1_we,
            0)) AS we_p1,
            AVG(COALESCE(s2.p2_we,
            0)) AS we_p2,
            (AVG(COALESCE(s0.p0_wk,
            0)) + AVG(COALESCE(s1.p1_wk,
            0)) + AVG(COALESCE(s2.p2_wk,
            0))) AS wk_total,
            (AVG(COALESCE(s0.p0_we,
            0)) + AVG(COALESCE(s1.p1_we,
            0)) + AVG(COALESCE(s2.p2_we,
            0))) AS we_total   
        FROM
            ( SELECT
                DISTINCT sp.uid   
            FROM
                stay_poi sp   
            JOIN
                xmu_hh_subsample_grids_202208 g   
                    ON sp.final_grid_id = g.final_grid_id   
            WHERE
                sp.city = 'V0110000'   
                AND sp.date = '20220801'   
                AND sp.ptype = 1 ) u   
        LEFT JOIN
            (
SELECT
                    uid,
                    SUM(weekday_day_time + weekday_eve_time) AS p0_wk,
                    SUM(weekend_day_time + weekend_eve_time) AS p0_we   
                FROM
                    stay_poi   
                WHERE
                    city = 'V0110000'   
                    AND date = '20220801'   
                    AND ptype = 0   
                GROUP BY
                    uid   
            ) s0   
                ON u.uid = s0.uid   
        LEFT JOIN
            (
SELECT
                    uid,
                    SUM(weekday_day_time + weekday_eve_time) AS p1_wk,
                    SUM(weekend_day_time + weekend_eve_time) AS p1_we   
                FROM
                    stay_poi   
                WHERE
                    city = 'V0110000'   
                    AND date = '20220801'   
                    AND ptype = 1   
                GROUP BY
                    uid   
            ) s1   
                ON u.uid = s1.uid   
        LEFT JOIN
            (
SELECT
                    uid,
                    SUM(weekday_day_time + weekday_eve_time) AS p2_wk,
                    SUM(weekend_day_time + weekend_eve_time) AS p2_we   
                FROM
                    stay_poi   
                WHERE
                    city = 'V0110000'   
                    AND date = '20220801'   
                    AND ptype = 2   
                GROUP BY
                    uid   
            ) s2   
                ON u.uid = s2.uid   
        JOIN
            user_attribute ua   
                ON ua.uid = u.uid   
                AND ua.city = 'V0110000'   
                AND ua.date = 20220801   
        GROUP BY
            ua.is_local ) t; 
-- is_local, November
SELECT
        '20231101' AS stat_date,
        'V0110000' AS city,
        t.is_local,
        CASE   
            WHEN t.group_n < 15 THEN '≤15人'   
            ELSE CAST(t.group_n AS STRING)   
        END AS group_n_display,
        CASE   
            WHEN t.wk_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.wk_p0 / t.wk_total,
            2)   
        END AS weekday_p0_share_pct,
        CASE   
            WHEN t.wk_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.wk_p1 / t.wk_total,
            2)   
        END AS weekday_p1_share_pct,
        CASE   
            WHEN t.wk_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.wk_p2 / t.wk_total,
            2)   
        END AS weekday_p2_share_pct,
        CASE   
            WHEN t.we_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.we_p0 / t.we_total,
            2)   
        END AS weekend_p0_share_pct,
        CASE   
            WHEN t.we_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.we_p1 / t.we_total,
            2)   
        END AS weekend_p1_share_pct,
        CASE   
            WHEN t.we_total = 0   
            OR t.group_n < 15 THEN NULL   
            ELSE ROUND(100.0 * t.we_p2 / t.we_total,
            2)   
        END AS weekend_p2_share_pct   
    FROM
        ( SELECT
            ua.is_local,
            COUNT(DISTINCT u.uid) AS group_n,
            AVG(COALESCE(s0.p0_wk,
            0)) AS wk_p0,
            AVG(COALESCE(s1.p1_wk,
            0)) AS wk_p1,
            AVG(COALESCE(s2.p2_wk,
            0)) AS wk_p2,
            AVG(COALESCE(s0.p0_we,
            0)) AS we_p0,
            AVG(COALESCE(s1.p1_we,
            0)) AS we_p1,
            AVG(COALESCE(s2.p2_we,
            0)) AS we_p2,
            (AVG(COALESCE(s0.p0_wk,
            0)) + AVG(COALESCE(s1.p1_wk,
            0)) + AVG(COALESCE(s2.p2_wk,
            0))) AS wk_total,
            (AVG(COALESCE(s0.p0_we,
            0)) + AVG(COALESCE(s1.p1_we,
            0)) + AVG(COALESCE(s2.p2_we,
            0))) AS we_total   
        FROM
            ( SELECT
                DISTINCT sp.uid   
            FROM
                stay_poi sp   
            JOIN
                xmu_hh_subsample_grids_202311 g   
                    ON sp.final_grid_id = g.final_grid_id   
            WHERE
                sp.city = 'V0110000'   
                AND sp.date = '20231101'   
                AND sp.ptype = 1 ) u   
        LEFT JOIN
            (
SELECT
                    uid,
                    SUM(weekday_day_time + weekday_eve_time) AS p0_wk,
                    SUM(weekend_day_time + weekend_eve_time) AS p0_we   
                FROM
                    stay_poi   
                WHERE
                    city = 'V0110000'   
                    AND date = '20231101'   
                    AND ptype = 0   
                GROUP BY
                    uid   
            ) s0   
                ON u.uid = s0.uid   
        LEFT JOIN
            (
SELECT
                    uid,
                    SUM(weekday_day_time + weekday_eve_time) AS p1_wk,
                    SUM(weekend_day_time + weekend_eve_time) AS p1_we   
                FROM
                    stay_poi   
                WHERE
                    city = 'V0110000'   
                    AND date = '20231101'   
                    AND ptype = 1   
                GROUP BY
                    uid   
            ) s1   
                ON u.uid = s1.uid   
        LEFT JOIN
            (
SELECT
                    uid,
                    SUM(weekday_day_time + weekday_eve_time) AS p2_wk,
                    SUM(weekend_day_time + weekend_eve_time) AS p2_we   
                FROM
                    stay_poi   
                WHERE
                    city = 'V0110000'   
                    AND date = '20231101'   
                    AND ptype = 2   
                GROUP BY
                    uid   
            ) s2   
                ON u.uid = s2.uid   
        JOIN
            user_attribute ua   
                ON ua.uid = u.uid   
                AND ua.city = 'V0110000'   
                AND ua.date = 20231101   
        GROUP BY
            ua.is_local ) t; 
-- 4. Move patterns
-- 4.1 Identifying chained trips
-- August 
DROP TABLE IF EXISTS xmu_chained_trips_202208; 
CREATE TABLE xmu_chained_trips_202208 AS SELECT
        '202208' AS stat_month,
        'V0110000' AS city,
        t.uid,
        t.date AS trip_date,
        /* 段1（当前行） */
  t.start_grid_id AS seg1_start_grid_id,
        t.end_grid_id AS seg1_end_grid_id,
        t.stime AS seg1_stime,
        t.etime AS seg1_etime,
        /* 段2（相邻下一行） */
  t.next_start_grid_id AS seg2_start_grid_id,
        t.next_end_grid_id AS seg2_end_grid_id,
        t.next_stime AS seg2_stime,
        t.next_etime AS seg2_etime,
        /* 间隔（秒） */
  CAST(UNIX_TIMESTAMP(t.next_stime) - UNIX_TIMESTAMP(t.etime) AS BIGINT) AS inter_trip_gap_sec 
    FROM
        (  SELECT
            m.uid,
            m.date,
            m.start_grid_id,
            m.end_grid_id,
            m.stime,
            m.etime,
            LEAD(m.start_grid_id) OVER (PARTITION 
        BY
            m.uid 
        ORDER BY
            m.stime) AS next_start_grid_id,
            LEAD(m.end_grid_id) OVER (PARTITION 
        BY
            m.uid 
        ORDER BY
            m.stime) AS next_end_grid_id,
            LEAD(m.stime) OVER (PARTITION 
        BY
            m.uid 
        ORDER BY
            m.stime) AS next_stime,
            LEAD(m.etime) OVER (PARTITION 
        BY
            m.uid 
        ORDER BY
            m.stime) AS next_etime  
        FROM
            move_month m  
        JOIN
            (
SELECT
                    DISTINCT uid  
                FROM
                    xmu_hh_members_202208  
            ) hh 
                ON m.uid = hh.uid  
        WHERE
            m.city = 'V0110000'  
            AND m.date BETWEEN 20220801 AND 20220831 
        ) t 
    WHERE
        t.end_grid_id = t.next_start_grid_id  
        AND t.etime < t.next_stime  
        AND UNIX_TIMESTAMP(t.next_stime) - UNIX_TIMESTAMP(t.etime) < 43200; 
-- 0.5 天
-- November 
DROP TABLE IF EXISTS xmu_chained_trips_202311; 
CREATE TABLE xmu_chained_trips_202311 AS SELECT
        '202311' AS stat_month,
        'V0110000' AS city,
        t.uid,
        t.date AS trip_date,
        t.start_grid_id AS seg1_start_grid_id,
        t.end_grid_id AS seg1_end_grid_id,
        t.stime AS seg1_stime,
        t.etime AS seg1_etime,
        t.next_start_grid_id AS seg2_start_grid_id,
        t.next_end_grid_id AS seg2_end_grid_id,
        t.next_stime AS seg2_stime,
        t.next_etime AS seg2_etime,
        CAST(UNIX_TIMESTAMP(t.next_stime) - UNIX_TIMESTAMP(t.etime) AS BIGINT) AS inter_trip_gap_sec 
    FROM
        (  SELECT
            m.uid,
            m.date,
            m.start_grid_id,
            m.end_grid_id,
            m.stime,
            m.etime,
            LEAD(m.start_grid_id) OVER (PARTITION 
        BY
            m.uid 
        ORDER BY
            m.stime) AS next_start_grid_id,
            LEAD(m.end_grid_id) OVER (PARTITION 
        BY
            m.uid 
        ORDER BY
            m.stime) AS next_end_grid_id,
            LEAD(m.stime) OVER (PARTITION 
        BY
            m.uid 
        ORDER BY
            m.stime) AS next_stime,
            LEAD(m.etime) OVER (PARTITION 
        BY
            m.uid 
        ORDER BY
            m.stime) AS next_etime  
        FROM
            move_month m  
        JOIN
            (
SELECT
                    DISTINCT uid  
                FROM
                    xmu_hh_members_202311  
            ) hh 
                ON m.uid = hh.uid  
        WHERE
            m.city = 'V0110000'  
            AND m.date BETWEEN 20231101 AND 20231130 
        ) t 
    WHERE
        t.end_grid_id = t.next_start_grid_id  
        AND t.etime < t.next_stime  
        AND UNIX_TIMESTAMP(t.next_stime) - UNIX_TIMESTAMP(t.etime) < 43200; 
-- Basic analysis about chained trips 
-- Demographics 
-- August 
SELECT
        ct.city,
        ct.stat_month,
        ua.gender,
        ua.is_local,
        /* 合规：<15 置空，人数脱敏显示 */
  CASE 
            WHEN COUNT(DISTINCT ct.uid) < 15 THEN NULL  
            ELSE ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT ct.uid),
            4)  
        END AS avg_chained_trips_per_person,
        CASE 
            WHEN COUNT(DISTINCT ct.uid) < 15 THEN '≤15人'  
            ELSE CAST(COUNT(DISTINCT ct.uid) AS STRING)  
        END AS group_n_display 
    FROM
        xmu_chained_trips_202208 ct 
    JOIN
        user_attribute ua  
            ON ua.uid = ct.uid  
            AND ua.city = ct.city  
            AND ua.date = 20220801 
    GROUP BY
        ct.city,
        ct.stat_month,
        ua.gender,
        ua.is_local 
    ORDER BY
        ct.city,
        ct.stat_month,
        ua.gender,
        ua.is_local; 
-- November 
SELECT
        ct.city,
        ct.stat_month,
        ua.gender,
        ua.is_local,
        /* 合规：<15 置空，人数脱敏显示 */
  CASE 
            WHEN COUNT(DISTINCT ct.uid) < 15 THEN NULL  
            ELSE ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT ct.uid),
            4)  
        END AS avg_chained_trips_per_person,
        CASE 
            WHEN COUNT(DISTINCT ct.uid) < 15 THEN '≤15人'  
            ELSE CAST(COUNT(DISTINCT ct.uid) AS STRING)  
        END AS group_n_display 
    FROM
        xmu_chained_trips_202311 ct 
    JOIN
        user_attribute ua  
            ON ua.uid = ct.uid  
            AND ua.city = ct.city  
            AND ua.date = 20231101 
    GROUP BY
        ct.city,
        ct.stat_month,
        ua.gender,
        ua.is_local 
    ORDER BY
        ct.city,
        ct.stat_month,
        ua.gender,
        ua.is_local; 
-- 4.2 Percentage of co-movements 
-- August 
-- 清理旧表（如需）
DROP TABLE IF EXISTS tmp_mm_aug_base;
DROP TABLE IF EXISTS tmp_mm_aug_seq;
DROP TABLE IF EXISTS tmp_mm_aug_grp;
DROP TABLE IF EXISTS tmp_mm_aug_agg;
DROP TABLE IF EXISTS xmu_comovements_202208;

-- 1) 裁剪 move_month，仅保留必要列 + 成员 + 月份范围
CREATE TABLE tmp_mm_aug_base AS
SELECT
  m.uid,
  m.city,
  m.date,
  m.start_grid_id,
  m.end_grid_id,
  m.stime,
  m.etime
FROM move_month m
JOIN (SELECT DISTINCT uid FROM xmu_hh_members_202208) hh
  ON m.uid = hh.uid
WHERE m.city = 'V0110000'
  AND m.date BETWEEN 20220801 AND 20220831;

-- 2) 计算上一条 stime 与是否开新组标记
CREATE TABLE tmp_mm_aug_seq AS
SELECT
  t.uid,
  t.city,
  t.date,
  t.start_grid_id,
  t.end_grid_id,
  t.stime,
  t.etime,
  LAG(t.stime) OVER (
    PARTITION BY t.city, t.date, t.start_grid_id, t.end_grid_id
    ORDER BY t.stime
  ) AS prev_stime,
  CASE
    WHEN LAG(t.stime) OVER (
           PARTITION BY t.city, t.date, t.start_grid_id, t.end_grid_id
           ORDER BY t.stime
         ) IS NULL
      OR UNIX_TIMESTAMP(t.stime) - UNIX_TIMESTAMP(
           LAG(t.stime) OVER (
             PARTITION BY t.city, t.date, t.start_grid_id, t.end_grid_id
             ORDER BY t.stime
           )
         ) > 15
    THEN 1 ELSE 0
  END AS is_new_group
FROM tmp_mm_aug_base t;

-- 3) 对 is_new_group 做前缀和得到组号 grp_seq
CREATE TABLE tmp_mm_aug_grp AS
SELECT
  city,
  date,
  start_grid_id,
  end_grid_id,
  uid,
  stime,
  etime,
  SUM(is_new_group) OVER (
    PARTITION BY city, date, start_grid_id, end_grid_id
    ORDER BY stime
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS grp_seq
FROM tmp_mm_aug_seq;

-- 4) 计算每组的 event_anchor 与 group_size（行上带组属性）
CREATE TABLE tmp_mm_aug_agg AS
SELECT
  g.city,
  g.date,
  g.start_grid_id,
  g.end_grid_id,
  g.uid,
  g.stime,
  g.etime,
  g.grp_seq,
  MIN(g.stime) OVER (
    PARTITION BY g.city, g.date, g.start_grid_id, g.end_grid_id, g.grp_seq
  ) AS event_anchor,
  COUNT(1) OVER (
    PARTITION BY g.city, g.date, g.start_grid_id, g.end_grid_id, g.grp_seq
  ) AS group_size
FROM tmp_mm_aug_grp g;

-- 5) 生成最终结果，按 event_anchor 编 comovement_id，并过滤组规模≥2
CREATE TABLE xmu_comovements_202208 AS
SELECT
  '202208' AS stat_month,
  a.city,
  a.date,
  a.start_grid_id,
  a.end_grid_id,
  DENSE_RANK() OVER (
    PARTITION BY a.city, a.date, a.start_grid_id, a.end_grid_id
    ORDER BY a.event_anchor
  ) AS comovement_id,
  a.event_anchor,
  a.group_size,
  a.uid,
  a.stime,
  a.etime
FROM tmp_mm_aug_agg a
WHERE a.group_size >= 2;

-- November
DROP TABLE IF EXISTS tmp_mm_nov_base;
DROP TABLE IF EXISTS tmp_mm_nov_seq;
DROP TABLE IF EXISTS tmp_mm_nov_grp;
DROP TABLE IF EXISTS tmp_mm_nov_agg;
DROP TABLE IF EXISTS xmu_comovements_202311;

CREATE TABLE tmp_mm_nov_base AS
SELECT
  m.uid,
  m.city,
  m.date,
  m.start_grid_id,
  m.end_grid_id,
  m.stime,
  m.etime
FROM move_month m
JOIN (SELECT DISTINCT uid FROM xmu_hh_members_202311) hh
  ON m.uid = hh.uid
WHERE m.city = 'V0110000'
  AND m.date BETWEEN 20231101 AND 20231130;

CREATE TABLE tmp_mm_nov_seq AS
SELECT
  t.uid,
  t.city,
  t.date,
  t.start_grid_id,
  t.end_grid_id,
  t.stime,
  t.etime,
  LAG(t.stime) OVER (
    PARTITION BY t.city, t.date, t.start_grid_id, t.end_grid_id
    ORDER BY t.stime
  ) AS prev_stime,
  CASE
    WHEN LAG(t.stime) OVER (
           PARTITION BY t.city, t.date, t.start_grid_id, t.end_grid_id
           ORDER BY t.stime
         ) IS NULL
      OR UNIX_TIMESTAMP(t.stime) - UNIX_TIMESTAMP(
           LAG(t.stime) OVER (
             PARTITION BY t.city, t.date, t.start_grid_id, t.end_grid_id
             ORDER BY t.stime
           )
         ) > 15
    THEN 1 ELSE 0
  END AS is_new_group
FROM tmp_mm_nov_base t;

CREATE TABLE tmp_mm_nov_grp AS
SELECT
  city,
  date,
  start_grid_id,
  end_grid_id,
  uid,
  stime,
  etime,
  SUM(is_new_group) OVER (
    PARTITION BY city, date, start_grid_id, end_grid_id
    ORDER BY stime
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS grp_seq
FROM tmp_mm_nov_seq;

CREATE TABLE tmp_mm_nov_agg AS
SELECT
  g.city,
  g.date,
  g.start_grid_id,
  g.end_grid_id,
  g.uid,
  g.stime,
  g.etime,
  g.grp_seq,
  MIN(g.stime) OVER (
    PARTITION BY g.city, g.date, g.start_grid_id, g.end_grid_id, g.grp_seq
  ) AS event_anchor,
  COUNT(1) OVER (
    PARTITION BY g.city, g.date, g.start_grid_id, g.end_grid_id, g.grp_seq
  ) AS group_size
FROM tmp_mm_nov_grp g;

CREATE TABLE xmu_comovements_202311 AS
SELECT
  '202311' AS stat_month,
  a.city,
  a.date,
  a.start_grid_id,
  a.end_grid_id,
  DENSE_RANK() OVER (
    PARTITION BY a.city, a.date, a.start_grid_id, a.end_grid_id
    ORDER BY a.event_anchor
  ) AS comovement_id,
  a.event_anchor,
  a.group_size,
  a.uid,
  a.stime,
  a.etime
FROM tmp_mm_nov_agg a
WHERE a.group_size >= 2;

-- August, comovement_events  
SELECT
  '202208' AS stat_month,
  rows.sample_size_rows,
  stats.comovement_events,
  stats.avg_group_size,
  stats.max_group_size,
  stats.min_group_size
FROM
  ( SELECT COUNT(1) AS sample_size_rows
    FROM xmu_comovements_202208
  ) rows
CROSS JOIN
  ( SELECT
      COUNT(1)                                               AS comovement_events,
      ROUND(AVG(ev.group_size), 2)                           AS avg_group_size,
      MAX(ev.group_size)                                     AS max_group_size,
      MIN(ev.group_size)                                     AS min_group_size
    FROM (
      SELECT
        a.city,
        a.date,
        a.start_grid_id,
        a.end_grid_id,
        a.comovement_id,
        a.event_anchor,
        MAX(a.group_size) AS group_size
      FROM xmu_comovements_202208 a
      GROUP BY
        a.city,
        a.date,
        a.start_grid_id,
        a.end_grid_id,
        a.comovement_id,
        a.event_anchor
    ) ev
  ) stats;

-- November, comovement_events 
SELECT
  '202311' AS stat_month,
  rows.sample_size_rows,
  stats.comovement_events,
  stats.avg_group_size,
  stats.max_group_size,
  stats.min_group_size
FROM
  ( SELECT COUNT(1) AS sample_size_rows
    FROM xmu_comovements_202311
  ) rows
CROSS JOIN
  ( SELECT
      COUNT(1)                                               AS comovement_events,
      ROUND(AVG(ev.group_size), 2)                           AS avg_group_size,
      MAX(ev.group_size)                                     AS max_group_size,
      MIN(ev.group_size)                                     AS min_group_size
    FROM (
      SELECT
        a.city,
        a.date,
        a.start_grid_id,
        a.end_grid_id,
        a.comovement_id,
        a.event_anchor,
        MAX(a.group_size) AS group_size   -- 每个事件的组规模（同事件内各行相同，取 MAX 稳妥）
      FROM xmu_comovements_202311 a
      GROUP BY
        a.city,
        a.date,
        a.start_grid_id,
        a.end_grid_id,
        a.comovement_id,
        a.event_anchor
    ) ev
  ) stats;

-- Comovement Percentage 
SELECT
  t08.total_trips_202208,
  c08.comov_trips_202208,
  CASE
    WHEN t08.total_trips_202208 = 0 THEN NULL
    ELSE ROUND(100.0 * c08.comov_trips_202208 / t08.total_trips_202208, 2)
  END AS pct_comovements_202208,

  t11.total_trips_202311,
  c11.comov_trips_202311,
  CASE
    WHEN t11.total_trips_202311 = 0 THEN NULL
    ELSE ROUND(100.0 * c11.comov_trips_202311 / t11.total_trips_202311, 2)
  END AS pct_comovements_202311
FROM
  ( SELECT COUNT(1) AS total_trips_202208
    FROM move_month
    WHERE city = 'V0110000'
      AND date BETWEEN 20220801 AND 20220831
  ) t08
CROSS JOIN
  ( SELECT COUNT(1) AS comov_trips_202208
    FROM xmu_comovements_202208
    WHERE city = 'V0110000'
  ) c08
CROSS JOIN
  ( SELECT COUNT(1) AS total_trips_202311
    FROM move_month
    WHERE city = 'V0110000'
      AND date BETWEEN 20231101 AND 20231130
  ) t11
CROSS JOIN
  ( SELECT COUNT(1) AS comov_trips_202311
    FROM xmu_comovements_202311
    WHERE city = 'V0110000'
  ) c11;

-- 4.2 Overlapping sample, 202208 and 202311 
SELECT
  t.total_uid_202208,
  r.retained_uid,
  CASE
    WHEN t.total_uid_202208 = 0 THEN NULL
    ELSE ROUND(100.0 * r.retained_uid / t.total_uid_202208, 2)
  END AS pct_retained
FROM
  /* 202208 北京的去重 UID 总数 */
  (
    SELECT COUNT(1) AS total_uid_202208
    FROM (
      SELECT DISTINCT uid
      FROM xmu_hh_members_202208
      WHERE city = 'V0110000'
    ) a
  ) t
CROSS JOIN
  /* 同时出现在 202311 北京样本的去重 UID 数（留存） */
  (
    SELECT COUNT(1) AS retained_uid
    FROM (
      SELECT DISTINCT h08.uid
      FROM xmu_hh_members_202208 h08
      INNER JOIN xmu_hh_members_202311 h11
        ON h08.uid = h11.uid
      WHERE h08.city = 'V0110000'
        AND h11.city = 'V0110000'
    ) b
  ) r;

-- 4.3 App Usage       
-- August 
DROP TABLE IF EXISTS xmu_app_usage_202208;
CREATE TABLE xmu_app_usage_202208 AS
SELECT
  '202208'                AS stat_month,
  uli.city,
  uli.date,
  uli.uid,
  -- 标签编目信息
  uli.lcode               AS full_label_code,
  lc.label_code,
  lc.label_name,
  lc.full_label_name,
  -- 使用强度
  uli.ltime,
  uli.lflux
FROM user_label_info uli
JOIN (SELECT DISTINCT uid FROM xmu_hh_members_202208) hh
  ON uli.uid = hh.uid
JOIN label_codes lc
  ON uli.lcode = lc.full_label_code
WHERE uli.city = 'V0110000'
  AND uli.date BETWEEN 20220801 AND 20220831;

-- August 带ua
DROP TABLE IF EXISTS xmu_app_usage_ua_202208;
CREATE TABLE xmu_app_usage_ua_202208 AS
SELECT
    a.stat_month,
    a.city,
    a.date,
    a.uid,
    a.full_label_code,
    a.label_code,
    a.label_name,
    a.full_label_name,
    a.ltime,
    a.lflux,
    ua.gender,
    ua.age
FROM xmu_app_usage_202208 a
JOIN user_attribute ua
    ON a.uid = ua.uid;

-- November 
DROP TABLE IF EXISTS xmu_app_usage_202311;
CREATE TABLE xmu_app_usage_202311 AS
SELECT
  '202311'                AS stat_month,
  uli.city,
  uli.date,
  uli.uid,
  uli.lcode               AS full_label_code,
  lc.label_code,
  lc.label_name,
  lc.full_label_name,
  uli.ltime,
  uli.lflux
FROM user_label_info uli
JOIN (SELECT DISTINCT uid FROM xmu_hh_members_202311) hh
  ON uli.uid = hh.uid
JOIN label_codes lc
  ON uli.lcode = lc.full_label_code
WHERE uli.city = 'V0110000'
  AND uli.date BETWEEN 20231101 AND 20231130;

-- November, 带ua
DROP TABLE IF EXISTS xmu_app_usage_ua_202208;
CREATE TABLE xmu_app_usage_ua_202311 AS
SELECT
    a.stat_month,
    a.city,
    a.date,
    a.uid,
    a.full_label_code,
    a.label_code,
    a.label_name,
    a.full_label_name,
    a.ltime,
    a.lflux,
    ua.gender,
    ua.age
FROM xmu_app_usage_202311 a
JOIN user_attribute ua
    ON a.uid = ua.uid;
    
-- August, gender, label_name
SELECT
  '202208'            AS stat_month,
  s.gender_int        AS gender,
  s.label_name,
  AVG(s.label_ltime)  AS avg_ltime_per_user
FROM (
  SELECT
    a.uid,
    CAST(a.gender AS INT) AS gender_int,
    a.label_name,
    SUM(a.ltime)          AS label_ltime
  FROM xmu_app_usage_ua_202208 a
  WHERE CAST(a.gender AS INT) IN (1, 2)
  GROUP BY
    a.uid,
    CAST(a.gender AS INT),
    a.label_name
) s
GROUP BY
  s.gender_int,
  s.label_name
ORDER BY
  s.gender_int,
  s.label_name;
  
-- November, gender, label_name
SELECT
  '202311'            AS stat_month,
  s.gender_int        AS gender,
  s.label_name,
  AVG(s.label_ltime)  AS avg_ltime_per_user
FROM (
  SELECT
    a.uid,
    CAST(a.gender AS INT) AS gender_int,
    a.label_name,
    SUM(a.ltime)          AS label_ltime
  FROM xmu_app_usage_ua_202311 a
  WHERE CAST(a.gender AS INT) IN (1, 2)
  GROUP BY
    a.uid,
    CAST(a.gender AS INT),
    a.label_name
) s
GROUP BY
  s.gender_int,
  s.label_name
ORDER BY
  s.gender_int,
  s.label_name;
  
-- August, gender, full 
SELECT
  '202208'            AS stat_month,
  s.gender_int        AS gender,
  s.full_label_name,
  AVG(s.label_ltime)  AS avg_ltime_per_user
FROM (
  SELECT
    a.uid,
    CAST(a.gender AS INT) AS gender_int,
    a.full_label_name,
    SUM(a.ltime)          AS label_ltime
  FROM xmu_app_usage_ua_202208 a
  WHERE CAST(a.gender AS INT) IN (1, 2)
  GROUP BY
    a.uid,
    CAST(a.gender AS INT),
    a.full_label_name
) s
GROUP BY
  s.gender_int,
  s.full_label_name
ORDER BY
  s.gender_int,
  s.full_label_name;
  
-- November, gender, full 
SELECT
  '202311'            AS stat_month,
  s.gender_int        AS gender,
  s.full_label_name,
  AVG(s.label_ltime)  AS avg_ltime_per_user
FROM (
  SELECT
    a.uid,
    CAST(a.gender AS INT) AS gender_int,
    a.full_label_name,
    SUM(a.ltime)          AS label_ltime
  FROM xmu_app_usage_ua_202311 a
  WHERE CAST(a.gender AS INT) IN (1, 2)
  GROUP BY
    a.uid,
    CAST(a.gender AS INT),
    a.full_label_name
) s
GROUP BY
  s.gender_int,
  s.full_label_name
ORDER BY
  s.gender_int,
  s.full_label_name;