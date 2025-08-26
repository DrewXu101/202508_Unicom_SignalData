-- Clear Hive Storage 
DROP TABLE IF EXISTS xmu_chained_trips_202311; 
DROP TABLE IF EXISTS xmu_chained_trips_202208; 
DROP TABLE IF EXISTS xmu_app_usage_ua_202311; 
DROP TABLE IF EXISTS xmu_app_usage_ua_202208; 
DROP TABLE IF EXISTS xmu_app_usage_202311; 
DROP TABLE IF EXISTS xmu_app_usage_202208; 
DROP TABLE IF EXISTS tmp_mm_nov_agg; 
DROP TABLE IF EXISTS tmp_monthly_pairs_202311; 
DROP TABLE IF EXISTS tmp_monthly_pairs_202208; 
DROP TABLE IF EXISTS tmp_mm_nov_grp; 
DROP TABLE IF EXISTS tmp_mm_nov_seq; 
DROP TABLE IF EXISTS tmp_mm_nov_base; 
DROP TABLE IF EXISTS tmp_mm_aug_agg; 
DROP TABLE IF EXISTS tmp_mm_aug_grp; 
DROP TABLE IF EXISTS tmp_mm_aug_seq; 
DROP TABLE IF EXISTS tmp_mm_aug_base; 
-- August
-- 清理（可选）
DROP TABLE IF EXISTS home_map_202208_tmp; 
DROP TABLE IF EXISTS moves_202208_tmp; 
DROP TABLE IF EXISTS home_times_202208_tmp; 
DROP TABLE IF EXISTS seq_202208_tmp; 
DROP TABLE IF EXISTS tagged_202208_tmp; 
DROP TABLE IF EXISTS chained_trips_202208; 
-- 1) 202208 的 home 映射
CREATE TABLE home_map_202208_tmp AS SELECT
        uid,
        final_grid_id AS home_grid   
    FROM
        xmu_hh_members_202208   
    GROUP BY
        uid,
        final_grid_id; 
-- 2) 关联 202208 的出行记录（仅北京），停留点=出行终点，带出所需字段
CREATE TABLE moves_202208_tmp AS SELECT
        m.uid,
        m.date,
        m.stime,
        m.etime,
        m.end_grid_id AS final_grid_id,
        h.home_grid,
        '202208' AS stat_month,
        m.distance,
        m.time,
        m.is_core,
        m.moi_id,
        m.start_ptype,
        m.end_ptype,
        m.province,
        ua.gender,
        ua.age,
        CASE   
            WHEN m.end_grid_id = h.home_grid THEN 1   
            ELSE 0   
        END AS is_home   
    FROM
        move_month m   
    JOIN
        home_map_202208_tmp h   
            ON m.uid = h.uid   
    LEFT JOIN
        user_attribute ua -- <== 新增
    
            ON m.uid = ua.uid   
    WHERE
        substr(m.date,1,6) = '202208'   
        AND m.city = 'V0110000'; 
-- 3) “到达 home 的时间点”（去重）
CREATE TABLE home_times_202208_tmp AS SELECT
        uid,
        date,
        stime AS home_stime   
    FROM
        ( SELECT
            DISTINCT uid,
            date,
            stime   
        FROM
            moves_202208_tmp   
        WHERE
            is_home = 1 ) t; 
-- 4) 计算 home_cum（不使用窗口函数）
CREATE TABLE seq_202208_tmp AS SELECT
        a.uid,
        a.date,
        a.stime,
        a.etime,
        a.final_grid_id,
        a.home_grid,
        a.is_home,
        a.distance,
        a.time,
        a.is_core,
        a.moi_id,
        a.start_ptype,
        a.end_ptype,
        a.province,
        a.gender,
-- <== 新增
        a.age,
-- <== 新增
  SUM(CASE   
            WHEN b.home_stime <= a.stime THEN 1   
            ELSE 0   
        END) AS home_cum   
    FROM
        moves_202208_tmp a   
    LEFT JOIN
        home_times_202208_tmp b   
            ON a.uid = b.uid   
            AND a.date = b.date   
    GROUP BY
        a.uid,
        a.date,
        a.stime,
        a.etime,
        a.final_grid_id,
        a.home_grid,
        a.is_home,
        a.distance,
        a.time,
        a.is_core,
        a.moi_id,
        a.start_ptype,
        a.end_ptype,
        a.province,
        a.gender,
        a.age; 
-- 5) 生成链编号
CREATE TABLE tagged_202208_tmp AS SELECT
        uid,
        date,
        stime,
        etime,
        final_grid_id,
        home_grid,
        is_home,
        distance,
        time,
        is_core,
        moi_id,
        start_ptype,
        end_ptype,
        province,
        gender,
-- <== 新增
  age,
-- <== 新增
  CASE   
            WHEN is_home = 1   
            AND home_cum > 1 THEN home_cum - 1   
            ELSE home_cum   
        END AS chain_trip_id   
    FROM
        seq_202208_tmp; 
-- 6) 输出链内明细表
CREATE TABLE chained_trips_202208 AS SELECT
        uid,
        date,
        '202208' AS stat_month,
        chain_trip_id,
        stime,
        etime,
        final_grid_id,
        home_grid,
        is_home,
        distance,
        `time`,
        is_core,
        moi_id,
        start_ptype,
        end_ptype,
        province,
        gender,
        age    
    FROM
        tagged_202208_tmp; 
-- November
-- 清理（可选）
DROP TABLE IF EXISTS home_map_202311_tmp; 
DROP TABLE IF EXISTS moves_202311_tmp; 
DROP TABLE IF EXISTS home_times_202311_tmp; 
DROP TABLE IF EXISTS seq_202311_tmp; 
DROP TABLE IF EXISTS tagged_202311_tmp; 
DROP TABLE IF EXISTS chained_trips_202311; 
-- 1) 202311 的 home 映射
CREATE TABLE home_map_202311_tmp AS SELECT
        uid,
        final_grid_id AS home_grid   
    FROM
        xmu_hh_members_202311   
    GROUP BY
        uid,
        final_grid_id; 
-- 2) 关联 202311 的出行记录（仅北京），停留点=出行终点，带出所需字段
DROP TABLE IF EXISTS moves_202311_tmp; 
CREATE TABLE moves_202311_tmp AS SELECT
        m.uid,
        m.date,
        m.stime,
        m.etime,
        m.end_grid_id AS final_grid_id,
        h.home_grid,
        '202311' AS stat_month,
        m.distance,
        m.time,
        m.is_core,
        m.moi_id,
        m.start_ptype,
        m.end_ptype,
        m.province,
        ua.gender,
-- <== 新增
  ua.age,
-- <== 新增
  CASE 
            WHEN m.end_grid_id = h.home_grid THEN 1 
            ELSE 0 
        END AS is_home 
    FROM
        move_month m 
    JOIN
        home_map_202311_tmp h  
            ON m.uid = h.uid 
    LEFT JOIN
        user_attribute ua -- <== 新增
  
            ON m.uid = ua.uid 
    WHERE
        substr(m.date,1,6) = '202311'  
        AND m.city = 'V0110000'; 
-- 3) “到达 home 的时间点”（去重防重复）
CREATE TABLE home_times_202311_tmp AS SELECT
        uid,
        date,
        stime AS home_stime   
    FROM
        ( SELECT
            DISTINCT uid,
            date,
            stime   
        FROM
            moves_202311_tmp   
        WHERE
            is_home = 1 ) t; 
-- 4) 计算 home_cum（不使用窗口函数）
DROP TABLE IF EXISTS seq_202311_tmp; 
CREATE TABLE seq_202311_tmp AS SELECT
        a.uid,
        a.date,
        a.stime,
        a.etime,
        a.final_grid_id,
        a.home_grid,
        a.is_home,
        a.distance,
        a.time,
        a.is_core,
        a.moi_id,
        a.start_ptype,
        a.end_ptype,
        a.province,
        a.gender,
-- <== 新增
  a.age,
-- <== 新增
  SUM(CASE 
            WHEN b.home_stime <= a.stime THEN 1 
            ELSE 0 
        END) AS home_cum 
    FROM
        moves_202311_tmp a 
    LEFT JOIN
        home_times_202311_tmp b  
            ON a.uid = b.uid  
            AND a.date = b.date 
    GROUP BY
        a.uid,
        a.date,
        a.stime,
        a.etime,
        a.final_grid_id,
        a.home_grid,
        a.is_home,
        a.distance,
        a.time,
        a.is_core,
        a.moi_id,
        a.start_ptype,
        a.end_ptype,
        a.province,
        a.gender,
        a.age; 
-- 5) 生成链编号
DROP TABLE IF EXISTS tagged_202311_tmp; 
CREATE TABLE tagged_202311_tmp AS SELECT
        uid,
        date,
        stime,
        etime,
        final_grid_id,
        home_grid,
        is_home,
        distance,
        time,
        is_core,
        moi_id,
        start_ptype,
        end_ptype,
        province,
        gender,
-- <== 新增
  age,
-- <== 新增
  CASE 
            WHEN is_home = 1 
            AND home_cum > 1 THEN home_cum - 1 
            ELSE home_cum 
        END AS chain_trip_id 
    FROM
        seq_202311_tmp; 
-- 6) 输出链内明细表
DROP TABLE IF EXISTS chained_trips_202311; 
CREATE TABLE chained_trips_202311 AS SELECT
        uid,
        date,
        '202311' AS stat_month,
        chain_trip_id,
        stime,
        etime,
        final_grid_id,
        home_grid,
        is_home,
        distance,
        `time`,
        is_core,
        moi_id,
        start_ptype,
        end_ptype,
        province,
        gender,
-- <== 新增
  age -- <== 新增
 
    FROM
        tagged_202311_tmp; 

-- age group aggregation, august

DROP TABLE IF EXISTS chained_trips_202208_ag;
CREATE TABLE chained_trips_202208_ag AS
SELECT
  uid,
  date,
  stat_month,
  chain_trip_id,
  stime,
  etime,
  final_grid_id,
  home_grid,
  is_home,
  distance,
  `time`,
  is_core,
  moi_id,
  start_ptype,
  end_ptype,
  province,
  gender,
  age,
  CASE
    WHEN age IN ('01','02','03','04') THEN 1
    WHEN age IN ('05','06','07')      THEN 2
    WHEN age IN ('08','09','10')      THEN 3
    WHEN age IN ('11','12','13','14','15') THEN 4
    ELSE NULL
  END AS age_group
FROM chained_trips_202208;

-- age group aggregation, november 

DROP TABLE IF EXISTS chained_trips_202311_ag;
CREATE TABLE chained_trips_202311_ag AS
SELECT
  uid,
  date,
  stat_month,
  chain_trip_id,
  stime,
  etime,
  final_grid_id,
  home_grid,
  is_home,
  distance,
  `time`,
  is_core,
  moi_id,
  start_ptype,
  end_ptype,
  province,
  gender,
  age,
  CASE
    WHEN age IN ('01','02','03','04') THEN 1
    WHEN age IN ('05','06','07')      THEN 2
    WHEN age IN ('08','09','10')      THEN 3
    WHEN age IN ('11','12','13','14','15') THEN 4
    ELSE NULL
  END AS age_group
FROM chained_trips_202311;
        
-- Descriptive Analysis 
-- Step 1: 先得到链级别指标
-- 202208
DROP TABLE IF EXISTS chain_summary_202208_ag;
CREATE TABLE chain_summary_202208_ag AS
SELECT
  uid,
  `date`,
  chain_trip_id,
  gender,
  age_group,
  is_core,
  COUNT(*) AS chain_length,                  
  /* 链时长（秒） */
  ( UNIX_TIMESTAMP(MAX(etime)) - UNIX_TIMESTAMP(MIN(stime)) ) AS chain_duration_sec,
  SUM(distance) AS chain_total_distance,     
  MAX(CASE WHEN is_core = '1' AND province = '011' THEN 1 ELSE 0 END) AS is_local
FROM chained_trips_202208_ag
GROUP BY uid, `date`, chain_trip_id, gender, age_group, is_core;

-- 202311
DROP TABLE IF EXISTS chain_summary_202311_ag;
CREATE TABLE chain_summary_202311_ag AS
SELECT
  uid,
  `date`,
  chain_trip_id,
  gender,
  age_group,
  is_core,
  COUNT(*) AS chain_length,
  ( UNIX_TIMESTAMP(MAX(etime)) - UNIX_TIMESTAMP(MIN(stime)) ) AS chain_duration_sec,
  SUM(distance) AS chain_total_distance,
  MAX(CASE WHEN is_core = '1' AND province = '011' THEN 1 ELSE 0 END) AS is_local
FROM chained_trips_202311_ag
GROUP BY uid, `date`, chain_trip_id, gender, age_group, is_core;


-- Step 2: 计算用户层面的每日平均链数、链长度和链距离
-- 202208
DROP TABLE IF EXISTS user_daily_stats_202208_ag;
CREATE TABLE user_daily_stats_202208_ag AS
SELECT
  uid,
  gender,
  age_group,
  is_local,
  is_core,
  CAST(COUNT(DISTINCT chain_trip_id) AS DOUBLE) / COUNT(DISTINCT date) AS avg_daily_chains,
  AVG(chain_length) AS avg_chain_length,
  AVG(chain_total_distance) AS avg_chain_distance
FROM chain_summary_202208_ag
GROUP BY uid, gender, age_group, is_core;

-- 202311
DROP TABLE IF EXISTS user_daily_stats_202311_ag;
CREATE TABLE user_daily_stats_202311_ag AS
SELECT
  uid,
  gender,
  age_group,
  is_local,
  is_core,
  CAST(COUNT(DISTINCT chain_trip_id) AS DOUBLE) / COUNT(DISTINCT date) AS avg_daily_chains,
  AVG(chain_length) AS avg_chain_length,
  AVG(chain_total_distance) AS avg_chain_distance
FROM chain_summary_202311_ag
GROUP BY uid, gender, age_group, is_core;

-- Step 3: 汇总对比本地 vs 外地
-- 202208
SELECT
  gender,
  age_group,
  is_core,
  AVG(avg_daily_chains)  AS mean_daily_chains,
  AVG(avg_chain_length)  AS mean_chain_length,
  AVG(avg_chain_distance) AS mean_chain_distance
FROM user_daily_stats_202208_ag
GROUP BY gender, age_group, is_core
ORDER BY gender, age_group, is_core;

-- 202311
SELECT
  gender,
  age_group,
  is_core,
  AVG(avg_daily_chains)  AS mean_daily_chains,
  AVG(avg_chain_length)  AS mean_chain_length,
  AVG(avg_chain_distance) AS mean_chain_distance
FROM user_daily_stats_202311_ag
GROUP BY gender, age_group, is_core
ORDER BY gender, age_group, is_core;