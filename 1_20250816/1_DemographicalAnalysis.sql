-- Gender 
SELECT
        t.month_label AS month,
        t.gender,
        CASE   
            WHEN t.cnt < 15 THEN '≤15人'   
            ELSE CAST(t.cnt AS STRING)   
        END AS freq   
    FROM
        ( SELECT
            CASE   
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'   
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'   
            END AS month_label,
            gender,
            COUNT(DISTINCT uid) AS cnt   
        FROM
            user_attribute   
        WHERE
            city = 'V0110000'   
            AND (
                date BETWEEN 20220801 AND 20220831   
                OR date BETWEEN 20231101 AND 20231130   
            )   
        GROUP BY
            CASE   
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'   
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'   
            END,
            gender ) t   
    WHERE
        t.month_label IS NOT NULL; 
        
-- Age 
SELECT
        t.month_label AS month,
        t.age,
        CASE 
            WHEN t.cnt < 15 THEN '≤15人' 
            ELSE CAST(t.cnt AS STRING) 
        END AS freq 
    FROM
        (  SELECT
            CASE  
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'  
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'  
            END AS month_label,
            age,
            COUNT(DISTINCT uid) AS cnt  
        FROM
            user_attribute  
        WHERE
            city = 'V0110000'  
            AND (
                date BETWEEN 20220801 AND 20220831  
                OR date BETWEEN 20231101 AND 20231130  
            )  
        GROUP BY
            CASE  
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'  
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'  
            END,
            age ) t 
    WHERE
        t.month_label IS NOT NULL; 
-- Area 
SELECT
        t.month_label AS month,
        t.area,
        CASE 
            WHEN t.cnt < 15 THEN '≤15人' 
            ELSE CAST(t.cnt AS STRING) 
        END AS freq 
    FROM
        (  SELECT
            CASE  
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'  
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'  
            END AS month_label,
            area,
            COUNT(DISTINCT uid) AS cnt  
        FROM
            user_attribute  
        WHERE
            city = 'V0110000'  
            AND (
                date BETWEEN 20220801 AND 20220831  
                OR date BETWEEN 20231101 AND 20231130  
            )  
        GROUP BY
            CASE  
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'  
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'  
            END,
            area ) t 
    WHERE
        t.month_label IS NOT NULL; 
-- Brand
SELECT
        t.month_label AS month,
        t.brand,
        CASE 
            WHEN t.cnt < 15 THEN '≤15人' 
            ELSE CAST(t.cnt AS STRING) 
        END AS freq 
    FROM
        (  SELECT
            CASE  
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'  
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'  
            END AS month_label,
            brand,
            COUNT(DISTINCT uid) AS cnt  
        FROM
            user_attribute  
        WHERE
            city = 'V0110000'  
            AND (
                date BETWEEN 20220801 AND 20220831  
                OR date BETWEEN 20231101 AND 20231130  
            )  
        GROUP BY
            CASE  
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'  
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'  
            END,
            brand ) t 
    WHERE
        t.month_label IS NOT NULL; 
-- Type
SELECT
        t.month_label AS month,
        t.type,
        CASE 
            WHEN t.cnt < 15 THEN '≤15人' 
            ELSE CAST(t.cnt AS STRING) 
        END AS freq 
    FROM
        (  SELECT
            CASE  
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'  
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'  
            END AS month_label,
            type,
            COUNT(DISTINCT uid) AS cnt  
        FROM
            user_attribute  
        WHERE
            city = 'V0110000'  
            AND (
                date BETWEEN 20220801 AND 20220831  
                OR date BETWEEN 20231101 AND 20231130  
            )  
        GROUP BY
            CASE  
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'  
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'  
            END,
            type ) t 
    WHERE
        t.month_label IS NOT NULL; 
-- Province
SELECT
        t.month_label AS month,
        t.province,
        CASE 
            WHEN t.cnt < 15 THEN '≤15人' 
            ELSE CAST(t.cnt AS STRING) 
        END AS freq 
    FROM
        (  SELECT
            CASE  
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'  
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'  
            END AS month_label,
            province,
            COUNT(DISTINCT uid) AS cnt  
        FROM
            user_attribute  
        WHERE
            city = 'V0110000'  
            AND (
                date BETWEEN 20220801 AND 20220831  
                OR date BETWEEN 20231101 AND 20231130  
            )  
        GROUP BY
            CASE  
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'  
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'  
            END,
            province ) t 
    WHERE
        t.month_label IS NOT NULL; 
-- is_core 
SELECT
        t.month_label AS month,
        t.is_core,
        CASE 
            WHEN t.cnt < 15 THEN '≤15人' 
            ELSE CAST(t.cnt AS STRING) 
        END AS freq 
    FROM
        (  SELECT
            CASE  
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'  
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'  
            END AS month_label,
            is_core,
            COUNT(DISTINCT uid) AS cnt  
        FROM
            user_attribute  
        WHERE
            city = 'V0110000'  
            AND (
                date BETWEEN 20220801 AND 20220831  
                OR date BETWEEN 20231101 AND 20231130  
            )  
        GROUP BY
            CASE  
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'  
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'  
            END,
            is_core ) t 
    WHERE
        t.month_label IS NOT NULL; 
-- is_local
SELECT
        t.month_label AS month,
        t.is_local,
        CASE 
            WHEN t.cnt < 15 THEN '≤15人' 
            ELSE CAST(t.cnt AS STRING) 
        END AS freq 
    FROM
        (  SELECT
            CASE  
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'  
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'  
            END AS month_label,
            is_local,
            COUNT(DISTINCT uid) AS cnt  
        FROM
            user_attribute  
        WHERE
            city = 'V0110000'  
            AND (
                date BETWEEN 20220801 AND 20220831  
                OR date BETWEEN 20231101 AND 20231130  
            )  
        GROUP BY
            CASE  
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'  
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'  
            END,
            is_local ) t 
    WHERE
        t.month_label IS NOT NULL; 
-- id_area
SELECT
        t.month_label AS month,
        t.id_area,
        CASE 
            WHEN t.cnt < 15 THEN '≤15人' 
            ELSE CAST(t.cnt AS STRING) 
        END AS freq 
    FROM
        (  SELECT
            CASE  
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'  
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'  
            END AS month_label,
            id_area,
            COUNT(DISTINCT uid) AS cnt  
        FROM
            user_attribute  
        WHERE
            city = 'V0110000'  
            AND (
                date BETWEEN 20220801 AND 20220831  
                OR date BETWEEN 20231101 AND 20231130  
            )  
        GROUP BY
            CASE  
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'  
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'  
            END,
            id_area ) t 
    WHERE
        t.month_label IS NOT NULL; 
-- home_district
SELECT
        t.month_label AS month,
        t.home_district,
        CASE 
            WHEN t.cnt < 15 THEN '≤15人' 
            ELSE CAST(t.cnt AS STRING) 
        END AS freq 
    FROM
        (  SELECT
            CASE  
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'  
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'  
            END AS month_label,
            home_district,
            COUNT(DISTINCT uid) AS cnt  
        FROM
            user_attribute  
        WHERE
            city = 'V0110000'  
            AND (
                date BETWEEN 20220801 AND 20220831  
                OR date BETWEEN 20231101 AND 20231130  
            )  
        GROUP BY
            CASE  
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'  
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'  
            END,
            home_district ) t 
    WHERE
        t.month_label IS NOT NULL; 
-- work_district 
SELECT
        t.month_label AS month,
        t.work_district,
        CASE 
            WHEN t.cnt < 15 THEN '≤15人' 
            ELSE CAST(t.cnt AS STRING) 
        END AS freq 
    FROM
        (  SELECT
            CASE  
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'  
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'  
            END AS month_label,
            work_district,
            COUNT(DISTINCT uid) AS cnt  
        FROM
            user_attribute  
        WHERE
            city = 'V0110000'  
            AND (
                date BETWEEN 20220801 AND 20220831  
                OR date BETWEEN 20231101 AND 20231130  
            )  
        GROUP BY
            CASE  
                WHEN date BETWEEN 20220801 AND 20220831 THEN '2022-08'  
                WHEN date BETWEEN 20231101 AND 20231130 THEN '2023-11'  
            END,
            work_district ) t 
    WHERE
        t.month_label IS NOT NULL; 
        
SELECT province_code, province_name
FROM unicom_area_code
WHERE province_code IN ('11', '18', '76', '17', '13')
ORDER BY province_code;

SELECT province_code, province_name
FROM unicom_area_code
WHERE province_name IN ('11', '18', '76', '17', '13')
ORDER BY province_name;

