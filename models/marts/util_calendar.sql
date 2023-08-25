WITH

nums AS (
    SELECT 0 AS n UNION ALL
    SELECT 1 UNION ALL
    SELECT 2 UNION ALL
    SELECT 3 UNION ALL
    SELECT 4 UNION ALL
    SELECT 5 UNION ALL
    SELECT 6 UNION ALL
    SELECT 7 UNION ALL
    SELECT 8 UNION ALL
    SELECT 9
),

nums10000 AS (
    SELECT n1.n * 10000 + n2.n * 1000 + n3.n * 100 + n4.n * 10 + n5.n AS num
    FROM nums n1
    CROSS JOIN nums n2
    CROSS JOIN nums n3
    CROSS JOIN nums n4
    CROSS JOIN nums n5
),

dates AS (
    SELECT DATE_ADD('1980-01-01', num) AS date_day
    FROM nums10000
    WHERE num <= 43829 -- number of days from 1980-01-01 to 2099-12-31
),

transformed_dates AS (
    SELECT 
        date_day,
        DATE_PART('day', date_day) AS day_of_month,
        DATE_PART('month', date_day) AS month_number,
        DATE_FORMAT(date_day, 'MMMM') AS month_name,
        DATE_TRUNC('month', date_day) AS month_start,
        DATE_SUB(DATE_TRUNC('month', DATEADD(MONTH, 1, date_day)), 1) AS month_end,
        DATE_FORMAT(date_day, 'MMM yyyy') AS month,
        CONCAT('Q', DATE_PART('quarter', date_day), ' ', DATE_FORMAT(date_day, 'yyyy')) AS quarter,
        DATE_TRUNC('quarter', date_day) AS quarter_start,
        DATE_SUB(DATE_TRUNC('quarter', DATEADD(QUARTER, 1, date_day)), 1) AS quarter_end,
        DATE_PART('quarter', date_day) AS quarter_number,
        DATE_PART('year', date_day) AS year,
        CASE 
            WHEN date_day = CAST(CURRENT_DATE() AS DATE) THEN 'Today'
            WHEN date_day = CAST(DATE_ADD(CURRENT_DATE(), -1) AS DATE) THEN 'Yesterday'
            WHEN date_day < CAST(CURRENT_DATE() AS DATE) THEN 'Past'
            WHEN date_day > CAST(CURRENT_DATE() AS DATE) THEN 'Future'
            ELSE NULL
        END AS is_today,
        DATEDIFF(date_day, CAST(CURRENT_DATE() AS DATE)) AS days_from_current_date,
        DATEDIFF(CAST(CURRENT_DATE() AS DATE), date_day) AS days_to_current_date,
        DATE_PART('year', CAST(CURRENT_DATE() AS DATE)) - DATE_PART('year', date_day) AS years_ago,
        CASE 
            WHEN date_day >= DATE_TRUNC('month', CAST(CURRENT_DATE() AS DATE)) AND date_day <= LAST_DAY(CAST(CURRENT_DATE() AS DATE)) THEN 'Current Month'
            WHEN date_day < DATE_TRUNC('month', CAST(CURRENT_DATE() AS DATE)) THEN 'Past Months'
            WHEN date_day > LAST_DAY(CAST(CURRENT_DATE() AS DATE)) THEN 'Future Months'
            ELSE NULL
        END AS is_current_month
    FROM dates
)

SELECT * FROM transformed_dates