WITH nums AS (
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
  SELECT DATE_ADD('1980-01-01', num) AS Date
  FROM nums10000
  WHERE num <= 43829 -- number of days from 1980-01-01 to 2099-12-31
),
transformed_dates AS (
  SELECT 
    Date AS DateKey,
    Date,
    DATE_PART('day', Date) AS DayOfMonth,
    DATE_PART('month', Date) AS MonthNumber,
    DATE_FORMAT(Date, 'MMMM') AS MonthName,
    DATE_TRUNC('month', Date) AS MonthStart,
    DATE_SUB(DATE_TRUNC('month', DATEADD(MONTH, 1, Date)), 1) AS MonthEnd,
    DATE_FORMAT(Date, 'MMM yyyy') AS Month,
    CONCAT('Q', DATE_PART('quarter', Date), ' ', DATE_FORMAT(Date, 'yyyy')) AS Quarter,
    DATE_TRUNC('quarter', Date) AS QuarterStart,
    DATE_SUB(DATE_TRUNC('quarter', DATEADD(QUARTER, 1, Date)), 1) AS QuarterEnd,
    DATE_PART('quarter', Date) AS QuarterNumber,
    DATE_PART('year', Date) AS Year,
    NOW() AS CurrentDateTime,
    CURRENT_DATE() AS CurrentDate,
    CASE 
        WHEN Date = CAST(CURRENT_DATE() AS DATE) THEN 'Today'
        WHEN Date = CAST(DATE_ADD(CURRENT_DATE(), -1) AS DATE) THEN 'Yesterday'
        WHEN Date < CAST(CURRENT_DATE() AS DATE) THEN 'Past'
        WHEN Date > CAST(CURRENT_DATE() AS DATE) THEN 'Future'
        ELSE NULL
    END AS IsToday,
    DATEDIFF(Date, CAST(CURRENT_DATE() AS DATE)) AS DaysFromCurrentDate,
    DATEDIFF(CAST(CURRENT_DATE() AS DATE), Date) AS DaysToCurrentDate,
    CASE
        WHEN DaysFromCurrentDate < -181 THEN 'Last 181+ days'
        WHEN DaysFromCurrentDate BETWEEN -180 AND -31 THEN 'Last 31-180 days'
        WHEN DaysFromCurrentDate BETWEEN -30 AND -1 THEN 'Last 30 days'
        WHEN DaysFromCurrentDate BETWEEN 0 AND 30 THEN 'Next 30 days'
        WHEN DaysFromCurrentDate BETWEEN 31 AND 180 THEN 'Next 31-180 days'
        WHEN DaysFromCurrentDate > 181 THEN 'Next 181+ days'
    END AS FuturePastPeriod,
    CASE 
        WHEN Date >= DATEADD(MONTH, -13, DATE_TRUNC('month', CAST(CURRENT_DATE() AS DATE))) AND Date <= LAST_DAY(CAST(CURRENT_DATE() AS DATE)) THEN 'Last13Months'
        WHEN Date < DATEADD(MONTH, -13, DATE_TRUNC('month', CAST(CURRENT_DATE() AS DATE))) THEN 'Before13Months'
        ELSE 'MonthsInFuture'
    END AS IsLast13Months,
    DATE_PART('year', CAST(CURRENT_DATE() AS DATE)) - DATE_PART('year', Date) AS YearsAgo,
    CASE 
        WHEN Date >= DATE_TRUNC('month', CAST(CURRENT_DATE() AS DATE)) AND Date <= LAST_DAY(CAST(CURRENT_DATE() AS DATE)) THEN 'CurrentMonth'
        WHEN Date < DATE_TRUNC('month', CAST(CURRENT_DATE() AS DATE)) THEN 'PastMonths'
        WHEN Date > LAST_DAY(CAST(CURRENT_DATE() AS DATE)) THEN 'FutureMonths'
        ELSE NULL
    END AS IsCurrentMonth
  FROM 
    dates
)

SELECT * FROM transformed_dates