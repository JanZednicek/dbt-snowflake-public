/* 
-----------------------------------------------------------------------------
  About me: 
    - Powered by Jan Zednicek - https://janzednicek.cz
    - If you find any inconsistencies, please let me know - jsem@janzednicek.cz
    - Feel free to use this code without any limitations for learning (non-commercial use)
      If you find it helpful, consider giving credit by referencing my website
  
  About this table:
    - Table:   d_date
    - Source:  N/A
    - Description: The d_date dimension is a part of a data warehouse and contains information about 
                   individual dates, such as day of the week, month, year, and whether the day is a weekday 
                   or weekend. It is commonly used in analytics and reporting applications for 
                   filtering and organizing data by date.
    - Log
        2024-02-11 - created
-----------------------------------------------------------------------------
*/

WITH DATE_RANGE AS (
  SELECT DATEADD(day, SEQ4(), '1990-01-01')                                 AS DATE
  FROM TABLE(GENERATOR(ROWCOUNT => 22416))
)
SELECT
  DATE 											                            AS SK_DATE,
  DAYOFWEEK(DATE)                                                           AS NUM_DAYOFWEEK,
  CASE WHEN DAYOFWEEK(DATE) IN (1,7) THEN 'Weekend' ELSE 'Weekday' END      AS TEXT_ISWEEKEND,
  EXTRACT(YEAR FROM DATE)                                                   AS NUM_YEAR,
  EXTRACT(QUARTER FROM DATE)                                                AS NUM_QUARTER,
  EXTRACT(MONTH FROM DATE)                                                  AS NUM_MONTH,
  MONTHNAME(DATE)                                                           AS TEXT_MONTH,
  LPAD(TO_CHAR(MONTH(DATE)), 2, '0') || '/' || TO_CHAR(DATE, 'YYYY')        AS CODE_MONTHYEAR,
  QUARTER(DATE) || 'Q/' || TO_CHAR(DATE, 'YYYY')                            AS CODE_QUARTERYEAR
FROM DATE_RANGE