/*
============================================================
Project:     Credit Risk Portfolio Analysis
File:        02_null_imputation.sql
Phase:       1 - Data Modeling & Cleaning
Task:        NULL Imputation & Clean Staging Table Creation
============================================================

Business Purpose:
    Deliver a clean, analysis-ready staging table by resolving
    the two NULL problems identified in Phase 1 profiling.
    Rows cannot be dropped without losing a significant slice
    of the default population, so statistically appropriate
    imputation is applied instead. One invalid age record is
    permanently excluded.

Key Techniques:
    - PERCENTILE_CONT(0.5) for median calculation -- chosen over
      mean because MonthlyIncome is right-skewed by high earners
    - SELECT DISTINCT to collapse PERCENTILE_CONT window output
      from one value per row to a single scalar value
    - TOP 1 ORDER BY COUNT(*) DESC for mode calculation
    - CROSS JOIN on single-row CTEs to apply scalar imputed
      values across all 149,999 rows in one pass
    - ISNULL() to replace NULLs while preserving non-null values
    - SELECT INTO to create and populate staging table in one
      statement

Output:
    cs_loans_clean -- 149,999 row staging table with:
    - Zero NULLs in MonthlyIncome (imputed with median)
    - Zero NULLs in NumberOfDependents (imputed with mode)
    - age = 0 record permanently excluded

Key Findings:
    - Median MonthlyIncome used as imputed value -- resistant
      to outlier distortion unlike mean
    - CROSS JOIN is safe here because both CTEs return exactly
      one row -- joining many-to-one produces no row duplication
    - Post-clean verification confirms 149,999 rows, 0 NULLs,
      0 bad age records
============================================================
*/

WITH MedianIncome AS (
    -- PERCENTILE_CONT returns one value per row as a window function
    -- SELECT DISTINCT collapses repeated values to a single scalar
    -- WHERE clause ensures NULLs are excluded from median calculation
    SELECT DISTINCT
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY MonthlyIncome) 
        OVER () AS MedianValue
    FROM cstraining
    WHERE MonthlyIncome IS NOT NULL
),

ModeDepend AS (
    -- Mode = most frequently occurring non-NULL value
    -- GROUP BY counts occurrences, TOP 1 DESC selects the winner
    SELECT TOP 1
        NumberOfDependents AS ModeValue
    FROM cstraining
    WHERE NumberOfDependents IS NOT NULL
    GROUP BY NumberOfDependents
    ORDER BY COUNT(*) DESC
)

-- Final clean table creation
-- ISNULL replaces NULLs with imputed values, non-NULLs pass through unchanged
-- CROSS JOIN works safely here -- both CTEs are guaranteed single-row results
-- WHERE age <> 0 permanently excludes the one invalid age record
SELECT
    t.column1,
    t.SeriousDlqin2yrs,
    ISNULL(t.MonthlyIncome, m.MedianValue) AS MonthlyIncome,
    ISNULL(t.NumberOfDependents, d.ModeValue) AS NumberOfDependents,
    t.age,
    t.DebtRatio,
    t.RevolvingUtilizationOfUnsecuredLines,
    t.NumberOfOpenCreditLinesAndLoans,
    t.NumberOfTimes90DaysLate,
    t.NumberRealEstateLoansOrLines,
    t.NumberOfTime60_89DaysPastDueNotWorse,
    t.NumberOfTime30_59DaysPastDueNotWorse
INTO cs_loans_clean
FROM cstraining AS t
CROSS JOIN MedianIncome AS m
CROSS JOIN ModeDepend AS d
WHERE t.age <> 0

/*
------------------------------------------------------------
Post-Load Verification Query
Run after SELECT INTO to confirm clean table meets
all expected data quality standards before Phase 2 begins
------------------------------------------------------------
*/

SELECT 
    COUNT(*) AS TotalRecords,
    -- Both should return 0 -- confirms imputation was successful
    COUNT(*) - COUNT(MonthlyIncome) AS RemainingNullIncome,
    COUNT(*) - COUNT(NumberOfDependents) AS RemainingNullDependents,
    -- Should return 0 -- confirms age filter worked correctly
    COUNT(CASE WHEN age = 0 THEN 1 END) AS BadAge_Count,
    -- Post-clean income stats -- average will be pulled slightly
    -- toward median compared to raw table due to imputed rows
    ROUND(AVG(CAST(MonthlyIncome AS FLOAT)), 2) AS AvgIncome_PostClean,
    MIN(MonthlyIncome) AS MinIncome,
    -- MaxIncome near 3,008,750 is a known outlier -- flagged for
    -- Phase 2 risk segmentation but not removed from clean table
    MAX(MonthlyIncome) AS MaxIncome
FROM cs_loans_clean