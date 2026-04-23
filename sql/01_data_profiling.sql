/*
============================================================
Project:     Credit Risk Portfolio Analysis
File:        01_data_profiling.sql
Phase:       1 - Data Modeling & Cleaning
Task:        Raw Data Quality Profiling
============================================================

Business Purpose:
    Establish a baseline data quality snapshot of the raw loan
    book before any cleaning or transformation is applied.
    Identify NULL rates in key financial columns and flag
    obvious invalid records that would corrupt downstream
    analysis if left uncorrected.

Key Techniques:
    - COUNT(*) vs COUNT(column) behavior difference with NULLs
    - Single-pass aggregation -- all metrics in one table scan
    - Float-safe percentage arithmetic using * 100.0
    - Conditional aggregation with CASE WHEN for bad data flagging

Output:
    Single row containing:
    - TotalRecords        : Full row count of raw loan book
    - NullIncome_Count    : Borrowers missing MonthlyIncome
    - NullIncome_Pct      : NULL rate as percentage of total
    - NullDependents_Count: Borrowers missing NumberOfDependents
    - NullDependents_Pct  : NULL rate as percentage of total
    - BadAge_Count        : Borrowers with age = 0 (entry error)

Key Findings:
    - MonthlyIncome:      29,731 NULLs (19.82%) -- median
                          imputation required before any
                          income-based analysis
    - NumberOfDependents: 3,924 NULLs (2.62%) -- mode
                          imputation sufficient
    - BadAge:             1 record -- excluded from clean table
============================================================
*/

SELECT 
    COUNT(*) AS TotalRecords,
    -- COUNT(column) skips NULLs, so subtracting from COUNT(*)
    -- gives us the exact NULL count without a CASE WHEN
    COUNT(*) - COUNT(MonthlyIncome) AS NullIncome_Count,
    -- Multiplying by 100.0 (not 100) forces decimal arithmetic
    -- Integer division would truncate result to 0
    ROUND((COUNT(*) - COUNT(MonthlyIncome)) * 100.0 / COUNT(*), 2) AS NullIncome_Pct,
    COUNT(*) - COUNT(NumberOfDependents) AS NullDependents_Count,
    ROUND((COUNT(*) - COUNT(NumberOfDependents)) * 100.0 / COUNT(*), 2) AS NullDependents_Pct,
    -- CASE WHEN returns NULL when condition is false
    -- COUNT() ignores NULLs, so only age = 0 rows are counted
    COUNT(CASE WHEN age = 0 THEN 1 END) AS BadAge_Count
FROM cstraining