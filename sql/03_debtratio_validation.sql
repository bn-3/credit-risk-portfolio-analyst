/*
============================================================
Project:     Credit Risk Portfolio Analysis
File:        03_debtratio_validation.sql
Phase:       1 - Data Modeling & Cleaning
Task:        DebtRatio Bucketing & Outlier Validation
============================================================

Business Purpose:
    Validate the clean staging table by segmenting borrowers
    into DebtRatio risk buckets and confirming default rates
    rise logically with debt burden. Identify and isolate
    data artifacts caused by near-zero income denominators
    inflating DebtRatio into meaningless territory before
    Phase 2 business analysis begins.

Key Techniques:
    - CASE WHEN bucketing using >= / < boundary pattern
      instead of BETWEEN to prevent floating-point gap
      misclassification
    - Two-CTE chain: bucket assignment then aggregation
    - Conditional aggregation with ELSE 0 for reliable
      SUM on binary default flag
    - Custom ORDER BY CASE to enforce logical risk sequence
      instead of alphabetical sort

Boundary Design Decisions:
    - >= / < used instead of BETWEEN on floating point columns
      BETWEEN is inclusive on both ends and creates silent gaps
      e.g. 0.9900018 falls between 0.99 and 1.00 matching
      nothing -- diagnosed and resolved during development
    - Extreme/Artifact bucket isolates DebtRatio > 5.0 where
      values reach 329,664 due to near-zero income denominators
      These are data artifacts, not genuine high-debt borrowers
    - Unknown bucket captures NULLs -- 18 records with 0 defaults

Output:
    One row per bucket showing BorrowerCount, DefaultCount,
    and DefaultRate_Pct ordered Low through Extreme/Artifact

Key Findings:
    - Default rate rises logically Low (5.82%) to High (11.40%)
      confirming clean table is analytically sound
    - Extreme/Artifact default rate (5.54%) is lower than Medium
      (8.32%) -- confirms these are artifacts not genuine risk
    - Max DebtRatio in Extreme bucket reaches 329,664 --
      diagnosed as near-zero income denominator inflation
    - 117 borrowers were initially misclassified due to
      floating-point gap between 0.99 and 1.00 -- resolved
      by replacing BETWEEN with >= / < boundary pattern
============================================================
*/

WITH DebtRatio_Calc AS (
    SELECT *,
        -- >= / < boundary pattern prevents floating-point gaps
        -- BETWEEN 0.35 AND 0.99 silently missed values like
        -- 0.9900018 -- discovered and corrected during validation
        CASE 
            WHEN DebtRatio < 0.35 THEN 'Low'
            WHEN DebtRatio >= 0.35 AND DebtRatio < 1.00 THEN 'Medium'
            WHEN DebtRatio >= 1.00 AND DebtRatio <= 5.00 THEN 'High'
            WHEN DebtRatio > 5.00 THEN 'Extreme/Artifact'
            ELSE 'Unknown'  -- catches NULLs only after >= / < fix
        END AS DebtRatio_Bucket
    FROM cs_loans_clean
),

BorrowerCount_Calc AS (
    SELECT 
        DebtRatio_Bucket,
        COUNT(*) AS BorrowerCount,
        -- ELSE 0 ensures non-defaulted rows contribute 0 to SUM
        -- omitting ELSE returns NULL for false rows, which SUM
        -- ignores -- produces correct result but is fragile practice
        SUM(CASE WHEN SeriousDlqin2yrs = 1 THEN 1 ELSE 0 END) AS DefaultCount
    FROM DebtRatio_Calc
    GROUP BY DebtRatio_Bucket
)

SELECT
    DebtRatio_Bucket,
    BorrowerCount,
    DefaultCount,
    -- 100.0 forces decimal arithmetic -- integer division would
    -- truncate result to 0 before multiplication
    ROUND(DefaultCount * 100.0 / BorrowerCount, 2) AS DefaultRate_Pct
FROM BorrowerCount_Calc
-- Alphabetical ORDER BY would sort High before Low
-- CASE expression enforces logical risk progression
ORDER BY 
    CASE DebtRatio_Bucket
        WHEN 'Low' THEN 1
        WHEN 'Medium' THEN 2
        WHEN 'High' THEN 3
        WHEN 'Extreme/Artifact' THEN 4
        WHEN 'Unknown' THEN 5
    END