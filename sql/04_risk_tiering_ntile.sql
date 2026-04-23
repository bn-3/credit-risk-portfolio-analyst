/*
============================================================
Project:     Credit Risk Portfolio Analysis
File:        04_risk_tiering_ntile.sql
Phase:       2 - SQL Business Analysis
Task:        Borrower Risk Tiering
============================================================

Business Purpose:
    Segment all 149,999 borrowers into 5 equal utilization-based
    risk tiers to identify default rate distribution across the
    portfolio. Surface the top 3 highest-utilization borrowers
    per tier for targeted relationship manager review.
    Answers the CRO's request to stop viewing the loan book
    as a flat list and instead see it tiered by risk.

Key Techniques:
    - Derived capping column (Utilization_Capped) isolates
      artifact borrowers above 2.0 from tier assignment without
      modifying source data or removing rows
    - NTILE(5) assigns equal-population tiers globally --
      no PARTITION BY needed since split is across all borrowers
    - DENSE_RANK() with PARTITION BY RiskTier resets rank
      counter per tier and handles ties without skipping ranks
    - Raw column used for DENSE_RANK ORDER BY to differentiate
      borrowers who share the same capped ceiling value of 2.0
      -- prevents all capped borrowers from tying at Rank 1

Capping Design Decision:
    RevolvingUtilizationOfUnsecuredLines contains extreme outliers
    reaching 50,708 caused by near-zero credit limit denominators.
    Cap of 2.0 was chosen because:
    - Values between 1.0 and 2.0 are genuine over-limit borrowers
      and represent a real risk signal worth preserving
    - Values above 2.0 are artifacts -- mathematically inflated
      ratios with no meaningful risk interpretation
    - Cap applies to tier assignment only -- raw values are
      preserved in all output columns for full transparency

Output:
    Query 1 -- One row per tier showing BorrowerCount,
               AvgUtilization, DefaultCount, DefaultRate_Pct
    Query 2 -- Top 3 highest-utilization borrowers per tier
               showing RawUtilization, Utilization_Capped,
               default status, and within-tier rank

Key Findings:
    - Default rate climbs from 1.94% in Tier 1 to 19.88%
      in Tier 5 -- a 10x increase driven purely by revolving
      utilization behavior
    - All three Tier 5 top borrowers show utilization above
      22,000 and zero defaults -- confirming artifact status
    - Tier 4 and Tier 5 combined account for 41,287 high
      risk borrowers representing the primary collections target
============================================================
*/

-- ============================================================
-- Query 1: Tier Summary
-- Aggregate default rate and borrower count per risk tier
-- ============================================================

WITH utilization_capped AS (
    SELECT *,
        -- Cap applied for tier assignment only
        -- Raw column preserved for ranking and output
        -- 2.0 threshold chosen to retain genuine over-limit
        -- borrowers while excluding denominator artifacts
        CASE 
            WHEN RevolvingUtilizationOfUnsecuredLines > 2.0 THEN 2.0
            ELSE RevolvingUtilizationOfUnsecuredLines
        END AS Utilization_Capped
    FROM cs_loans_clean
),
tiers AS (
    SELECT *,
        -- No PARTITION BY -- global split across all borrowers
        -- Capped column used so outliers don't distort tier
        -- boundaries and push legitimate borrowers into Tier 4
        NTILE(5) OVER (ORDER BY Utilization_Capped) AS RiskTier
    FROM utilization_capped
)
SELECT 
    RiskTier,
    COUNT(*) AS BorrowerCount,
    -- AVG on capped column gives meaningful tier averages
    -- Raw column average would be distorted by artifacts in Tier 5
    ROUND(AVG(Utilization_Capped), 4) AS AvgUtilization,
    -- SUM on binary flag is cleaner than COUNT(CASE WHEN)
    -- when column is already 0/1
    SUM(CAST(SeriousDlqin2yrs AS INT)) AS DefaultCount,
    -- 100.0 forces decimal arithmetic throughout
    ROUND(SUM(CAST(SeriousDlqin2yrs AS INT)) * 100.0 / COUNT(*), 2) AS DefaultRate_Pct
FROM tiers
GROUP BY RiskTier
ORDER BY RiskTier;

-- ============================================================
-- Query 2: Top 3 Borrowers per Tier
-- Surfaces highest-utilization individuals within each tier
-- for relationship manager and collections team review
-- ============================================================

WITH utilization_capped AS (
    SELECT *,
        CASE 
            WHEN RevolvingUtilizationOfUnsecuredLines > 2.0 THEN 2.0
            ELSE RevolvingUtilizationOfUnsecuredLines
        END AS Utilization_Capped
    FROM cs_loans_clean
),
tiers AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY Utilization_Capped) AS RiskTier
    FROM utilization_capped
),
tiers_ranked AS (
    SELECT *,
        -- PARTITION BY resets rank counter for each tier
        -- ORDER BY raw column (not capped) so borrowers who
        -- share the 2.0 ceiling are differentiated by their
        -- actual utilization -- prevents mass tie at Rank 1
        -- DENSE_RANK chosen over RANK so no rank numbers are
        -- skipped when ties exist at lower positions
        DENSE_RANK() OVER (
            PARTITION BY RiskTier 
            ORDER BY RevolvingUtilizationOfUnsecuredLines DESC
        ) AS Rnk
    FROM tiers
)
SELECT 
    RiskTier,
    id AS BorrowerID,
    RevolvingUtilizationOfUnsecuredLines AS RawUtilization,
    -- Both columns shown so reviewer can see original value
    -- alongside the capped value used for tier assignment
    Utilization_Capped,
    SeriousDlqin2yrs,
    Rnk
FROM tiers_ranked
-- Filter applied here not in CTE because window functions
-- cannot be filtered in the same SELECT they are defined in
WHERE Rnk <= 3
ORDER BY RiskTier, Rnk