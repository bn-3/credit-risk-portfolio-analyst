/*
============================================================
Project:     Credit Risk Portfolio Analysis
File:        05_age_band_lag_analysis.sql
Phase:       2 - SQL Business Analysis
Task:        Period-over-Period Default Trend Analysis
============================================================

Business Purpose:
    Analyze how default rates and default counts change across
    borrower age bands in sequential order. Provides the CRO
    with a trend view that goes beyond a static snapshot --
    identifying which life stage segments are improving or
    worsening relative to the prior band and by how much.
    Dual metric comparison (count vs rate) prevents volume
    effects from masking genuine risk changes.

Key Techniques:
    - LAG() applied directly to aggregate expressions inside
      a GROUP BY CTE -- SQL processes GROUP BY first, then
      window functions run on the grouped result set
    - Two LAG() calls in the same CTE targeting different
      metrics -- Prev_DefaultCount for volume trend and
      Prev_DefaultRate for rate trend
    - No PARTITION BY on LAG() -- function looks across all
      bands sequentially, not within a group
    - Custom ORDER BY CASE inside OVER() clause enforces
      logical age progression instead of alphabetical sort
    - NULL check on Prev_DefaultCount must come before
      arithmetic conditions in CASE WHEN -- NULL arithmetic
      evaluates to NULL which falls to ELSE, not to the
      correct Baseline label
    - Alias self-reference avoided -- Change_vs_Prior
      expression is repeated inside CASE WHEN because T-SQL
      evaluates all SELECT columns simultaneously and cannot
      reference an alias defined in the same SELECT

Dual Metric Design Decision:
    Change_vs_Prior measures absolute count change and is
    heavily influenced by band population size. Mid-Career
    shows Worsening on count (+4,178) but Improving on rate
    (-2.67%) because it has 6.5x more borrowers than Young.
    Both columns are required for an accurate risk narrative --
    count alone would incorrectly flag Mid-Career as the most
    dangerous segment when Young has the highest individual
    default rate at 11.73%.

Output:
    One row per age band ordered Young through Retired showing
    BorrowerCount, DefaultCount, DefaultRate_Pct,
    Prev_DefaultCount, Change_vs_Prior, Rate_Change_vs_Prior,
    and Trend flag

Key Findings:
    - Young borrowers carry the highest default rate at 11.73%
      -- nearly 5x the Retired rate of 2.32%
    - Mid-Career appears Worsening on count but Improving on
      rate -- a volume effect from 57,560 borrowers, not a
      genuine risk deterioration
    - Senior and Retired show consistent improvement on both
      count and rate -- most stable segments in the portfolio
    - Default rate drops 9.41 percentage points from Young
      to Retired -- strongest signal for age-based underwriting
============================================================
*/

WITH age_tiers AS (
    SELECT *,
        -- BETWEEN is safe for integer age column -- no
        -- floating-point boundary gap risk unlike DebtRatio
        -- Retired catches all ages 70+ via ELSE
        CASE 
            WHEN age < 30 THEN 'Young'
            WHEN age BETWEEN 30 AND 49 THEN 'Mid-Career'
            WHEN age BETWEEN 50 AND 69 THEN 'Senior'
            ELSE 'Retired'
        END AS AgeBand
    FROM cs_loans_clean
),

band_details AS (
    SELECT
        AgeBand,
        COUNT(*) AS BorrowerCount,
        -- CAST to INT ensures SUM treats flag as numeric
        -- SeriousDlqin2yrs is binary 0/1 so SUM = total defaults
        SUM(CAST(SeriousDlqin2yrs AS INT)) AS DefaultCount,
        ROUND(SUM(CAST(SeriousDlqin2yrs AS INT)) * 100.0 / COUNT(*), 2) AS DefaultRate_Pct,
        -- LAG wraps the full aggregate expression because
        -- individual row values no longer exist at this point --
        -- only the grouped results are available to the window
        -- No PARTITION BY -- LAG looks across all bands in sequence
        -- ORDER BY CASE enforces Young(1) > Mid-Career(2) >
        -- Senior(3) > Retired(4) progression not alphabetical
        LAG(SUM(CAST(SeriousDlqin2yrs AS INT))) OVER (ORDER BY
                CASE AgeBand
                    WHEN 'Young' THEN 1
                    WHEN 'Mid-Career' THEN 2
                    WHEN 'Senior' THEN 3
                    WHEN 'Retired' THEN 4
                END
        ) AS Prev_DefaultCount,

        -- Second LAG targets rate instead of count
        -- Enables fair apples-to-apples comparison independent
        -- of band population size differences
        LAG(ROUND(SUM(CAST(SeriousDlqin2yrs AS INT)) * 100.0 
            / COUNT(*), 2)) OVER (ORDER BY
                CASE AgeBand
                    WHEN 'Young' THEN 1
                    WHEN 'Mid-Career' THEN 2
                    WHEN 'Senior' THEN 3
                    WHEN 'Retired' THEN 4
                END
        ) AS Prev_DefaultRate
    FROM age_tiers
    GROUP BY AgeBand
)
SELECT
    AgeBand,
    BorrowerCount,
    DefaultCount,
    DefaultRate_Pct,
    Prev_DefaultCount,
    -- NULL for Young band since no prior band exists
    -- arithmetic on NULL returns NULL which is correct --
    -- Change_vs_Prior for Young displays as NULL not zero
    DefaultCount - Prev_DefaultCount AS Change_vs_Prior,
    ROUND(DefaultRate_Pct - Prev_DefaultRate, 2) AS Rate_Change_vs_Prior,
    -- NULL check must be first -- Prev_DefaultCount IS NULL
    -- for Young band and NULL arithmetic falls to ELSE without
    -- this guard, producing misleading 'No Change' label
    CASE 
        WHEN Prev_DefaultCount IS NULL THEN 'Baseline'
        WHEN (DefaultCount - Prev_DefaultCount) < 0 THEN 'Improving'
        WHEN (DefaultCount - Prev_DefaultCount) > 0 THEN 'Worsening'
        ELSE 'No Change'
    END AS Trend
FROM band_details
-- Repeated ORDER BY CASE required -- alias Change_vs_Prior
-- cannot be referenced here because T-SQL evaluates all
-- SELECT columns simultaneously in the same query level
ORDER BY
    CASE AgeBand
        WHEN 'Young' THEN 1
        WHEN 'Mid-Career' THEN 2
        WHEN 'Senior' THEN 3
        WHEN 'Retired' THEN 4
    END