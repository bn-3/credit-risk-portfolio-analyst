/*
============================================================
Project:     Credit Risk Portfolio Analysis
File:        06_collections_watchlist_lead.sql
Phase:       2 - SQL Business Analysis
Task:        Collections Watch List with Peer Chaining
============================================================

Business Purpose:
    Build an actionable watch list for the collections team
    by identifying borrowers with confirmed delinquency history
    and ranking them by debt burden within each age band.
    LEAD() surfaces each borrower's next-ranked peer so
    collections managers can see who is coming up behind each
    high-risk borrower on the priority list -- enabling
    sequential risk triage without requiring additional queries.

Key Techniques:
    - Population filter applied inside CTE 1 before any window
      functions run -- cleaner and more efficient than filtering
      in a WHERE clause after ranking
    - RANK() with PARTITION BY AgeBand resets rank counter per
      band -- each age segment has its own independent ranking
    - Three LEAD() calls sharing identical OVER() clauses --
      all partitioned by AgeBand and ordered by DebtRatio DESC
      to maintain consistent peer reference direction
    - LEAD() returns NULL for the last ranked borrower in each
      band -- no next peer exists, NULL is correct behavior
    - DebtRatio < 5.0 filter excludes Extreme/Artifact records
      identified in Phase 1 -- borrowers with ratios reaching
      329,664 due to near-zero income denominators would
      dominate every band's top ranks if included

Filter Design Decisions:
    NumberOfTimes90DaysLate >= 1 scopes the watch list to
    borrowers with confirmed severe delinquency -- 90+ days
    late is the same threshold used by SeriousDlqin2yrs and
    represents the collections team's primary trigger event.
    DebtRatio < 5.0 (strictly less than) excludes the artifact
    boundary -- using <= 5.0 included borrowers at exactly 5.0
    who were artifact cases, diagnosed during development and
    corrected by tightening to strict less than.

RANK vs DENSE_RANK Decision:
    RANK() is used here because ties at the same DebtRatio
    represent genuinely equal debt burden -- both borrowers
    deserve the same priority rank. DENSE_RANK() would also
    work correctly. ROW_NUMBER() was intentionally avoided
    because it would arbitrarily break ties and misrepresent
    equal-risk borrowers as having different priority levels.

Output:
    One row per qualifying borrower ordered by AgeBand then
    DebtRank showing BorrowerID, DebtRatio, default status,
    within-band rank, and next peer's ID, DebtRatio, and
    default status. Last borrower per band shows NULL for
    all three LEAD columns.

Key Findings:
    - High debt burden does not guarantee default -- many
      top-ranked borrowers show SeriousDlqin2yrs = 0,
      confirming DebtRatio alone is insufficient for
      collections prioritization without delinquency filter
    - Tied ranks at round DebtRatio values (4.0, 5.0) signal
      manual data entry -- these are real borrowers, not
      artifacts, and correctly share the same rank
    - Young band tops out at 4.44 DebtRatio -- well below
      artifact threshold, confirming clean population
============================================================
*/

WITH age_tiers AS (
    SELECT *,
        -- BETWEEN safe for integer age column
        -- Retired catches all ages 70+ via ELSE
        CASE 
            WHEN age < 30 THEN 'Young'
            WHEN age BETWEEN 30 AND 49 THEN 'Mid-Career'
            WHEN age BETWEEN 50 AND 69 THEN 'Senior'
            ELSE 'Retired'
        END AS AgeBand
    FROM cs_loans_clean
    -- Filters applied here before window functions run
    -- NumberOfTimes90DaysLate >= 1 scopes to confirmed
    -- delinquent borrowers only -- collections target population
    -- DebtRatio < 5.0 (strict) excludes artifact boundary --
    -- <= 5.0 included exact 5.0 values that were artifact cases
    WHERE NumberOfTimes90DaysLate >= 1
      AND DebtRatio < 5.0
),

ranking AS (
    SELECT
        AgeBand,
        id AS BorrowerID,
        DebtRatio,
        SeriousDlqin2yrs,
        -- PARTITION BY resets rank counter for each age band
        -- ORDER BY DESC surfaces highest debt burden first
        -- Ties receive equal rank -- intentional, equal debt
        -- burden = equal collections priority
        RANK() OVER (PARTITION BY AgeBand ORDER BY DebtRatio DESC) AS DebtRank,
        -- All three LEAD() calls share identical OVER() clause
        -- ensuring peer reference is always the next borrower
        -- in the same band at the next debt rank position
        -- Returns NULL for last ranked borrower in each band
        LEAD(id) OVER (PARTITION BY AgeBand ORDER BY DebtRatio DESC) AS Next_BorrowerID,
        LEAD(DebtRatio) OVER (PARTITION BY AgeBand ORDER BY DebtRatio DESC) AS Next_DebtRatio,
        LEAD(SeriousDlqin2yrs) OVER (PARTITION BY AgeBand ORDER BY DebtRatio DESC) AS Next_DefaultStatus
    FROM age_tiers
)

SELECT *
FROM ranking
-- AgeBand first groups bands together visually
-- DebtRank second shows priority order within each band
ORDER BY AgeBand, DebtRank