# Credit Risk Portfolio Analysis
**Tools:** SQL Server · T-SQL · Power BI · DAX  
**Dataset:** GiveMeSomeCredit — Kaggle (150,000 records)

## Project Overview
End-to-end credit risk analytics pipeline simulating a mid-sized 
bank engagement. Built a complete data cleaning, business analysis, 
and executive reporting solution across four phases.

## Business Problem
A 150,000 record loan book with significant data quality issues 
needed to be transformed into actionable risk intelligence for 
the Chief Risk Officer and collections team.

## Key Findings
- Mid-Career borrowers represent 38% of the portfolio but generate 
  52% of all defaults — a 13.62 point concentration gap
- Tier 5 utilization borrowers default at 20x the rate of Tier 1
- Young borrowers carry the highest default rate at 11.73% — 
  nearly 5x the rate of Retired borrowers
- 19.82% NULL rate in MonthlyIncome required median imputation 
  before any income-based analysis was valid

## Technical Highlights
- Three-CTE NULL imputation pipeline using PERCENTILE_CONT 
  for median and mode imputation
- Window functions: NTILE, RANK, DENSE_RANK, LAG, LEAD
- Diagnosed floating-point boundary gaps in CASE WHEN bucketing
- Isolated DebtRatio artifacts reaching 329,664 from near-zero 
  income denominators
- Star schema Power BI model with 12 DAX measures
- Three-page interactive dashboard with dynamic slicer filtering

## Dashboard Pages
| Page | Audience | Purpose |
|---|---|---|
| Executive Summary | CRO / Board | Portfolio KPIs and concentration risk |
| Risk Analysis | Risk Team | Tier breakdown and debt burden analysis |
| Borrower Detail | Collections | Filterable watch list with risk flags |

## Repository Structure
- /sql — All T-SQL queries organized by phase
- /powerbi — Power BI report file (.pbix)
- /screenshots — Dashboard preview images

## Data Source
GiveMeSomeCredit dataset from Kaggle. Due to licensing the raw 
data is not included. Download instructions at:
https://www.kaggle.com/competitions/GiveMeSomeCredit/data
