# Claims Management System - Business Documentation

## Executive Summary

The Claims Management System is an automated data pipeline that processes, validates, and analyzes promotional scheme claims at PNG (Procter & Gamble). This document provides business stakeholders with a clear understanding of the system's functionality, key performance indicators (KPIs), and data flow.

---

## Table of Contents
1. [System Overview](#system-overview)
2. [Data Pipeline Architecture](#data-pipeline-architecture)
3. [Key Business Metrics (KPIs)](#key-business-metrics-kpis)
4. [Scheme-Level Summary](#scheme-level-summary)
5. [Overall Summary & Analytics](#overall-summary--analytics)
6. [Data Validation Rules](#data-validation-rules)
7. [Common Terms & Definitions](#common-terms--definitions)

---

## System Overview

### Purpose
The Claims Management System processes sales data from PNG's distribution network to:
- Track promotional scheme disbursements across different channels
- Validate claims against established business rules
- Calculate disallowances for non-compliant transactions
- Generate comprehensive reports for business analysis

### Key Users
- Finance Team (Settlement & Reconciliation)
- Sales Operations (Scheme Monitoring)
- Regional Management (Performance Analysis)
- Executive Leadership (Strategic Insights)

### Processing Frequency
- **Daily**: Data ingestion and validation
- **Fortnightly & Monthly**: Aggregated reporting and settlement

---

## Data Pipeline Architecture

### Stage 1: Data Ingestion (Script 1.0.0)
**Input Files:**
- Settlement Report (CSV) - Scheme master data
- DIME Channel Summary (Excel) - DIME channel scheme details
- Channel Summary Files (Excel) - Multi-channel scheme configurations
- IHR Data (Inventory & Retail data) - Sales transactions

**Processing:**
- Column standardization and data cleanup
- Duplicate record removal (based on Scheme Code)
- Data type validation and null check handling
- Invalid row removal for mandatory fields

**Output:**
- `stg.cs_combined` - Cleaned channel summary
- `claims_mgmt.dime_channel_summary` - DIME schemes
- `claims_mgmt.settelment_report` - Settlement schemes

---

### Stage 2: Data Readiness Check (Script 1.1.1)
**Validation Checks:**
- File format verification
- Required column presence
- Data completeness validation
- Schema compliance

**Action on Failure:**
- Alerts to data management team
- Processing halted until issues resolved

---

### Stage 3: Raw Report Generation (Script 6)
**Functionality:**
- Combines IHR sales data with scheme rules
- Applies matrix checks (Fortnight/Monthly consideration)
- Calculates claim amounts by period
- Validates against settlement rules

**Key Calculations:**
- `Amount_Disbursed_IHR` - Total claim amount
- `First_FortNight_IHR` - 1st-15th claims (if applicable)
- `Second_FortNight_IHR` - 16th-end month claims (if applicable)
- `Wrong_Rate` - Rate discrepancies
- `Disallowance_SRN` - Sales return deductions

---

### Stage 4: SRN Calculations (Script 3)
**SRN (Sales Return Note) Processing:**
- Matches negative returns to original invoices
- Calculates return-based disallowances
- Handles damaged return exceptions
- Applies promotion slab matching logic

**Output Metrics:**
- Return invoice tracking
- Adjusted claim amounts post-returns
- Disallowance calculations

---

### Stage 5: Scheme-Level Summary (Script 7)
**Aggregation Level:** Distributor × Site × Scheme

**Metrics Generated:**
- Approved vs. non-approved disbursements
- Disallowance components breakdown
- CS (Channel Summary) compliance status
- Scheme eligibility flags

---

### Stage 6: Overall Summary (Script 8.1.1)
**Aggregation Level:** Distributor × Site (across all schemes)

**Key Outputs:**
1. Overall IHR Amount
2. Consider vs. Not Consider split
3. Disallowance categories
4. Net approved claims

---

## Key Business Metrics (KPIs)

### 1. **Overall IHR (Incentive Handling Request)**
**Definition:** Total claim amount submitted for all schemes in a period
**Formula:** SUM(Disbursement Amount) for all valid transactions
**Business Use:** Revenue impact assessment

### 2. **Consider Month IHR vs. Not Consider**
**Definition:** Claims split by business rule applicability
- **Consider:** Applied to scheme rules (eligible for disbursement)
- **Not Consider:** Excluded from scheme rules (laundry plans, multi-brand schemes)

**Business Use:** Scheme eligibility tracking

### 3. **Disallowance Components**

| Component | Definition | Formula |
|-----------|-----------|---------|
| **Wrong Rate** | Rate/price difference in claim | Claimed Amount - Calculated Amount |
| **Wrong Date** | Claim outside scheme validity | If InvoiceDate > SchemeEndDate |
| **Wrong Channel** | Channel mismatch vs. scheme definition | If Channel ≠ Approved Channel |
| **Free Goods** | Claims for free goods schemes (LFG codes) | Amount where InitCode starts with "LFG" |
| **Sales Return** | Deductions from return invoices | Negative IHR amounts |
| **Settlement Check** | Post-scheme-end date disallowance | Amount after Required_Valid_To |

### 4. **CS Not Approved**
**Definition:** Schemes present in IHR but not validated in Channel Summary
**Business Impact:** High-risk claims requiring investigation
**Action:** Typically set to zero in settlement

### 5. **Final Disallowance (Net Approved Amount)**
**Formula:**
```
Net Approved Amount = Total IHR - (Wrong Rate + Wrong Date + Wrong Channel + Free Goods + Sales Return + Settlement Check)
```
**Business Use:** Final settlement amount to be paid to distributors

### 6. **Scheme Status**
**Possible Values:**
- **SCHEME EXIST IN CS** - Validated in Channel Summary
- **MISSING IN CS** - Not found in Channel Summary
- **CHECK FAILED IN CS** - Mismatch in Channel Summary validation

---

## Scheme-Level Summary

### Data Structure

**Aggregation Level:** Distributor + Site + Scheme Code

**Key Columns:**

| Column Name | Description | Sample Value |
|---|---|---|
| **Distributor_Code** | Distributor ID | 2002340719 |
| **Site_Name** | Regional hub/site | Mumbai |
| **InitiativeCode** | Scheme identifier | LTR2606N00004394 |
| **InitiativeName** | Scheme name | Trade Promotion Plan |
| **Scheme_Type** | Scheme category | Channel Level Scheme |
| **Amount_Disbursed_IHR** | Total claim for the scheme | ₹50,00,000 |
| **Amount_Disbursed_Fortnight** | 1st-15th month claims | ₹25,00,000 |
| **Amount_Disbursed_Monthly** | 16th-end month claims | ₹25,00,000 |
| **Type_of_Brand** | Product brand involved | Brand A, Brand B |
| **Wrong_Rate** | Rate disallowance | ₹5,000 |
| **Wrong_Date** | Date compliance disallowance | ₹2,000 |
| **Wrong_Channel** | Channel compliance disallowance | ₹10,000 |
| **Free_Goods** | Free goods disallowance | ₹1,000 |
| **Sales_Return_CM** | Current month returns | ₹15,000 |
| **Sales_Return_PM** | Previous month returns | ₹8,000 |
| **Final_Disallowance** | Total disallowance | ₹41,000 |
| **Approved_Amount** | Amount for settlement | ₹49,59,000 |
| **Scheme_Status** | Validation status | SCHEME EXIST IN CS |

### Example Analysis

**Scenario:** Distributor D001, Site Mumbai, Scheme LTR2606N00004394

| Metric | Amount | Notes |
|---|---|---|
| Overall IHR | ₹50,00,000 | All claims submitted |
| Wrong Rate | ₹5,000 | Price discrepancies |
| Wrong Channel | ₹10,000 | Sold in non-approved channels |
| Sales Return | ₹23,000 | Return deductions |
| **Net Approved** | **₹49,62,000** | Amount to be paid |
| Disallowance % | 0.76% | Compliance quality |

---

## Overall Summary & Analytics

### 1. Distributor-Level Roll-up

**Aggregation:** All schemes by Distributor × Site

**Business Questions Answered:**
- "What is total claim vs. approved amount for Distributor X?"
- "What is disallowance trend across distributors?"
- "Which distributor has compliance issues?"

**Key Metrics:**
- **Overall IHR:** Total across all schemes
- **Approved Amount:** Consider Month + Not Consider Month
- **Total Disallowance:** Sum of all disallowance categories
- **Compliance Rate:** (Approved / Overall IHR) × 100

### 2. Period Analysis

**Monthly vs. Fortnightly Reporting:**

| Period | Coverage | Use Case |
|---|---|---|
| **Fortnightly** | 1st-14th & 15th-end month | Weekly monitoring, quick intervention |
| **Monthly** | Full calendar month | Finance settlement, analytics |

### 3. Sample Overall Summary Report

| Site | Distributor | Overall IHR | Approved Amount | Wrong Rate | Wrong Channel | Sales Return | Disallowance % |
|---|---|---|---|---|---|---|---|
| Mumbai | D001 | ₹1,00,00,000 | ₹98,50,000 | ₹50,000 | ₹75,000 | ₹1,25,000 | 1.50% |
| Delhi | D002 | ₹85,00,000 | ₹84,20,000 | ₹30,000 | ₹40,000 | ₹70,000 | 0.94% |
| Bangalore | D003 | ₹95,00,000 | ₹93,10,000 | ₹60,000 | ₹85,000 | ₹1,45,000 | 2.00% |

---

## Data Validation Rules

### Mandatory Fields Check
- Scheme Code (Initiative Code) - Must not be NULL
- Amount Disbursed - Must be numeric
- Ship Date - Must be valid date within reporting period
- Distributor Code - Must match master distributor list

### Business Rule Validation

| Rule | Condition | Action |
|---|---|---|
| **Date Compliance** | Invoice Date ≤ Scheme End Date + 2 days | Allow; Else Disallow |
| **Channel Matching** | Channel in Approved List OR Trade Plan Match | Allow; Else Mark as Wrong_Channel |
| **Scheme Existence** | Scheme Code in Channel Summary | Allow; Else Mark as CS_Not_Approved |
| **Settlement Validity** | Ship Date ≤ Required_Valid_To | Allow; Else Mark as Settlement_Check |
| **Rate Matching** | Claim Rate = Expected Rate | Allow; Else Calculate Wrong_Rate |

### Laundry & Multi-Brand Special Handling
- Excluded from Fortnightly calculation
- Automatically marked as "Consider" for Monthly
- Not counted in CS validation failures

---

## Common Terms & Definitions

### Scheme Types

**1. Trade Plan Scheme (LTR codes)**
- Channel-level promotional scheme
- Applied to specific distributors and channels
- Time-bound validity

**2. Laundry Scheme (Laundry Plan)**
- Pilot scheme for laundry use
- Excluded from regular fortnightly processing
- Processed monthly only

**3. Multi-Brand Scheme (LSS codes)**
- Cross-brand promotional scheme
- Bundle offerings
- Special calculation rules

**4. DIME Channel Scheme**
- Direct to store/channel scheme
- Alternative channel validation path
- Separate master data management

### Calculation Periods

**Fortnightly:**
- Period 1: 1st-14th of month
- Period 2: 15th-end of month
- Used for: Quick monitoring, early intervention

**Monthly:**
- Full calendar month
- Used for: Settlement, financial reporting, comprehensive analysis

### Key Process Abbreviations

| Abbreviation | Full Form | Meaning |
|---|---|---|
| **IHR** | Inventory & Retail | Sales transaction data |
| **CS** | Channel Summary | Approved scheme configuration |
| **SRN** | Sales Return Note | Return/negative invoice tracking |
| **LFG** | Laundry Free Goods | Free product scheme |
| **MRI** | MRP Regularization Initiative | Price correction scheme |
| **CM** | Current Month | Present reporting month |
| **PM** | Previous Month | Month prior to reporting |

---

## Operational FAQs

### Q1: When does "Wrong Date" disallowance apply?
**A:** When an invoice is submitted beyond 2 days after the scheme end date. This ensures claims are timely.

### Q2: What does "CS Not Approved" mean?
**A:** A scheme code appears in the claim but is missing or failed validation in the Channel Summary master data. These amounts are typically not settled.

### Q3: How are returns (SRN) handled?
**A:** Return invoices create negative claims that reduce the original scheme amount. The system matches returns to original invoices and calculates net disallowances.

### Q4: What's the difference between "Consider" and "Not Consider" schemes?
**A:** 
- **Consider:** Standard schemes applicable to the period (included in settlement)
- **Not Consider:** Laundry/multi-brand schemes processed separately (excluded from period-specific rules)

### Q5: How is the final approval amount calculated?
**A:** 
```
Final Approval = Total IHR - All Disallowances
OR
Final Approval = Amount_Disbursed_IHR - (Wrong_Rate + Wrong_Date + Wrong_Channel + Free_Goods + Sales_Return + Settlement_Check)
```

---

## Next Steps & Recommendations

1. **Regular Monitoring:** Review scheme-level and overall summaries weekly
2. **Compliance Review:** Investigate schemes with disallowance > 2%
3. **Master Data Audit:** Validate Channel Summary quarterly
4. **Training:** Educate distributors on common disallowance causes

---

## Contact & Support

For data-related questions or technical issues:
- **Data Team:** [Contact Info]
- **Finance Team:** [Contact Info]
- **Sales Operations:** [Contact Info]

---

*Document Version: 1.0*  
*Last Updated: September 2026*  
*Next Review: December 2026*
