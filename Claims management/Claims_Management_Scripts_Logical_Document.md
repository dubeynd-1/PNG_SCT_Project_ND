# Claims Management Project - Scripts Logical Documentation

**Project**: PNG SCT Claims Management  
**Repository**: PNG_SCT_Project_ND  
**Language**: Python (Databricks Notebooks)  
**Last Updated**: September 2026

---

## Table of Contents
1. [Project Overview](#project-overview)
2. [Script-wise Logic](#script-wise-logic)
3. [Data Flow Architecture](#data-flow-architecture)
4. [Key Business Logic](#key-business-logic)

---

## Project Overview

The Claims Management system processes and validates trade plan claims across multiple distribution channels. It ingests raw data, applies business rules, calculates disallowances, and generates executive summaries at distributor level.

**Total Scripts**: 9 Databricks Notebooks  
**Processing Cycle**: Monthly (with fortnight breakdowns)  
**Data Sources**: Excel files, Databricks tables, CDL (Central Data Lake)

---

## Script-wise Logic

### Script 1: **1.0.0.claims_mgmt_data_ingestion.py**

**Objective**: Ingest raw data from Excel files into Databricks staging tables

**Processing Steps**:
```
Input: Excel files (Settlement, DIME Channel Summary, Channel Summary)
  ↓
Step 1: Settlement Report Ingestion
  - Read Excel file from dbfs:/mnt/datahub/claims_mgmt/{folder}/
  - Match filename pattern: "settlement" or "settelment" (case-insensitive)
  - Clean column names: Remove special characters, normalize spaces
  - Filter: Keep only rows where Scheme_Code IS NOT NULL
  - Deduplicate: Remove exact duplicates
  - Rename columns: initiativecode → Scheme_Code
  - Validate Required Columns: Scheme_Code, Description, Short_Description, etc.
  - Date Parsing: Parse Required_Valid_To and Current_Valid_To (multiple formats)
  - Cast to Schema: Match target table (claims_mgmt.settelment_report) schema
  - Delete existing month data + Append new data
  ↓
Step 2: DIME Channel Summary Ingestion
  - Read Excel: Match filename pattern "dime"
  - Clean columns: Sr_No_, Applicable_ → Sr_No, applicable_perc
  - Deduplicate: Remove exact duplicates
  - Type Casting: Cast all columns to target schema
  - Delete old month + Append to claims_mgmt.dime_channel_summary
  ↓
Step 3: Channel Summary Ingestion (Multiple Files)
  - Read ALL Excel files matching pattern "channel" (exclude "dime")
  - Clean columns: Standardize all column names (lowercase, remove special chars)
  - Union all files: Combine using unionByName with allowMissingColumns
  - Add RptMonthyear (YYYYMM format)
  - Null/Blank Removal: Drop rows where mandatory columns (plan_for, start_date, etc.) are empty
  - Deduplication Logic:
    * Group by: initcode
    * Keep: 1 row per initcode (prefer sr_no NOT NULL)
    * Logic: If sr_no is NULL for all rows, keep first encountered
  - Save to stg.cs_combined
  ↓
Output: Three staging tables loaded with cleaned, validated data
```

**Key Validations**:
- Column name standardization (regex: `[^A-Za-z0-9]+` → `_`)
- Date format handling: yyyyMMdd, M/d/yy H:mm, M/d/yy
- Schema compliance: All columns cast to target table types
- Deduplication by initcode (scheme level)

---

### Script 2: **1.1.1 - claims data readiness check.py**

**Objective**: Validate data completeness and identify gaps before processing

**Processing Steps**:
```
Step 1: Load Scheme Master Configuration
  - Read stg.scheme_master_conf
  - Filter valid schemes for fortnight/month using matrix_check_fortnight

Step 2: Check Master Missing Codes
  - Query: SELECT distinct InitiativeCode from IHR
  - Filter: Code NOT IN (sd_science.trade_plan_dtls_master ∪ stg.masters_auditex)
  - Classify: 
    * IF InitiativeCode LIKE 'L%' → 'Normal'
    * ELSE → 'Sitecode'
  - Aggregate: SUM(amountdisbursed) by code, sort DESC
  - Output: List of missing codes with amounts for manual follow-up

Step 3: Check Mapping Missing Codes
  - Query: SELECT distinct InitiativeCode from IHR
  - Filter: Code NOT IN sd_science.trade_plan_dtls_mapping_v1
  - Same aggregation and output

Step 4: Check Combined Channel Summary & DIME Missing
  - Query: Find codes in IHR where:
    * Code NOT IN stg.cs_combined AND
    * Code NOT IN claims_mgmt.dime_channel_summary
  - Filter: InitiativeEndDate >= start_date_value (active schemes)
  - Output: Missing schemes with amounts

Output: Validation reports for data quality assurance
```

**Purpose**: Identify missing scheme master data before SRN/Wrong Rate calculations

---

### Script 3: **2.Multi category schemes-REVAMP-CS-T1.py**

**Objective**: Classify schemes as Laundry/Multi-Brand based on product composition

**Processing Steps**:
```
Step 1: Data Preparation
  - Load Master: sd_science.trade_plan_dtls_master + stg.masters_auditex
  - Explode subfNameList by pipe delimiter (|) → Get individual product levels
  - Load IHR: All initiatives in date range
  - Load Product Master: BrandName, BrandformName, CategoryName, ProductCode

Step 2: Normalize Text Data
  Function normalize(col_name):
    - Convert to lowercase
    - Remove special characters: [^a-z0-9]+ → ' '
    - Normalize spaces: Remove extra spaces
  - Apply to: Level_Desc, BrandName, BrandformName, ProductCode, CategoryName

Step 3: Brand Matching (INNER JOIN)
  - Match scheme Level_Desc against Product Master by Level_Type:
    * SUB-BRANDFORM → Match Product.SubbfName
    * BRANDFORM → Match Product.BrandformName
    * BRAND → Match Product.BrandName
    * CATEGORY → Match Product.CategoryName
    * SKU → Match Product.ProductCode
  - Keep matched rows
  - Output: (scheme_code, level_desc, brandname, business_brand)

Step 4: Collect Distinct Brands per Scheme
  - Group by: scheme_code
  - Aggregate: collect_set(business_brand) → brand_set array
  - Output: (scheme_code, brand_set[...])
  - Filter out: "na" brands (not relevant for classification)

Step 5: Classification Logic
  IF (brand_set contains "tide" AND brand_set contains "ariel")
    → scheme_category = "Laundry Scheme"
  ELSE IF (size(brand_set) > 1) AND NOT (
    (contains "gillette" AND contains "venus" AND NO other brands) OR
    (contains "gillette" AND contains "old spice" AND NO other brands)
  )
    → scheme_category = "Multi Brand Scheme"
  ELSE
    → scheme_category = NULL

Step 6: Save Classification
  - Keep schemes with scheme_category IS NOT NULL
  - Add RptMonthyear (YYYYMM)
  - Delete old month data + Append to claims_mgmt.laundry_multi_brand_schemes

Output: Classification table for use in raw report generation
```

**Business Logic**: Laundry and Multi-Brand schemes have different reporting/calculation rules

---

### Script 4: **3.SRN_Calculations_Continued_6th_July_onwards.py**

**Objective**: Calculate Sales Return Note (SRN) disallowances with promotion group logic

**Processing Steps**:
```
Step 1: Load Master Data
  - Master: Trade plan details with Promotion_Group_Name, Promotion_Slab info
  - IHR: Positive claims (amountdisbursed > 0)
  - PSR: Sales details (quantity, gross amount, net amount, transaction type)

Step 2: Join IHR ← PSR (Positive Claims)
  - Join condition: invoice_number, time_period_end_date, retailer_code
  - Aggregate by: scheme_code, invoice_number, Promotion_Group_Name
  - Get: item_unit_qty (SUM), gross_transact_amt (SUM), net_transact_amt (SUM)

Step 3: Match Promotion Slab
  - For each row:
    * Calculate condition = item_unit_qty (if scheme_type = 'QUANTITY') 
                          OR net_transact_amt (if scheme_type = 'VALUE')
    * Find Promotion_Slab where: condition >= Promotion_Slab_Buy_Min AND condition <= Promotion_Slab_Buy_Max
    * Calculate: Promotion_Slab_Get_Discount
    * Result: invcXscheme_lvl_Amount_disbursed_Calculated

Step 4: Deduplicate Same SBF Across Groups
  - Partition by: invoice_number, scheme_code, SubbfName
  - Keep: Row with highest invcXscheme_lvl_Amount_disbursed_Calculated
  - Sum by: invoice_number, scheme_code (final calculated amount)

Step 5: Load Returns Data
  - Query: Find return invoices (ApplyToDocNum in PSR)
  - Join return PSR ← original IHR
  - Apply Damaged Return Logic:
    * IF transaction_type = 'DAMAGED RETURN' THEN
        - gross_transact_amt_return = 0
        - item_unit_qty_return = 0
        - net_transact_amt_return = 0
        - amountdisbursed_retrun = actual amount (keep as is)
    * ELSE (normal return)
        - Include actual amounts

Step 6: Calculate Adjusted Amounts with Returns
  - gross_transact_amt_updt = original_gross + return_gross (with damage logic)
  - item_unit_qty_updt = original_qty + return_qty (with damage logic)
  - net_transact_amt_updt = original_net + return_net (with damage logic)
  - amountdisbursed_retrun = sum of return IHR amounts

Step 7: Apply Same Slab Matching for Returns
  - Same logic as Step 3 but on adjusted amounts
  - Calculate: amountdisbursed_calc_return

Step 8: Group Count Check
  - Count distinct Promotion_Group_Name (where item_unit_qty_updt ≠ 0)
  - IF current_group_count < promotion_group_count THEN
      group_count_check = 0 (eligibility fails)
  - ELSE
      group_count_check = 1

Step 9: Variance Check
  - For each scheme/invoice/Promotion_Group:
    * Count distinct SubbfName (where item_unit_qty_updt ≠ 0)
    * Get Variance threshold from Master
    * IF sbf_count < Variance THEN variance_check = 0
    * ELSE variance_check = 1

Step 10: Calculate Final Disallowance
  - IF group_count_check = 0 THEN new_AmtDisbursed = 0
  - ELSE new_AmtDisbursed = calculated_amount
  
  - IF slab_max_limit EXISTS AND slab_max_limit < new_AmtDisbursed THEN
      new_AmtDisbursed_capped = slab_max_limit
  - ELSE
      new_AmtDisbursed_capped = new_AmtDisbursed

  - disallowance = (amountdisbursed_original + amountdisbursed_return) - new_AmtDisbursed_capped
  
  - IF Remarks_SRN = 'Full Return' THEN disallowance = 0
  - IF Remarks_SRN = 'Partial Return' AND disallowance < 0 THEN disallowance = 0

Step 11: Save Results
  - Delete old month data + Append to claims_mgmt.srn_calcs
  - Include: invoice_number, return_invoice_number, scheme_code, disallowance, Remarks_SRN

Output: SRN disallowance table with detailed calculations
```

**Key Business Rules**:
- Damaged Returns: Do NOT reduce scheme eligibility (qty/amount treated as 0)
- Group Count: ALL promotion groups must be present in transaction
- Variance: Minimum number of sub-brands per group must be met
- Partial Return: Negative disallowance rounded to 0

---

### Script 5: **4.WrongRate_Calculations.py**

**Objective**: Identify Wrong Rate claims (discrepancies between actual vs calculated amounts)

**Processing Steps**:
```
Step 1: Load Data
  - PSR: tblbasetrn_salesdetails (base transaction level)
  - IHR: tblrefinedview_ihr (incentive/claim level)
  - Master: Trade plan details

Step 2: Join IHR ← PSR
  - Join condition: invoice_number (InvCode), time_period_end_date (InvDate), retailer_code

Step 3: Product Level Matching
  - Join with Product Master → Get Category, Brand, BrandForm, SubbfName
  - Filter by Master Level_Type → Match appropriate product level

Step 4: Aggregate by Promotion Group
  - Group by: scheme_code, invoice_number, Promotion_Group_Name
  - Calculate: net_transact_amt_sum, item_unit_qty_sum

Step 5: Slab Matching (Same as SRN)
  - Condition = qty (if QUANTITY type) OR amount (if VALUE type)
  - Find matching slab: condition >= Buy_Min AND <= Buy_Max
  - Calculate: Promotion_Slab_Get_Discount

Step 6: Dedup Same SBF Across Groups
  - Keep highest disallowance per (invoice, scheme, SBF)

Step 7: Group Count Check
  - Count distinct groups with qty > 0
  - IF count < expected THEN set calculated = 0

Step 8: Retailer Apply Count Check
  - Count distinct ShipDate per (scheme_code, retailer_code) in month
  - IF count > Retailer_Apply_Count THEN set calculated = 0

Step 9: Slab Max Limit
  - IF calculated_amount > slab_max_limit THEN
      calculated_amount_capped = slab_max_limit
  - Calculate diff = ABS(calculated_capped - amountdisbursed)

Step 10: Wrong Rate Calculation
  - FOR Fortnight (day <= 14):
      IF diff > 0.2 THEN wrong_rate = diff
      ELSE wrong_rate = 0
      
  - FOR 2nd Fortnight (day > 14) OR Monthly:
      IF scheme_code LIKE '%MRI' (except whitelist) THEN
          wrong_rate = 0  (MRI schemes zeroed for 2nd fortnight)
      ELSE IF diff > 0.2 THEN
          wrong_rate = diff
      ELSE
          wrong_rate = 0
      
  - FOR LFG Schemes (FREE GOODS):
      wrong_rate = 0 (always)

Step 11: Save Results
  - Delete old month + Append to claims_mgmt.worng_rate_calcs
  - Include: InitiativeCode, invoice_number, ShipDate, calculated, wrong_rate, Comments

Output: Wrong Rate disallowance table
```

**Business Rules**:
- Only invoice-level differences > 0.2 are flagged
- MRI (Multi-Retailer Initiative) schemes: Zero out for 2nd fortnight
- Free Goods (LFG schemes): Always zero (calculated separately)
- Retailer Count: Max times a scheme can be claimed per retailer per month

---

### Script 6: **5.ChannelValidations.py**

**Objective**: Validate channel and sub-channel appropriateness for each claim

**Processing Steps**:
```
Step 1: Load Raw Data
  - IHR: Get channel_name
  - PSR: Get transaction_customer_type (sub-channel)
  - Channel Summary (stg.cs_combined)
  - DIME Channel Summary (claims_mgmt.dime_channel_summary)

Step 2: Channel Validation for Retailer-Level Schemes
  - Filter: schemes where Retailer_Code IS NOT NULL AND Branch_code IS NOT NULL
  
  - Extract Rules from CS:
    FOR EACH scheme with customer_type = 'ALL CUSTOMER TYPES':
        allowed_channels = collect_set(channel_name from Plan_For)
        allowed_subchannels = NULL
    FOR EACH scheme with customer_type ≠ 'ALL':
        allowed_channels = NULL
        allowed_subchannels = collect_set(sub_channel from Customer_Type)
  
  - Extract Rules from DIME (same logic as CS)
  
  - Validation Logic:
    IF (CS rule exists):
        IF (all_customer_type = 1) AND (IHR_channel IS NULL) → MATCHING
        ELSE IF (array_contains(allowed_channels, IHR_channel) OR 
                 array_contains(allowed_subchannels, PSR_subchannel)) → MATCHING
        ELSE → MISMATCH
    ELSE IF (DIME rule exists) → Apply same logic
    ELSE → NOT IN CS/MAPPING
  
  - Output: REMARK = 'CHANNEL/SUB-CHANNEL MATCHING' or 'MISMATCH' or 'NOT IN CS/MAPPING'

Step 3: Channel Validation for Channel-Level Schemes
  - Filter: schemes where Retailer_Code IS NULL (channel/distributor level)
  - Apply same validation logic as Step 2
  - Output: REMARK for channel-level schemes

Step 4: Combine Results
  - Union retailer-level and channel-level validations
  - Add Scheme_Type, Scheme_Check metadata
  - Join with vs_list → Add consideration remarks

Step 5: Final Output
  - Save to claims_mgmt.Channel_lvl_check
  - Include: salesinvoiceno, InitiativeCode, Retailer_Code, Channel, REMARK
  - Repartition: 500 partitions for performance

Output: Channel validation table for claims audit trail
```

**Business Logic**: Different schemes are applicable to different channels/sub-channels

---

### Script 7: **6.claims_mgmt_Raw_Report.py**

**Objective**: Create comprehensive raw report combining ALL calculations and validations

**Processing Steps**:
```
Step 1: Aggregate IHR Data
  - Union: Current month IHR + SRN-related IHR (from srn_calcs)
  - Get distinct records by SalesInvoiceNo, InitiativeCode

Step 2: Join with ALL Reference Tables
  - vs_list → matrix_check_fortnight, matrix_check_monthly_release, scheme remarks
  - PSR → invoice date, channel, customer type (sub-channel)
  - Retailer Master → RetailerCode, RetailerName, BranchCode, BranchName
  - Channel_lvl_check → Channel REMARK
  - stg.cs_combined → CS Approval status
  - sd_science.trade_plan_dtls_mapping_v1 → Mapping status
  - claims_mgmt.dime_channel_summary → DIME status
  - claims_mgmt.worng_rate_calcs → Wrong_Rate disallowance
  - claims_mgmt.srn_calcs → SRN disallowance + SRN dates
  - claims_mgmt.settelment_report → Settlement status
  - claims_mgmt.laundry_multi_brand_schemes → Laundry/Multi-Brand classification
  - Location hierarchy → SiteName

Step 3: Calculate Fortnight/Monthly IHR
  
  First_FortNight_IHR Calculation:
    IF (matrix_check_fortnight = 'Consider' 
        AND date_format(ShipDate, 'yyyyMM') = formatted_date 
        AND dayofmonth(ShipDate) <= 14
        AND scheme_code NOT IN laundry_multi_brand_schemes)
      → First_FortNight_IHR = AmountDisbursed
    ELSE
      → First_FortNight_IHR = 0
  
  Second_FortNight_IHR Calculation:
    IF (
      (trim(upper(matrix_check_monthly_release)) = 'Consider' 
       AND trim(upper(matrix_check_fortnight)) = 'Not Consider')
      OR (trim(upper(matrix_check_fortnight)) = 'Consider' AND dayofmonth(ShipDate) > 14)
      OR scheme_code IN laundry_multi_brand_schemes
    ) AND date_format(ShipDate, 'yyyyMM') = formatted_date
      → Second_FortNight_IHR = AmountDisbursed
    ELSE
      → Second_FortNight_IHR = 0

Step 4: Remarks Assignment Logic
  
  Remarks_Fortnight:
    IF matrix_check_fortnight = 'Not Consider' → scheme_code_consideration_remark_1
    ELSE IF AmountDisbursed < 0 → "Negative Claim"
    ELSE IF Disallowance_SRN = 0 AND Wrong_Rate = 0 → "Verified"
    ELSE IF Disallowance_SRN > 0 AND Wrong_Rate = 0 AND date_format(srn_date) = formatted_date → "Sales return excluding partial return"
    ELSE IF Disallowance_SRN = 0 AND Wrong_Rate > 0 → "Wrong Rate"
    ELSE IF Disallowance_SRN > 0 AND Wrong_Rate > 0 → "Sales return with wrong rate"
  
  Remarks_Monthly:
    IF matrix_check_monthly_release = 'Not Consider' → scheme_code_consideration_remark_2
    ELSE → Similar logic as Remarks_Fortnight
  
  FOR Laundry/Multi-Brand Schemes:
    → Override with specific remarks (e.g., "Not Consider - Laundry Plan")

Step 5: Disallowance Calculation
  - Free_Goods: Extracted from Wrong_Rate for LFG schemes
  - Wrong_Channel: From Channel validation mismatches
  - Wrong_Date: From settlement date mismatches
  - Total_Disallowance = Wrong_Rate + Wrong_Channel + Wrong_Date + Disallowance_SRN + ...

Step 6: Apply Manual Overrides
  - FOR formatted_date = '202602': Zero out First_FortNight_IHR for specific schemes:
    * MRI schemes (except whitelist)
    * Specific 10 scheme codes
    * Specific 50+ scheme codes
    * Laundry schemes (LHG2602N4925, LHG2602N4924)

Step 7: Select Final Columns
  - SalesInvoiceNo, ShipDate, InitiativeCode, AmountDisbursed
  - First_FortNight_IHR, Second_FortNight_IHR
  - Wrong_Rate, Disallowance_SRN, Wrong_Channel, Free_Goods, Wrong_Date
  - Remarks_Fortnight, Remarks_Monthly
  - matrix_check_fortnight, matrix_check_monthly_release
  - RptMonthYear

Step 8: Save Results
  - Delete old month data (WHERE rptmonthyear = formatted_date)
  - Append new data to claims_mgmt.claims_mgmt_raw_report

Output: Raw report with all claims, calculations, and audit trail
```

**Key Output**: Foundation for all downstream summary and executive reports

---

### Script 8: **7.claims_mgmt_Summary_Report.py**

**Objective**: Generate summary reports at fortnight and monthly levels

**Processing Steps**:
```
Step 1: Read Raw Report Data
  - Query claims_mgmt_raw_report WHERE RptMonthYear = formatted_date

Step 2: Load Reference Data
  - Distributor names mapping
  - Scheme type classification
  - Channel/Sub-channel mappings

Step 3: FOR FORTNIGHT SUMMARY (if end_date ends with "14"):
  
  Filter: matrix_check_fortnight = 'Consider'
  
  Aggregate by: Distributor, SiteName, InitiativeCode, InitiativeName
  
  Calculations:
    - AmountDisbursed_IHR = SUM(AmountDisbursed) [current month only]
    - Amount_Disbursed_Fortnight = SUM(First_FortNight_IHR)
    - Amount_Disbursed_Monthly = SUM(Second_FortNight_IHR)
    - Wrong_Rate = SUM(Wrong_Rate)
    - Wrong_Channel = SUM(Wrong_Channel)
    - Wrong_Date = SUM(Wrong_Date)
    - Free_Goods = SUM(Free_Goods) [for LFG schemes]
    - Disallowance_SRN = SUM(Disallowance_SRN)
    - Final_Disallowance = Wrong_Rate + Free_Goods + Wrong_Channel + Wrong_Date + (excluding SRN for fortnight)
  
  Join with scheme_Type → Get scheme classification
  
  Save to: claims_mgmt.claims_mgmt_summary_fortnight

Step 4: FOR MONTHLY SUMMARY (if end_date NOT "14"):
  
  Filter: matrix_check_fortnight = 'Consider' OR matrix_check_monthly_release = 'Consider'
  
  Similar aggregation as fortnight BUT:
    - Amount_Disbursed_Monthly = SUM(Second_FortNight_IHR)
    - INCLUDE Disallowance_SRN in Final_Disallowance (full month SRN)
  
  For Wrong Rate & Free Goods:
    - Separate aggregation for 2nd fortnight data
    - Include in monthly calculations
  
  Save to: claims_mgmt.claims_mgmt_summary_monthly

Output: Summary tables at distributor/site/scheme level
```

**Difference**: Fortnight excludes SRN disallowance; Monthly includes it

---

### Script 9: **8.1.1Overall summary claim report.py**

**Objective**: Create executive summary at distributor level

**Processing Steps**:
```
Step 1: Load Summary Data
  - Read claims_mgmt_summary_fortnight OR claims_mgmt_summary_monthly (based on end_date)

Step 2: Build Distributor Base Summary (df_base)
  
  Identify CS_Not_Approved:
    - FOR FORTNIGHT: Only count schemes with InitiativeCode LIKE '%MRI'
    - FOR MONTHLY: Count all schemes NOT IN (cs_combined ∪ mapping ∪ dime)
  
  Aggregate by: Distributor, SiteName
  
  Calculations:
    - AmountDisbursed_IHR = SUM(Amount_Disbursed_Fortnight or _Monthly)
    - CS_Not_Approved = SUM(Amount_Disbursed) where NOT in CS/DIME/Mapping
    - Wrong_Rate = SUM(Wrong_Rate)
    - Wrong_Date = SUM(Wrong_Date)
    - Wrong_Channel = SUM(Wrong_Channel)
    - Free_Goods = SUM(Free_Goods)
    - Disallowance_Settlement = SUM(Disallowance_SRN) [monthly only]

Step 3: Build Sales Return Summary (sales_return_df)
  
  Query claims_mgmt_raw_report:
    - Filter: WHERE RptMonthYear = formatted_date AND ShipDate in range
    
    - total_ihr = SUM(AmountDisbursed) [all claims in date range]
    
    - not_consider_for_calculation = total_ihr - SUM(First_FortNight_IHR or Second_FortNight_IHR)
      [Claims not marked for fortnight/monthly consideration]
    
    - sales_return_cm = SUM(Disallowance_SRN)
      WHERE matrix_check matches AND srn_shipdate = formatted_date AND shipdate = formatted_date
      [Returns in same month]
    
    - sales_return_pm = SUM(Disallowance_SRN)
      WHERE shipdate ≠ formatted_date AND srn_shipdate = formatted_date
      [Returns for prior month claims]

Step 4: Combine Base + Sales Return
  
  Join on: Distributor, SiteName, RptMonthYear
  
  Calculate:
    - Overall_IHR = total_ihr (from sales_return_df)
    - Consider_Fortnight_IHR or Consider_Month_IHR = AmountDisbursed_IHR (from df_base)
    - Not_Consider_For_Calculation = not_consider_for_calculation
    
    - Total_Disallowance = CS_Not_Approved + Wrong_Rate + Wrong_Date + Free_Goods 
                          + Wrong_Channel + Disallowance_Settlement 
                          + Sales_Return_CM + Sales_Return_PM 
                          + CN_Adjustments + Cash_Claims + Other_CN
    
    - Final_Approved_Amount = AmountDisbursed_IHR - Total_Disallowance

Step 5: Format Output
  - Round all numeric columns to 2 decimals
  - Replace underscores with spaces in column headers
  - Drop internal columns (Disallowance_Settlement, etc.)

Step 6: Save Results
  - FOR FORTNIGHT: Save to claims_mgmt.overall_dist_summ_fortnight_v1
  - FOR MONTHLY: Save to claims_mgmt.overall_dist_summ_monthly_v1
  - Delete old month data + Append new data

Output: Executive summary at distributor/site level
```

**KPI Columns**:
- **Overall_IHR**: Total incentive amount across all dates
- **Consider_Fortnight/Month_IHR**: Amount considered for fortnight/monthly calculation
- **Total_Disallowance**: Sum of all adjustments/rejections
- **Final_Approved_Amount**: Net amount to be paid

---

## Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    EXCEL INPUT FILES                            │
│  (Settlement, DIME Channel Summary, Channel Summary)             │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────────┐
        │  Script 1: Data Ingestion      │
        │  (Clean, Validate, Deduplicate)│
        └────────┬───────────────────────┘
                 │
    ┌────────────┼────────────┐
    ▼            ▼            ▼
Settelment    DIME Summary   Channel Summary
  Table        Table         (cs_combined)
    │            │            │
    └────────────┼────────────┘
                 │
                 ▼
    ┌────────────────────────────────────┐
    │  Script 2: Data Quality Checks     │
    │  (Identify Missing Schemes)        │
    └────────┬───────────────────────────┘
             │
             ▼
    ┌────────────────────────────────────┐
    │  Script 3: Multi-Category          │
    │  Classification (Laundry/Multi)    │
    └────────┬───────────────────────────┘
             │
    ┌────────┴──────┐
    │               │
    ▼               ▼
  Script 4      Script 5
  SRN Calc      Wrong Rate
  │              │
  ├─────────┬────┘
  │         │
  ▼         ▼
Script 6: RAW REPORT
(Combine all calculations)
  │
  ▼
┌─────────────────────────────────┐
│  Script 7: Summary Reports      │
│  (Fortnight & Monthly)          │
└────────┬────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│  Script 9: Executive Summary    │
│  (Distributor Level)            │
└─────────────────────────────────┘

Script 5: Channel Validations (Runs in parallel with Scripts 4-6)
  - Validates each claim for channel appropriateness
  - Used in Script 6 for audit trail
```

---

## Key Business Logic

### 1. **Fortnight vs Monthly Calculation**
- **First Fortnight (Days 1-14)**: matrix_check_fortnight = 'Consider'
- **Second Fortnight (Days 15-31)**: matrix_check_monthly_release = 'Consider'
- **Laundry/Multi-Brand Schemes**: Excluded from 1st fortnight, included in 2nd fortnight/monthly

### 2. **Disallowance Types**
| Type | Calculation | When Applied |
|------|------------|--------------|
| **SRN Disallowance** | (Original + Return Amount) - Calculated Amount | All months (returned claims) |
| **Wrong Rate** | ABS(Actual Disbursed - Calculated) | Only if diff > 0.2, MRI=0 for 2nd FN |
| **Wrong Channel** | Full amount | If channel validation fails |
| **Wrong Date** | Full amount | If settlement date exceeds valid_to |
| **Free Goods** | Full amount | LFG schemes (calculated separately) |
| **CS Not Approved** | Full amount | Schemes not in Channel Summary |

### 3. **Promotion Group Logic**
- **Group Count Check**: ALL promotion groups present in transaction
  - If current_group_count < expected_group_count → Disallowance = 0
- **Variance Check**: Minimum sub-brands per group
  - If sbf_count < Variance → Disallowance = 0

### 4. **Damaged Return Handling**
- Damaged returns do NOT reduce eligibility (qty/amount = 0)
- Original IHR amount fully retained
- Does not affect promotion group count

### 5. **Slab Max Limit**
- Calculated disallowance capped at slab_max_limit
- If calculated > limit → Amount capped to limit

### 6. **Laundry Scheme Classification**
- **Criteria**: Scheme contains BOTH "Tide" AND "Ariel" brands
- **Treatment**: 
  - Excluded from 1st fortnight
  - Included in 2nd fortnight/monthly
  - Special remarks in final report

### 7. **MRI Scheme Treatment**
- **MRI** = Multi-Retailer Initiative schemes
- **1st Fortnight**: Included normally
- **2nd Fortnight**: Wrong Rate = 0 (except whitelisted schemes)
- **Monthly**: Included with all disallowances

---

## Summary of Processing Flow

```
1. INGESTION (Script 1)
   - Clean raw Excel data
   - Standardize columns
   - Deduplicate

2. VALIDATION (Script 2)
   - Check scheme master presence
   - Verify channel summary completeness

3. CLASSIFICATION (Script 3)
   - Identify Laundry/Multi-Brand schemes
   - Brand-level product matching

4. CALCULATION (Scripts 4-5)
   - SRN: Sales return disallowances
   - Wrong Rate: Disbursement discrepancies
   - Channel: Validation checks

5. AGGREGATION (Script 6)
   - Combine all calculations
   - Apply business rules
   - Generate raw report

6. SUMMARIZATION (Script 7-8-9)
   - Aggregate by fortnight/monthly
   - Distributor-level rollup
   - Executive summary

OUTPUT: Final claims report with all disallowances and approved amounts
```

---

## Document Information

**Project Name**: PNG SCT Claims Management  
**Repository**: dubeynd-1/PNG_SCT_Project_ND  
**Data Warehouse**: Databricks  
**Reporting Frequency**: Monthly (with fortnight breakdowns)  
**Last Updated**: September 15, 2026

---

**For questions or clarifications, please contact the SCT Project Team.**
