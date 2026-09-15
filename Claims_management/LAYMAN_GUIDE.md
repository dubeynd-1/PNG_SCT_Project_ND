# Claims Management System - Layman's Guide

**Simple Explanation for Business Team Without Technical Background**

---

## What is the Claims Management System? (In Simple Terms)

Imagine PNG (our company) runs promotional schemes to boost sales through distributors. Each time a distributor makes a sale under these schemes, they submit a claim for the incentive/bonus they're supposed to get. The Claims Management System automatically:

1. **Receives all claims** from distributors
2. **Checks if they're valid** (Did they follow the rules?)
3. **Calculates the correct amount** to pay them
4. **Flags any problems** (overcharges, discrepancies)
5. **Generates reports** showing everything clearly

---

## The Big Picture: How It Works

```
STEP 1: Distributors submit claims
        ↓
STEP 2: System validates the data
        ↓
STEP 3: System checks against rules
        ↓
STEP 4: System calculates correct payment
        ↓
STEP 5: Reports generated for Finance team
```

---

## Key Numbers to Know (KPIs)

### 1. **Overall IHR (Total Claim Amount)**
- **What it is:** The total money claimed by distributors
- **Example:** ₹1,00,00,000 in claims this month
- **Why it matters:** Shows the size of promotional spending

### 2. **Approved Amount**
- **What it is:** The amount we actually pay after deductions
- **Example:** ₹98,50,000 (after removing incorrect claims)
- **Why it matters:** This is the real cost to the company

### 3. **Disallowance (Money NOT Paid)**
- **What it is:** Claims we reject or reduce
- **Example:** ₹1,50,000 rejected
- **Why it matters:** Shows compliance issues or errors

### 4. **Compliance Rate**
- **Formula:** (Approved ÷ Total Claimed) × 100
- **Example:** 98.5% = "Good! Most claims are valid"
- **Example:** 85% = "Concern! Many claims have issues"

---

## Why Claims Get Rejected (Disallowances Explained)

### ❌ **Wrong Rate** 
- Distributor claims ₹100 but the scheme says ₹80
- We only pay ₹80, disallow ₹20

### ❌ **Wrong Date**
- Scheme ended on June 15, but claim submitted on July 1
- Too late! Claim rejected

### ❌ **Wrong Channel**
- Scheme was for "Modern Trade" but sold through "General Trade"
- Doesn't match rules, claim rejected

### ❌ **Wrong Product** 
- Free goods scheme but they claimed for paid products
- Invalid, rejected

### ❌ **Sales Return**
- Customer returned the product after 5 days
- Need to reduce the original claim

### ❌ **Not in Approved List**
- Scheme not found in our Channel Summary database
- Risky! Claim rejected for safety

---

## Two Reporting Periods (Why We Report Twice)

### 📅 **Fortnightly (Every 2 weeks)**
- 1st-14th of month = Period 1
- 15th-30th of month = Period 2
- **Purpose:** Quick monitoring, catch issues early
- **Audience:** Operations team for quick decisions

### 📅 **Monthly (Full Month)**
- Entire calendar month
- **Purpose:** Complete financial reporting, settlement
- **Audience:** Finance team, Distributors for final payment

---

## Understanding the Reports

### **Report 1: Scheme-Level Summary**
Shows breakdown by individual scheme:

| Scheme Name | Distributor | Total Claim | Approved | Rejected | Notes |
|---|---|---|---|---|---|
| Brand X Launch | Mumbai | ₹50,00,000 | ₹49,50,000 | ₹50,000 | Minor date issue |
| Loyalty Program | Delhi | ₹30,00,000 | ₹29,80,000 | ₹20,000 | Rate discrepancy |

**What to look for:**
- High rejection rates = communication needed with distributor
- Consistent patterns = process improvement needed

### **Report 2: Overall Summary**
Shows total across all schemes by distributor/site:

| Site | Total IHR | Approved | Disallowed | % Rejected |
|---|---|---|---|---|
| Mumbai | ₹1,00,00,000 | ₹98,50,000 | ₹1,50,000 | 1.5% |
| Delhi | ₹85,00,000 | ₹84,20,000 | ₹80,000 | 0.9% |
| Bangalore | ₹95,00,000 | ₹93,10,000 | ₹1,90,000 | 2.0% |

**What to look for:**
- Trends across months
- Which distributors have compliance issues
- Cost of promotional schemes

---

## Common Questions Answered

### Q: "Why was a claim rejected?"
**A:** Check the disallowance report. Common reasons:
- Wrong date (submitted too late)
- Price mismatch (wrong rate applied)
- Channel mismatch (wrong sales channel)
- Not in approved scheme list

### Q: "When will we pay the distributor?"
**A:** 
- Fortnightly report = For quick review
- Monthly report = Approved amount is settled/paid

### Q: "What's 'CS Not Approved'?"
**A:** The scheme code doesn't exist in our approved list. It's flagged as risky until verified. Like accepting an ID we don't recognize at the entrance.

### Q: "Why do rejected claims matter?"
**A:** 
- Financial impact = Less budget spent
- Compliance = Shows process adherence
- Risk management = Prevents fraud/errors
- Distributor relationship = Need transparency

### Q: "What's the difference between 'Consider' and 'Not Consider'?"
**A:**
- **Consider:** Normal scheme, follows standard rules (most schemes)
- **Not Consider:** Special scheme (laundry, multi-brand), handled separately

---

## What Success Looks Like

✅ **Good Compliance:**
- Disallowance rate < 2%
- Most claims approved
- Consistent patterns

⚠️ **Watch Out For:**
- Disallowance rate > 5%
- Specific distributors with high rejection
- Sudden spikes in rejected claims
- Many "CS Not Approved" claims

---

## For Different Teams

### 💰 **Finance Team**
- Focus on: Approved amounts, total cost
- Action: Process settlements, budget tracking
- Report needed: Monthly overall summary

### 📊 **Sales Operations**
- Focus on: Disallowance reasons, compliance
- Action: Coach distributors, improve processes
- Report needed: Weekly scheme-level summary

### 👔 **Management**
- Focus on: Trends, distributor performance
- Action: Strategic decisions, vendor management
- Report needed: Monthly dashboard with trends

### 📋 **Distributors**
- Focus on: Why their claims were rejected
- Action: Fix processes, resubmit correctly
- Report needed: Individual distributor report showing rejections + reasons

---

## The Bottom Line

**In one sentence:** The Claims Management System automatically checks, validates, and reports on all distributor promotional claims to ensure we pay only what's correct.

**The goal:** Faster processing, fewer errors, lower costs, happier distributors.

---

*This guide is designed for business users without technical background. For detailed technical information, refer to BUSINESS_DOCUMENTATION.md*
