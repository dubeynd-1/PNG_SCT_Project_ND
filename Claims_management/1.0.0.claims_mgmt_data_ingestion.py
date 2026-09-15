# Databricks notebook source
# MAGIC %md
# MAGIC ## Notes:
# MAGIC - Channel_Summary files should start with Channel_Summary---.xlsx
# MAGIC - settelement report file name should in format - Settelment Nov-2025.xlsx
# MAGIC - Laundry report file name should in format - Laundry Schemes Nov-2025.xlsx

# COMMAND ----------

# MAGIC %md
# MAGIC #### Product Master ingestion adhoc

# COMMAND ----------

# DBTITLE 1,Product Master ingestion adhic
# file_path = "dbfs:/mnt/datahub/Adhoc Inputs/Product_Master_ALL_BRAND.csv"

# product_master_df = (
#     spark.read
#         .option("header", "true")
#         .option("inferSchema", "true")
#         .option("delimiter", ",")
#         .csv(file_path)
# )

# product_master_df.write.mode("overwrite").saveAsTable("stg.product_master_all_brand")

# COMMAND ----------

# file_path = "dbfs:/mnt/datahub/Adhoc Inputs/Product_Master_ALL_BRAND.csv"

# product_master_df = (
#     spark.read
#         .option("header", "true")
#         .option("inferSchema", "true")
#         .option("delimiter", ",")
#         .csv(file_path)
# )

# product_master_df.write.mode("overwrite").saveAsTable("stg.product_master_all_brand")

# COMMAND ----------

import io
import math
import requests
import calendar
from dateutil.rrule import rrule, MONTHLY
import shutil
import os
import re

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType, DateType, IntegerType, BooleanType
from pyspark.sql.functions import lit, current_date, col
from pyspark.sql import functions as F
from pyspark.sql.functions import  *
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("start_date","")
dbutils.widgets.text("end_date","")

# COMMAND ----------

current_date = (datetime.now()-timedelta(days=0)).strftime('%Y-%m-%d')
print(current_date)
previous_date = (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d')
print(previous_date)

start_date=dbutils.widgets.get("start_date")
end_date=dbutils.widgets.get("end_date")
if start_date=="" and end_date=="":
    start_date_value = (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d')
    end_date_value = (datetime.now()-timedelta(days=0)).strftime('%Y-%m-%d')
else:
    start_date_value=start_date
    end_date_value=end_date

date_obj = datetime.strptime(start_date_value, "%Y-%m-%d")
month_start = date_obj.replace(day=1).strftime("%Y-%m-%d")
formatted_date = date_obj.strftime("%Y%m")
ihr_date_format=start_date_value[:-2]+"%"
folder=date_obj.strftime("%b-%y")
file_name=date_obj.strftime("%b-%Y")

print(formatted_date)
print(month_start)
print(ihr_date_format)
print(start_date_value,end_date_value,folder,file_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SETTLEMENT

# COMMAND ----------

import re
from pyspark.sql import functions as F
from pyspark.sql.functions import *

files = dbutils.fs.ls(f"dbfs:/mnt/datahub/claims_mgmt/{folder}/")

file_path = next(
    f.path for f in files
    if f.name.lower().startswith(("settlement", "settelment"))
    and f.name.lower().endswith(".xlsx")
)

settletment_df = (spark.read.format("com.crealytics.spark.excel")
    .option("header","true")
    .option("inferSchema","true")
    .load(file_path)
    .withColumn("RptMonthyear",lit(formatted_date))
    .filter(col("initiativecode").isNotNull())
    # .filter(col("Scheme_Code").isNotNull())
    .dropDuplicates())

settletment_df = settletment_df.select(*[
    F.col(f"`{c}`").alias(re.sub(r"\W+","_",c).strip("_"))
    for c in settletment_df.columns
])

# rename_map = {
#     "SchemeCode":"Scheme_Code",
#     "ShortDescription":"Short_Description",
#     "CurrentValidTo":"Current_Valid_To",
#     "RequiredValidTo":"Required_Valid_To"
# }
rename_map = {
    "initiativecode":"Scheme_Code",
    "initiativename":"Short_Description",
    "initiativestartdate":"Current_Valid_To",
    "initiativeenddate":"Required_Valid_To"
}

for o,n in rename_map.items():
    if o in settletment_df.columns:
        settletment_df = settletment_df.withColumnRenamed(o,n)

required_cols = [
    "Scheme_Code",
    "Description",
    "Short_Description",
    "Current_Valid_To",
    "Required_Valid_To",
    "RptMonthyear"
]

for c in required_cols:
    if c not in settletment_df.columns:
        settletment_df = settletment_df.withColumn(c,lit(""))

settletment_df = settletment_df.select(*required_cols).dropDuplicates()

req_tbl = spark.table("claims_mgmt.settelment_report")
req_cols = req_tbl.columns

for c in req_cols:
    if c not in settletment_df.columns:
        settletment_df = settletment_df.withColumn(c,lit(""))

settletment_df = settletment_df.select(*req_cols)

date_parser = lambda c: date_format(
    coalesce(
        to_date(col(c),"yyyyMMdd"),
        to_date(col(c),"M/d/yy H:mm"),
        to_date(col(c),"M/d/yy")
    ),
    "yyyyMMdd"
)

settletment_df = (settletment_df
    .withColumn("Required_Valid_To",date_parser("Required_Valid_To"))
    .withColumn("Current_Valid_To",date_parser("Current_Valid_To"))
)
for f in req_tbl.schema:
    settletment_df = settletment_df.withColumn(f.name,F.col(f.name).cast(f.dataType))
spark.sql(f"DELETE FROM claims_mgmt.settelment_report WHERE RptMonthyear='{formatted_date}'")
settletment_df.write.mode("append").saveAsTable("claims_mgmt.settelment_report")
# display(settletment_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select RptMonthyear,Count(*) from claims_mgmt.settelment_report group by RptMonthyear

# COMMAND ----------

# MAGIC %md
# MAGIC ### DIME CS

# COMMAND ----------

# DBTITLE 1,Dime channel summary
import re
from pyspark.sql import functions as F
from pyspark.sql.functions import lit

files = dbutils.fs.ls(f"dbfs:/mnt/datahub/claims_mgmt/{folder}/")

file_path = next(
    f.path for f in files
    if f.name.lower().startswith("dime") and f.name.lower().endswith(".xlsx")
)

dime_chnl_df = (spark.read.format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("inferSchema", "false") #change from true-> false
    .load(file_path)
    .withColumn("RptMonthyear", lit(formatted_date))
    .dropDuplicates())


dime_chnl_df=dime_chnl_df.select(*[
    F.col(f"`{c}`").alias(re.sub(r"\W+","_",c).strip("_"))
    for c in dime_chnl_df.columns
])

rename_map={
    "Sr_No_":"Sr_No",
    "Applicable":"applicable_perc",
    "Applicable_":"applicable_perc",
    "CustomerType":"Customer_Type",
    "PlanFor":"Plan_For",
    "StartDate":"Start_Date",
    "EndDate":"End_Date",
    "BaseConditionProductLevel":"Base_Condition_Product_Level",
    "BaseConditionProductList":"Base_Condition_Product_List"
}

for o,n in rename_map.items():
    if o in dime_chnl_df.columns:
        dime_chnl_df=dime_chnl_df.withColumnRenamed(o,n)


# Applicable_perc into , separated --start FOR APRIL 2026 only
#-- For april 2026, for 1 initCode the file gives 2 or more applicable percentage in different rows instead of being comma separated. Hence grouping by to one row. For other months it is coming as comma separated in file.
if formatted_date == '202604':
    print("Processing for April 2026:: Special Logic")
    group_cols = [
        c for c in dime_chnl_df.columns
        if c not in ["applicable_perc", "Sr_No"]
    ]

    dime_chnl_df= dime_chnl_df.withColumn("applicable_perc", F.col("applicable_perc").cast("float"))

    dime_chnl_df = (
        dime_chnl_df
        .groupBy(*group_cols)
        .agg(
            F.first("Sr_No", ignorenulls=True).alias("Sr_No"),

            F.concat_ws(
                ",",
                F.transform(
                    F.sort_array(
                        F.collect_set(
                            F.col("applicable_perc").cast("double")
                        )
                    ),
                    lambda x: x.cast("string")
                )
            ).alias("applicable_perc")
        )
    )


# Applicable_perc into , separated --end

req_tbl=spark.table("claims_mgmt.dime_channel_summary")
# req_cols=req_tbl.columns

# for c in req_cols:
#     if c not in dime_chnl_df.columns:
#         dime_chnl_df=dime_chnl_df.withColumn(c,lit(""))

# dime_chnl_df=dime_chnl_df.select(*req_cols)

for f in req_tbl.schema:
    if f.name in dime_chnl_df.columns:
        dime_chnl_df=dime_chnl_df.withColumn(f.name,F.col(f.name).cast(f.dataType))

spark.sql(f"DELETE FROM claims_mgmt.dime_channel_summary WHERE RptMonthyear='{formatted_date}'")
dime_chnl_df.write.mode("append").saveAsTable("claims_mgmt.dime_channel_summary")
# display(dime_chnl_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     RptMonthyear,
# MAGIC     COUNT(*) total_rows,
# MAGIC     COUNT(DISTINCT concat_ws('||', *)) distinct_rows
# MAGIC FROM claims_mgmt.dime_channel_summary
# MAGIC GROUP BY RptMonthyear
# MAGIC order by RptMonthyear asc;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Channel Summary

# COMMAND ----------

import re
from pyspark.sql import functions as F
from pyspark.sql.functions import lit


# ============================================================
# 1. CLEAN COLUMN NAMES
# ============================================================

def clean_columns(df):
    original_to_cleaned = {}

    for c in df.columns:
        new_c = re.sub(r"[^A-Za-z0-9]+", "_", c)
        new_c = re.sub(r"_+", "_", new_c)
        new_c = new_c.lower().strip("_")

        original_to_cleaned[c] = new_c

    # Check for collisions after cleaning
    cleaned_names = list(original_to_cleaned.values())

    duplicates = {
        name
        for name in cleaned_names
        if cleaned_names.count(name) > 1
    }

    if duplicates:
        raise ValueError(
            f"Duplicate columns created after cleaning: {sorted(duplicates)}"
        )

    for old_name, new_name in original_to_cleaned.items():
        if old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)

    return df


# ============================================================
# 2. FIND CHANNEL FILES
# ============================================================

files = dbutils.fs.ls(
    f"dbfs:/mnt/datahub/claims_mgmt/{folder}/"
)

file_paths = [
    f.path
    for f in files
    if "channel" in f.name.lower()
    and "dime" not in f.name.lower()
    and f.name.lower().endswith(".xlsx")
]

print("Files picked:")

for path in file_paths:
    print(path.split("/")[-1])


# Stop if no files found
if not file_paths:
    raise ValueError(
        f"No channel .xlsx files found for folder: {folder}"
    )


# ============================================================
# 3. READ ALL FILES
# ============================================================

dfs = []

for path in file_paths:

    print(f"Reading: {path.split('/')[-1]}")

    df = (
        spark.read
        .format("com.crealytics.spark.excel")
        .option("header", "true")
        .load(path)
    )

    df = clean_columns(df)

    dfs.append(df)


# ============================================================
# 4. UNION ALL FILES
# ============================================================

chnl_summ_df = dfs[0]

for df in dfs[1:]:
    chnl_summ_df = chnl_summ_df.unionByName(
        df,
        allowMissingColumns=True
    )


# ============================================================
# 5. ADD REPORTING MONTH
# ============================================================

chnl_summ_df = (
    chnl_summ_df
    .withColumn("rptmonthyear", lit(formatted_date))
)


# ============================================================
# 6. RENAME applicable -> applicable_perc
# ============================================================

if "applicable" in chnl_summ_df.columns:

    if "applicable_perc" in chnl_summ_df.columns:
        raise ValueError(
            "Both 'applicable' and 'applicable_perc' exist. "
            "Cannot safely rename applicable."
        )

    chnl_summ_df = chnl_summ_df.withColumnRenamed(
        "applicable",
        "applicable_perc"
    )


# ============================================================
# 7. MAIN COLUMNS
# ============================================================

main_cols = [
    "plan_for",
    "start_date",
    "end_date",
    "brand",
    "recommended_trade_plan",
    "details",
    "plan_status",
    "method_of_disbursement",
    "applicable_sites",
    "base_condition_product_level",
    "disbursement_product_level",
    "disbursement_product_list",
    "customer_type",
    "applicable_perc"
]


# ============================================================
# 8. REQUIRED COLUMN VALIDATION
# ============================================================

required_cols = ["initcode"] + main_cols

missing_cols = [
    c
    for c in required_cols
    if c not in chnl_summ_df.columns
]

if missing_cols:
    raise ValueError(
        f"Required columns missing from channel files: {missing_cols}"
    )


# ============================================================
# 9. NORMALIZE TYPES
# ============================================================

req_tbl = spark.table("stg.cs_combined")

for f in req_tbl.schema:

    if f.name in chnl_summ_df.columns:

        chnl_summ_df = chnl_summ_df.withColumn(
            f.name,
            F.col(f.name).cast("string")
        )


# ============================================================
# 10. REMOVE ROWS WITH NULL/BLANK MANDATORY COLUMNS
#
# A row is INVALID if ANY of the main_cols is NULL or blank.
# Invalid rows are dropped (not just reported).
# ============================================================
null_condition = F.lit(False)

for c in main_cols:

    null_condition = (
        null_condition
        | F.col(c).isNull()
        | (F.trim(F.col(c).cast("string")) == "")
    )

invalid_count = (
    chnl_summ_df
    .filter(null_condition)
    .count()
)
print(
    f"Rows having at least one NULL/blank mandatory column "
    f"(will be removed): {invalid_count}")

# Keep only rows where NONE of the mandatory columns are null/blank

chnl_summ_df = chnl_summ_df.filter(~null_condition)
after_null_removal = chnl_summ_df.count()
print(
    f"Rows remaining after removing NULL/blank mandatory rows: "
    f"{after_null_removal}"
)


# ============================================================
# 11. DUPLICATION RULE
#
# Duplicate = same initcode, regardless of other columns
# (recommended_trade_plan/details may differ only by a
# scope prefix like "NM " / "RTD,NM,VIJ_" / "VIJ_" for the
# same underlying scheme).
#
# When multiple rows share an initcode, keep ONE row:
#   - Prefer the row where sr_no is NOT null
#     (this is the "clean"/national version in practice).
#   - If all rows for that initcode have null sr_no,
#     keep the first one encountered.
# ============================================================

from pyspark.sql import Window
from pyspark.sql.functions import row_number

before_dedup = chnl_summ_df.count()

if "sr_no" in chnl_summ_df.columns:
    order_col = F.col("sr_no").isNull().asc()
else:
    # sr_no not present - fallback, no real preference,
    # just keeps a deterministic first row per initcode
    order_col = F.lit(0).asc()

dedup_window = Window.partitionBy("initcode").orderBy(order_col)

chnl_summ_df = (
    chnl_summ_df
    .withColumn("_row_rank", row_number().over(dedup_window))
    .filter(F.col("_row_rank") == 1)
    .drop("_row_rank")
)

after_dedup = chnl_summ_df.count()

duplicates_removed = before_dedup - after_dedup


print(
    f"Rows before deduplication: {before_dedup}"
)

print(
    f"Duplicate initcode rows removed: {duplicates_removed}"
)

print(
    f"Final rows to be saved: {after_dedup}"
)


# ============================================================
# 12. DELETE CURRENT REPORTING MONTH
# ============================================================

spark.sql(f"""
DELETE FROM stg.cs_combined
WHERE rptmonthyear = '{formatted_date}'
""")


# ============================================================
# 13. SAVE ALL COLUMNS
# ============================================================

(
    chnl_summ_df.write
    .option("mergeSchema", "true")
    .mode("append")
    .saveAsTable("stg.cs_combined")
)


print(
    f"Successfully saved {after_dedup} rows "
    f"for reporting month {formatted_date}"
)

chnl_summ_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select RptMonthyear,count(*) from stg.cs_combined group by RptMonthyear order by rptmonthyear asc;

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DELETE FROM stg.cs_combined where end_date is null
# MAGIC select 
# MAGIC distinct 
# MAGIC end_date 
# MAGIC from stg.cs_combined 
# MAGIC -- where end_date is null

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE stg.cs_combined
# MAGIC SET end_date = date_format(
# MAGIC     coalesce(
# MAGIC         try_to_timestamp(trim(end_date), 'yyyy-MM-dd HH:mm:ss'),
# MAGIC         try_to_timestamp(trim(end_date), 'yyyy-MM-dd HH:mm:ss.SSS'),
# MAGIC         try_to_timestamp(trim(end_date), 'yyyy-MM-dd'),
# MAGIC         try_to_timestamp(trim(end_date), 'M/d/yy'),
# MAGIC         try_to_timestamp(trim(end_date), 'M/d/yyyy'),
# MAGIC         try_to_timestamp(trim(end_date), 'yyyy/MM/dd'),
# MAGIC         try_to_timestamp(trim(end_date), 'd-MMM-yy'),
# MAGIC         try_to_timestamp(trim(end_date), 'dd-MMM-yy'),
# MAGIC         try_to_timestamp(trim(end_date), 'dd-MM-yyyy'),
# MAGIC         try_to_timestamp(regexp_replace(trim(end_date), '^[A-Za-z]+, ', ''), 'MMMM dd, yyyy')
# MAGIC     ),
# MAGIC     'M/d/yy'
# MAGIC );

# COMMAND ----------

Check date format for cs and dime end date to be used
%sql
UPDATE claims_mgmt.dime_channel_summary
SET End_Date = date_format(
    coalesce(
        try_to_timestamp(trim(end_date), 'yyyy-MM-dd HH:mm:ss'),
        try_to_timestamp(trim(end_date), 'yyyy-MM-dd HH:mm:ss.SSS'),
        try_to_timestamp(trim(end_date), 'M/d/yy'),
        try_to_timestamp(trim(end_date), 'M/d/yyyy'),
        try_to_timestamp(trim(end_date), 'yyyy/MM/dd'),
        try_to_timestamp(trim(end_date), 'd-MMM-yy'),
        try_to_timestamp(trim(end_date), 'dd-MMM-yy'),
        try_to_timestamp(trim(end_date), 'dd-MM-yyyy'),
        try_to_timestamp(
            regexp_replace(trim(end_date), '^[A-Za-z]+, ', ''),
            'MMMM dd, yyyy'
        )
    ),
    'M/d/yy'
);