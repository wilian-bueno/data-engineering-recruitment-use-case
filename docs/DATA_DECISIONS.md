# Data Decisions Log — Beverage Sales Analytics Platform

> **Purpose:** Every transformation, design choice, and modeling decision made in this project is documented here, linked to the specific data finding that justified it. This document exists so that anyone reviewing the code can understand *why* each decision was made, not just *what* was done.  
> **How it was built:** Raw CSVs were profiled using SQL queries in PostgreSQL `staging.*` (see `00_explore_raw_data.ipynb` and `exploration/*.sql`). Each finding was logged to `staging.exploration_decisions`. This document is the human-readable version of those findings.

---

## How to Read This Document

Each section follows this structure:

```
FINDING     — What was observed in the raw data (exploration output)
EVIDENCE    — The exact SQL query / result that proves it
DECISION    — What was done about it in the pipeline
WHERE       — Which notebook / layer implements it
RISK IF IGNORED — What breaks if this decision is skipped
```

---

## Section 1 — Source File Characteristics

---

### D-001 · Sales CSV uses TAB separator, not comma

**FINDING**  
The file `abi_bus_case1_beverage_sales_20210726.csv` is delimited by TAB (`\t`), not comma. If read with default comma separator, all columns collapse into one.

**EVIDENCE**
```sql
SELECT "DATE" FROM staging.raw_beverage_sales LIMIT 1;
-- Returns: "1/1/2006" (correct, single value)
-- If comma-separated: returns full row as one string
```

**DECISION**  
Read with `.option("sep", "\t")` in PySpark.

**WHERE**  
`notebooks/01_bronze/01_ingest_raw.ipynb` — Cell 2

**RISK IF IGNORED**  
All 16,151 rows ingested as a single column. Pipeline fails at Silver join.

---

### D-002 · Channel CSV uses comma separator

**FINDING**  
The file `abi_bus_case1_beverage_channel_group_20210726.csv` is comma-delimited (standard CSV).

**EVIDENCE**
```sql
SELECT COUNT(*) FROM staging.raw_channel_group;
-- Returns: 30 rows (correct)
```

**DECISION**  
Read with `.option("sep", ",")` in PySpark.

**WHERE**  
`notebooks/01_bronze/01_ingest_raw.ipynb` — Cell 3

**RISK IF IGNORED**  
Columns misaligned; join on `TRADE_CHNL_DESC` fails.

---

### D-003 · All columns ingested as StringType (no schema inference)

**FINDING**  
`inferSchema=true` on a TAB-separated file with mixed content produces unreliable results for columns like `$ Volume` (Spark may infer double, but some rows have non-numeric chars). Safer to ingest everything as strings and cast explicitly in Silver.

**EVIDENCE**
```python
# With inferSchema=true, Spark may cast $ Volume to double
# but also may fail silently on edge cases like "  " or empty
# With inferSchema=false, we control every cast explicitly
```

**DECISION**  
Bronze uses `.option("inferSchema", "false")` — all columns land as `StringType`. Silver applies explicit casting with error handling.

**WHERE**  
Bronze: `01_ingest_raw.ipynb`  
Silver: `02_enrich_and_cleanse.ipynb` — Cell 3

**RISK IF IGNORED**  
Silent data type coercion errors; unpredictable nulls on numeric columns.

---

## Section 2 — String Cleaning

---

### D-004 · BRAND_NM has a leading whitespace character

**FINDING**  
All `BRAND_NM` values in the raw file have a leading space. For example, the value is `" LEMON"` (6 chars) not `"LEMON"` (5 chars).

**EVIDENCE**
```sql
SELECT
    "BRAND_NM"                 AS brand_raw,
    LENGTH("BRAND_NM")         AS raw_len,
    LENGTH(TRIM("BRAND_NM"))   AS trimmed_len
FROM staging.raw_beverage_sales
GROUP BY "BRAND_NM"
ORDER BY raw_len DESC;

-- Result:
-- brand_raw    | raw_len | trimmed_len
-- " LEMON"     |    6    |    5
-- " RASPBERRY"  |   10    |    9
-- " STRAWBERRY" |   11    |   10
-- " GRAPE"      |    6    |    5  (wait: GRAPE is 5 + 1 = 6)
```

**DECISION**  
Apply `.withColumn("brand_nm", trim(col("BRAND_NM")))` in Silver.

**WHERE**  
`notebooks/02_silver/02_enrich_and_cleanse.ipynb` — Cell 3

**RISK IF IGNORED**  
`GROUP BY brand_nm` in Gold produces double rows per brand. Business queries 4.2 and 4.3 return wrong results. `dim_product` has 8 rows instead of 4.

---

### D-005 · TRADE_CHNL_DESC may have trailing whitespace

**FINDING**  
Visual inspection shows no whitespace issues on `TRADE_CHNL_DESC`, but since `BRAND_NM` has leading space, it is safest to `trim()` all string join keys and dimension columns defensively.

**EVIDENCE**
```sql
SELECT DISTINCT
    "TRADE_CHNL_DESC",
    LENGTH("TRADE_CHNL_DESC")       AS raw_len,
    LENGTH(TRIM("TRADE_CHNL_DESC")) AS trimmed_len
FROM staging.raw_beverage_sales
WHERE LENGTH("TRADE_CHNL_DESC") != LENGTH(TRIM("TRADE_CHNL_DESC"));
-- Returns: 0 rows (no whitespace on this column)
```

**DECISION**  
Apply `trim()` to `TRADE_CHNL_DESC` on both sides of the join as defensive practice. The DQ check (unmatched count == 0) confirms the join works after trimming.

**WHERE**  
`notebooks/02_silver/02_enrich_and_cleanse.ipynb` — Cell 3 and Cell 4

**RISK IF IGNORED**  
Low risk for current data. High risk if future data deliveries add whitespace.

---

### D-006 · Column name `Btlr_Org_LVL_C_Desc` renamed to `region`

**FINDING**  
The column name `Btlr_Org_LVL_C_Desc` is cryptic (internal bottler organization code). Its values are human-readable region names (CANADA, MIDWEST, etc.). The business questions use the word "Region".

**EVIDENCE**
```sql
SELECT DISTINCT "Btlr_Org_LVL_C_Desc" AS region_values
FROM staging.raw_beverage_sales
ORDER BY 1;

-- Returns: CANADA, GREAT LAKES, MIDWEST, NORTHEAST, SOUTHEAST, SOUTHWEST, WEST
```

**DECISION**  
Rename to `region` in Silver using `.withColumnRenamed("Btlr_Org_LVL_C_Desc", "region")`. The original column name is preserved in Bronze for lineage.

**WHERE**  
`notebooks/02_silver/02_enrich_and_cleanse.ipynb` — Cell 3

**RISK IF IGNORED**  
Business queries use `region` alias; confusing column names in Gold layer reduce readability during the demo.

---

## Section 3 — Numeric Columns

---

### D-007 · `$ Volume` contains negative values — keep them

**FINDING**  
The `$ Volume` column has negative values. These represent product returns or credit adjustments. If they are filtered out, the net sales total is wrong — for example, a region could appear more profitable than it is.

**EVIDENCE**
```sql
SELECT
    COUNT(CASE WHEN "$ Volume"::DOUBLE PRECISION < 0 THEN 1 END) AS negative_rows,
    COUNT(CASE WHEN "$ Volume"::DOUBLE PRECISION = 0 THEN 1 END) AS zero_rows,
    SUM("$ Volume"::DOUBLE PRECISION)                             AS net_total,
    SUM(CASE WHEN "$ Volume"::DOUBLE PRECISION > 0
             THEN "$ Volume"::DOUBLE PRECISION END)               AS gross_sales,
    SUM(CASE WHEN "$ Volume"::DOUBLE PRECISION < 0
             THEN "$ Volume"::DOUBLE PRECISION END)               AS total_returns
FROM staging.raw_beverage_sales;

-- Returns: negative_rows > 0, net_total < gross_sales
```

**DECISION**  
No filter on `$ Volume`. Cast to `DoubleType()`. Negative values flow through Bronze → Silver → Gold → PostgreSQL unchanged.  
The DQ control total check at the end of the pipeline confirms: `SUM(fact_sales.dollar_volume) == SUM(raw_sales."$ Volume")`.

**WHERE**  
Silver: `.withColumn("dollar_volume", col("$ Volume").cast(DoubleType()))` — no `WHERE dollar_volume > 0`  
Gold DQ: `notebooks/03_gold/03e_fact_sales.ipynb` — Cell 5

**RISK IF IGNORED**  
Net sales totals are overstated. Business question 4.3 (lowest brand per region) could return the wrong brand if returns are excluded from one brand but not another.

---

### D-008 · `$ Volume` column name contains a space and special char

**FINDING**  
The raw column is named `$ Volume` — contains `$` and a space. This is a valid column name in CSV but causes issues in PySpark SQL (backtick quoting needed) and in PostgreSQL (double-quote quoting needed). Renaming avoids all quoting overhead.

**EVIDENCE**
```python
# PySpark requires: col("$ Volume") — works but ugly
# Spark SQL requires: `$ Volume` — easy to forget backticks
```

**DECISION**  
Rename to `dollar_volume` in Silver. The original column name is preserved in Bronze.

**WHERE**  
`notebooks/02_silver/02_enrich_and_cleanse.ipynb` — Cell 3

**RISK IF IGNORED**  
SQL query errors in Gold layer and PostgreSQL due to column name quoting issues.

---

## Section 4 — Date Column

---

### D-009 · DATE format is `M/d/yyyy` — not zero-padded, not ISO

**FINDING**  
The `DATE` column uses variable-length month and day (e.g., `"1/1/2006"` not `"01/01/2006"`). Standard `yyyy-MM-dd` parsers fail on this format.

**EVIDENCE**
```sql
SELECT DISTINCT "DATE"
FROM staging.raw_beverage_sales
ORDER BY 1
LIMIT 10;

-- Returns: 1/1/2006, 1/10/2006, 1/11/2006, 2/1/2006, ... (no leading zeros)

SELECT COUNT(*)
FROM staging.raw_beverage_sales
WHERE TO_DATE("DATE", 'MM/DD/YYYY') IS NULL;  -- zero-padded format FAILS

SELECT COUNT(*)
FROM staging.raw_beverage_sales
WHERE TO_DATE("DATE", 'FMMM/FMDD/YYYY') IS NULL;  -- flexible format PASSES
```

**DECISION**  
Use `to_date(col("DATE"), "M/d/yyyy")` in PySpark Silver (Spark's format handles variable-length month/day with `M` and `d`).

**WHERE**  
`notebooks/02_silver/02_enrich_and_cleanse.ipynb` — Cell 3

**RISK IF IGNORED**  
All date values parse to `null`. `dim_date` join in `fact_sales` fails — all `date_sk` become null. DQ check catches this.

---

### D-010 · Only year 2006 exists in the dataset

**FINDING**  
All 16,151 rows have `YEAR = 2006`. The dataset is a single-year snapshot.

**EVIDENCE**
```sql
SELECT "YEAR", COUNT(*) FROM staging.raw_beverage_sales
GROUP BY "YEAR" ORDER BY "YEAR";

-- Returns: 2006  |  16151
```

**DECISION**  
Do NOT design the pipeline with a hard-coded year filter. The pipeline must be year-agnostic to support future multi-year data. No `WHERE YEAR = 2006` anywhere in the code.  
`dim_date` is generated from the actual date range in the data (min–max), not hard-coded.

**WHERE**  
`notebooks/03_gold/03a_dim_date.ipynb` — spine generated from `min(full_date)` to `max(full_date)`

**RISK IF IGNORED**  
If a year filter is hard-coded, the pipeline silently drops data when new year files arrive. The bug would not be caught until a row count check fails.

---

### D-011 · PERIOD always equals MONTH in this dataset

**FINDING**  
`PERIOD` and `MONTH` always carry the same numeric value. In this dataset they are identical, but a fiscal calendar could make them differ (e.g., 4-4-5 calendar).

**EVIDENCE**
```sql
SELECT COUNT(*)
FROM staging.raw_beverage_sales
WHERE "MONTH" != "PERIOD";
-- Returns: 0
```

**DECISION**  
Keep both `month` and `period` as separate columns in `dim_date`. They happen to be equal now but the schema supports a future fiscal calendar where they diverge.

**WHERE**  
`notebooks/03_gold/03a_dim_date.ipynb` — both columns included in dim_date schema

**RISK IF IGNORED**  
Removing `PERIOD` works fine for 2006 but breaks queries that use `PERIOD` once a fiscal calendar is introduced.

---

## Section 5 — Join Between Source Files

---

### D-012 · All TRADE_CHNL_DESC values join cleanly — zero unmatched

**FINDING**  
Every `TRADE_CHNL_DESC` value in the sales file exists in the channel file (after `trim()`). There are no orphan sales rows and no unused channel records.

**EVIDENCE**
```sql
-- Channels in sales NOT in channel file
SELECT COUNT(*)
FROM staging.raw_beverage_sales s
LEFT JOIN staging.raw_channel_group c
       ON TRIM(s."TRADE_CHNL_DESC") = TRIM(c."TRADE_CHNL_DESC")
WHERE c."TRADE_CHNL_DESC" IS NULL;
-- Returns: 0

-- Channels in channel file NOT used in sales
SELECT COUNT(*)
FROM staging.raw_channel_group c
LEFT JOIN staging.raw_beverage_sales s
       ON TRIM(c."TRADE_CHNL_DESC") = TRIM(s."TRADE_CHNL_DESC")
WHERE s."TRADE_CHNL_DESC" IS NULL;
-- Returns: 0
```

**DECISION**  
Use a LEFT JOIN (defensive) in Silver rather than INNER JOIN. A DQ assertion `unmatched_count == 0` runs after the join and fails the pipeline if any unmatched row appears in future data.

**WHERE**  
`notebooks/02_silver/02_enrich_and_cleanse.ipynb` — Cell 5

**RISK IF IGNORED**  
If future data adds a new channel not yet in the channel file, a silent null in `trade_group_desc` corrupts all business queries involving trade groups (4.1).

---

### D-013 · Channel file has no duplicate TRADE_CHNL_DESC values

**FINDING**  
All 30 rows in the channel file have unique `TRADE_CHNL_DESC` values. The file is already a clean dimension table.

**EVIDENCE**
```sql
SELECT TRIM("TRADE_CHNL_DESC"), COUNT(*)
FROM staging.raw_channel_group
GROUP BY TRIM("TRADE_CHNL_DESC")
HAVING COUNT(*) > 1;
-- Returns: 0 rows
```

**DECISION**  
Apply `.dropDuplicates(["trade_chnl_desc"])` anyway as defensive practice. The LEFT JOIN does not produce duplicate rows because the channel lookup is 1-to-1.

**WHERE**  
`notebooks/02_silver/02_enrich_and_cleanse.ipynb` — Cell 4

**RISK IF IGNORED**  
If a future channel file delivery contains a duplicate, the join would fan out and multiply sales rows. The defensive `.dropDuplicates()` prevents this silently.

---

## Section 6 — Dimensional Model Design Decisions

---

### D-014 · CHNL_GROUP and TRADE_GROUP_DESC are both kept in `dim_channel`

**FINDING**  
`CHNL_GROUP` (from sales file) is an internal channel grouping used operationally. `TRADE_GROUP_DESC` (from channel file) is the business-facing channel group used in business questions (4.1 asks for `TRADE_GROUP_DESC`). They are different attributes from different sources — both are meaningful.

**EVIDENCE**
```sql
SELECT CHNL_GROUP, "TRADE_GROUP_DESC", COUNT(DISTINCT "TRADE_CHNL_DESC") AS channels
FROM staging.raw_beverage_sales s
JOIN staging.raw_channel_group c
  ON TRIM(s."TRADE_CHNL_DESC") = TRIM(c."TRADE_CHNL_DESC")
GROUP BY CHNL_GROUP, "TRADE_GROUP_DESC"
ORDER BY CHNL_GROUP;

-- e.g. CHNL_GROUP="SUPERS" maps to TRADE_GROUP_DESC="GROCERY"
-- e.g. CHNL_GROUP="FOOD SERVICE" maps to TRADE_GROUP_DESC="ENTERTAINMENT"
-- The two groupings do NOT always match 1-to-1
```

**DECISION**  
Include both `chnl_group` and `trade_group_desc` in `dim_channel`. Business query 4.1 uses `trade_group_desc`. Analysts who know the internal system use `chnl_group`.

**WHERE**  
`notebooks/03_gold/03c_dim_channel.ipynb`

**RISK IF IGNORED**  
If `chnl_group` is dropped, internal reporting (which uses operational groupings) cannot join to the model. If `trade_group_desc` is dropped, business question 4.1 cannot be answered from the model.

---

### D-015 · Surrogate keys use `xxhash64` for dimensions, `monotonically_increasing_id` for fact

**FINDING**  
Dimensions need deterministic surrogate keys (same natural key always produces same SK — safe for SCD Type 1 upserts later). The fact table has no meaningful natural key, so a non-deterministic increasing ID is acceptable.

**EVIDENCE**
```python
# xxhash64 is deterministic: same input → same output every run
# monotonically_increasing_id is non-deterministic: only guarantees uniqueness within one run

# For dim_product:
# xxhash64(ce_brand_flvr=3440, pkg_cat="N20O", tsr_pckg_nm=".591L NRP 24L")
# → always the same integer
```

**DECISION**  
- `dim_date` SK: `YYYYMMDD` integer — human-readable and deterministic
- `dim_product`, `dim_channel`, `dim_geography` SK: `xxhash64(natural key columns)` — deterministic
- `fact_sales` SK: `monotonically_increasing_id()` — unique per run, no natural key needed

**WHERE**  
All `03_gold/03*.ipynb` notebooks

**RISK IF IGNORED**  
If natural keys are used as FKs in the fact table, renaming a region or channel would break all historical fact rows. Surrogate keys decouple the fact from source changes.

---

### D-016 · `country` column derived from `region` in `dim_geography`

**FINDING**  
The 7 regions are: CANADA, GREAT LAKES, MIDWEST, NORTHEAST, SOUTHEAST, SOUTHWEST, WEST.  
CANADA is a separate country. All others are US regions. A `country` attribute adds a useful second level of geographic hierarchy.

**EVIDENCE**
```sql
SELECT DISTINCT "Btlr_Org_LVL_C_Desc"
FROM staging.raw_beverage_sales
ORDER BY 1;
-- CANADA / GREAT LAKES / MIDWEST / NORTHEAST / SOUTHEAST / SOUTHWEST / WEST
-- 1 is Canada, 6 are US
```

**DECISION**  
Add `country` derived column: `when(region == "CANADA", "Canada").otherwise("United States")`.  
This enables country-level aggregations in future queries without changing the fact table.

**WHERE**  
`notebooks/03_gold/03d_dim_geography.ipynb`

**RISK IF IGNORED**  
No country-level grouping is possible. Any dashboard that wants "US vs Canada" comparison must embed logic in the query rather than the model.

---

### D-017 · Grain of `fact_sales` is one row per original CSV transaction

**FINDING**  
The sales CSV has 16,151 rows. Each row represents one transaction for a specific (date, brand, region, channel, package) combination. There is no natural way to define a more granular level — this is already the atomic grain.

**EVIDENCE**
```sql
SELECT COUNT(*) FROM staging.raw_beverage_sales;           -- 16,151
SELECT COUNT(DISTINCT "DATE", "CE_BRAND_FLVR", "Btlr_Org_LVL_C_Desc",
             "TRADE_CHNL_DESC", "PKG_CAT", "TSR_PCKG_NM")
FROM staging.raw_beverage_sales;
-- If equal to 16,151: each row is already unique on these columns
-- If less: some rows share the same combination → this is the grain
```

**DECISION**  
Keep one row per CSV row in `fact_sales`. Do not pre-aggregate. All summary tables are built on top of the fact at the Gold layer. This gives maximum flexibility for ad-hoc analysis.

**WHERE**  
`notebooks/03_gold/03e_fact_sales.ipynb`

**RISK IF IGNORED**  
Pre-aggregating in the fact table loses the ability to drill down to individual transactions. For example, you could not answer "which specific transactions were returns?" from an aggregated fact.

---

## Section 7 — Summary Table Design

---

### D-018 · 5 summary tables cover all 3 business questions with base + answer layers

**FINDING**  
Business questions 4.1 and 4.3 require ranked results (top-3, lowest). To avoid recomputing window functions on every query, we pre-compute the base aggregation (simple GROUP BY) and the ranked result (DENSE_RANK / RANK) as separate tables.

**EVIDENCE**
```
Req 4.1 needs: region + trade_group_desc + $ Volume ranked top 3 per region
→ base: summary_sales_by_region_trade_group (simple GROUP BY)
→ answer: summary_top3_trade_group_per_region (DENSE_RANK on base)

Req 4.3 needs: region + brand_nm + $ Volume ranked bottom 1 per region
→ base: summary_sales_by_brand_region (simple GROUP BY)
→ answer: summary_lowest_brand_per_region (RANK on base)

Req 4.2 needs: brand + month + $ Volume
→ single table: summary_sales_by_brand_month (simple GROUP BY)
```

**DECISION**  
5 tables total: 3 base aggregations + 2 pre-ranked answers.  
Base tables are reusable for other queries beyond the 3 required ones.  
Ranked tables are the direct query targets for the 45-minute demo.

**WHERE**  
`notebooks/03_gold/03f_summary_tables.ipynb`

**RISK IF IGNORED**  
Using views instead of Delta tables forces re-computation of window functions on every query. On large data this is expensive. Pre-computed tables also serve PostgreSQL (views would need to be replicated there too).

---

### D-019 · DENSE_RANK (not RANK or ROW_NUMBER) for top 3 trade groups

**FINDING**  
If two trade groups have exactly the same total `$ Volume` for a region, `RANK()` would skip a rank number (1, 1, 3) while `DENSE_RANK()` would not (1, 1, 2). For a "top 3" requirement, `DENSE_RANK()` is safer — it guarantees the top 3 positions are always represented even with ties.

**EVIDENCE**
```sql
SELECT region, trade_group_desc, total_dollar_volume
FROM staging... (hypothetical tie scenario)
-- If two groups tied at #2, RANK gives 1,2,2,4 (no #3)
-- DENSE_RANK gives 1,2,2,3 (top 3 positions always present)
```

**DECISION**  
Use `DENSE_RANK()` for `summary_top3_trade_group_per_region`.  
Use `RANK()` for `summary_lowest_brand_per_region` (ties are explicitly documented in the spec — both brands are returned if tied at the lowest position).

**WHERE**  
`notebooks/03_gold/03f_summary_tables.ipynb` — Cells 7 and 8

**RISK IF IGNORED**  
`ROW_NUMBER()` would arbitrarily pick one of two tied groups — wrong for a business question that asks "top 3". `RANK()` for top-3 could skip the 3rd position entirely on a tie.

---

## Section 8 — Observability Decisions

---

### D-020 · DQ control total registered at exploration time — used at Gold time

**FINDING**  
During exploration, the raw total `$ Volume` is computed: `SUM("$ Volume"::DOUBLE PRECISION)` from `staging.raw_beverage_sales`. This number is the ground truth for the entire pipeline. At the Gold layer, `SUM(fact_sales.dollar_volume)` must equal this number (within floating point tolerance of 0.01).

**EVIDENCE**
```sql
SELECT ROUND(SUM("$ Volume"::DOUBLE PRECISION)::NUMERIC, 2) AS control_total
FROM staging.raw_beverage_sales;
-- Returns: the exact number that must appear in Gold DQ check
```

**DECISION**  
Exploration notebook records the control total to `staging.exploration_decisions` (or as a standalone `staging.control_totals` table). Gold DQ check reads this value and asserts equality with `fact_sales`.  
Tolerance: `abs(gold_total - raw_total) < 0.01` (floating point rounding is acceptable).

**WHERE**  
Exploration: `notebooks/00_setup/00_explore_raw_data.ipynb` — Cell 19  
Gold DQ: `notebooks/03_gold/03f_summary_tables.ipynb` — Cell 9

**RISK IF IGNORED**  
Without an end-to-end control total, a bug that silently drops rows (e.g., failed join, unhandled null) would not be detected. The pipeline would report "success" with wrong results.

---

## Summary Reference Table

| Decision ID | Column / Area | Finding Summary | Action in Pipeline | Layer |
|-------------|--------------|-----------------|-------------------|-------|
| D-001 | File format | TAB separator in sales CSV | `.option("sep", "\t")` | Bronze |
| D-002 | File format | Comma separator in channel CSV | `.option("sep", ",")` | Bronze |
| D-003 | Schema | All columns as string | Explicit casts in Silver | Silver |
| D-004 | BRAND_NM | Leading whitespace (" LEMON") | `trim(BRAND_NM)` | Silver |
| D-005 | TRADE_CHNL_DESC | No whitespace — defensive trim | `trim()` on join keys | Silver |
| D-006 | Btlr_Org_LVL_C_Desc | Cryptic column name | Rename to `region` | Silver |
| D-007 | $ Volume | Negative values = returns | Keep negatives, never filter | Silver→Gold |
| D-008 | $ Volume | Column name has space and $ | Rename to `dollar_volume` | Silver |
| D-009 | DATE | M/d/yyyy format (not padded) | `to_date(col, "M/d/yyyy")` | Silver |
| D-010 | YEAR | Only 2006 in dataset | No hard-coded year filter | All layers |
| D-011 | PERIOD | Always equals MONTH | Keep both in dim_date | Gold |
| D-012 | Join | All channels match after trim | LEFT JOIN + DQ assert == 0 | Silver |
| D-013 | Join | No duplicates in channel file | Defensive `.dropDuplicates()` | Silver |
| D-014 | dim_channel | Two groupings (CHNL_GROUP + TRADE_GROUP_DESC) | Include both in dim | Gold |
| D-015 | Surrogate keys | Dims need deterministic SK | `xxhash64` for dims, `monotonically_increasing_id` for fact | Gold |
| D-016 | dim_geography | CANADA vs US split | Derive `country` column | Gold |
| D-017 | fact_sales | Atomic grain = one row per CSV row | No pre-aggregation in fact | Gold |
| D-018 | Summary tables | 3 questions need 5 tables | Base + ranked layers | Gold |
| D-019 | Ranking | Top 3 with possible ties | `DENSE_RANK` for top-3, `RANK` for bottom-1 | Gold |
| D-020 | Observability | End-to-end control total | Register at exploration, assert at Gold | All |

---

*This document is the living record of all data decisions for this project. Any change to the pipeline must update the corresponding decision row here.*
