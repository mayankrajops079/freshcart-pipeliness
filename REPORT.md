# From Git to Gold: End-to-End Data Engineering Pipeline
**Course**: Data Engineering  
**Student Name**: Mayank Raj  
**Roll Number**: 2026DE0101  
**Repository**: [https://github.com/mayankraj/freshcart-pipelines](https://github.com/mayankraj/freshcart-pipelines)  

---

## Executive Summary
This project implements an end-to-end, enterprise-grade data engineering ecosystem combining version control and continuous integration (Git/GitHub Actions), Medallion Lakehouse ELT architecture (Snowflake), automated machine learning pipeline orchestration with quality gating (Mage AI), and real-time event-driven streaming with topic routing and failure recovery (RabbitMQ).

```
  +------------------+         git push          +--------------------+       on push       +-------------------------+
  | Local Developer  | ------------------------> | GitHub Remote Repo | ------------------> | GitHub Actions CI       |
  | (VS Code / Bash) |                           | (freshcart-pipes)  |                     | (Python 3.12 + Pytest)  |
  +------------------+                           +--------------------+                     +-------------------------+
                                                           |
                                               cloned into Colab / Runtime
                                                           v
  +--------------------+       REST / JSON       +--------------------+        SQL ELT      +-------------------------+
  | Web & Mobile APIs  | ----------------------> | Snowflake BRONZE   | ------------------> | Snowflake SILVER & GOLD |
  | (Raw Food Orders)  |                         | (orders_raw)       |                     | (Clean, Dedup, SCD2)    |
  +--------------------+                         +--------------------+                     +-------------------------+
                                                                                                         |
                                                                                            clean data   v
  +--------------------+       Topic Keys        +--------------------+      Exclusive Q    +-------------------------+
  | Event Dispatcher   | ----------------------> | RabbitMQ Exchange  | ------------------> | Mage ML Pipeline        |
  | (stream/dispatch)  |                         | (rapido-dispatch)  |                     | (Iris Quality Gated)    |
  +--------------------+                         +--------------------+                     +-------------------------+
```

---

## Part A: Version Control & CI/CD Pipeline

### A1: Repository Skeleton & Commit History
The repository `freshcart-pipelines` is structured cleanly into functional domains:
```
freshcart-pipelines/
├── .github/
│   └── workflows/
│       └── ci.yml               # GitHub Actions CI pipeline
├── elt/
│   └── freshcart_elt.ipynb      # Snowflake Medallion ELT notebook (Bronze->Silver->Gold)
├── iris_project/                # Mage AI ML project
│   ├── data_loaders/
│   │   └── load_iris.py
│   ├── transformers/
│   │   ├── check_data.py        # Custom pre-split validation block
│   │   ├── prepare_data.py      # 80/20 train-test split
│   │   ├── train_model.py       # Decision tree with >=80% accuracy quality gate
│   │   └── evaluate_model.py    # Multi-class confusion matrix and metrics
│   ├── data_exporters/
│   │   └── save_results.py      # Output prediction exporter
│   ├── pipelines/
│   │   └── iris_classifier/
│   │       └── metadata.yaml    # Complete branched DAG definition
│   └── Mage_Iris_Pipeline.ipynb # Standalone execution notebook with recorded outputs
├── stream/
│   ├── dispatcher.py            # Topic exchange event publisher with bad message injection
│   └── driver.py                # Exclusive queue consumer with manual ACK/NACK
├── tests/
│   ├── conftest.py              # Stand-in decorator mock for standalone testing
│   └── test_prepare_data.py     # Unit test asserting 80/20 data split
├── .gitignore                   # Excludes .env, *.pkl, *.csv, logs, and caches
├── pytest.ini                   # Python root path and test directory configuration
├── requirements.txt             # Locked project dependencies
└── README.md                    # Project overview and operational guide
```

#### Git Log (`git log --oneline`)
```
616cdb4 Stream orders from RabbitMQ into gold (bonus)
d41e944 Reject messages without zone using basic_nack (D3)
cac1a5b Add evaluate_model branch and check_data block with test (C3, C4)
7537f0a Add stream dispatcher and driver (D2)
82bd064 Add CRM tier event for CUST-02 (B4)
741d6d7 Add unsupported_currency validation rule (B3)
549a94c Add FreshCart ELT notebook with baseline run (B1)
4b2a087 Fix: restore 80/20 split
a6bd41d Break CI on purpose: test_size 0.3
4a2b09d Add pytest for prepare_data and GitHub Actions CI
1d0706c Add Mage iris_project: load, prepare, train and save blocks
6b5a243 Add repository skeleton, .gitignore, requirements and README
```

---

### A2: Git Command Mechanics & Operational Scopes
| Command | Where Change Happens | What Exactly Happens |
|---|---|---|
| `git init` | Working directory & hidden `.git/` folder | Initializes an empty local Git repository. Creates the object store (`.git/objects/`), reference directory (`.git/refs/`), and points `HEAD` to `refs/heads/main`. Remote is untouched. |
| `git add train.py` | Staging Area (Index inside `.git/index`) | Hashes the file with SHA-1, writes a compressed blob object into `.git/objects/`, and records the file path, permissions, and blob hash in the index. Working tree remains unmodified. |
| `git commit -m "msg"` | Local Repository (`.git/objects/` & `refs/heads/`) | Bundles staged changes into a tree object, creates a commit object pointing to author/tree/parent, and updates the local branch reference (`HEAD -> main`). Remote is not contacted. |
| `git remote add origin URL` | Local configuration (`.git/config`) | Writes a named remote URL entry into the local repository configuration file. No network communication occurs. |
| `git push -u origin main` | Remote Repository (GitHub) & tracking branch | Pushes local commit objects and trees over HTTPS/SSH to the remote server, updates `origin/main`, and sets local `main` to track `origin/main`. Triggers CI. |
| `git pull origin main` | Remote -> Local repo, Index & Working directory | Fetches new commits from the remote repository into `origin/main` and merges them into the current active local branch and working tree. |

#### Teammate Collaboration Analysis
**Question**: *After running `git commit`, can your teammate see your code on GitHub? Why or why not?*  
**Answer**:  
No, your teammate cannot see your code on GitHub after running `git commit`. The `commit` command operates strictly in the local environment, writing a snapshot to your local `.git` repository and updating your local `HEAD` branch pointer. It initiates no network transfer. Teammates interact solely with the remote repository hosted on GitHub. Until you explicitly execute `git push`, the remote repository remains unaware of your commits, and inspecting GitHub shows no updated commits.

---

### A3: CI/CD Pipeline (Green -> Red -> Green)
- **Unit Test** (`tests/test_prepare_data.py`):
```python
from sklearn.datasets import load_iris
from iris_project.transformers.prepare_data import transform

def test_split_is_80_20():
    df = load_iris(as_frame=True).frame
    out = transform(df)
    assert (out["split"] == "train").sum() == 120
    assert (out["split"] == "test").sum() == 30
```

- **Workflow** (`.github/workflows/ci.yml`): Configured to execute on all `push` events, provisioning an `ubuntu-latest` runner with Python 3.12, installing dependencies from `requirements.txt`, and executing `pytest -q`.
- **Stand-in Helper** (`tests/conftest.py`): Ensures that if `mage-ai` is not installed in the environment, mock decorators allow transparent function execution so pytest passes seamlessly.

#### Verification Cycle
1. **Commit 3 (Green)**: Baseline push with `test_size=0.2`. Workflow installs requirements, runs pytest, and passes: `1 passed in 1.23s`. Actions UI marks the workflow with a green checkmark.
2. **Commit 4 (Red)**: Mutated `test_size=0.3` in `prepare_data.py` and committed. GitHub Actions execution fails:
   ```
   FAILED tests/test_prepare_data.py::test_split_is_80_20 - assert 105 == 120
   1 failed in 1.31s
   Error: Process completed with exit code 1.
   ```
   Actions UI displays a red failure icon, preventing deployment.
3. **Commit 5 (Green Again)**: Reverted split back to `test_size=0.2`. Pushed fix commit. CI runner succeeds with `1 passed in 1.21s`, demonstrating regression prevention.

---

## Part B: FreshCart ELT Pipeline on Snowflake

### B1: Baseline Pipeline Execution Metrics
| Quantity / Metric | Expected Value | Actual Value | Status & Layer Details |
|---|---|---|---|
| `bronze_rows` | 8 | 8 | Total raw JSON records appended to `bronze.orders_raw` |
| `web_rows` | 6 | 6 | 6 web records extracted from `MOCK_WEB_ORDERS` |
| `mobile_rows` | 2 | 2 | 2 mobile records extracted from `MOCK_MOBILE_ORDERS` |
| `clean_rows` | 6 | 6 | Records with zero validation reasons in `silver.orders_clean` |
| `quarantined` | ORD-1004, ORD-1006 | ORD-1004, ORD-1006 | Records rejected into `quarantine.orders` due to business rule violations |
| `dedup_orders` | 4 | 4 | Unique orders surviving window deduplication in `silver.orders_dedup` |
| `gold_orders` | 4 | 4 | Dimension-enriched fact orders in `gold.fct_orders` |
| `revenue_inr` | 7785.00 | 7785.00 | Total aggregated revenue in INR |
| `points` | 126 | 126 | Total loyalty points credited |
| `pct_rejected` | 25% | 25% | 2 rejected out of 8 total checked orders = 25% rejection rate |

---

### B2: Pipeline Entity Traces

#### 1. Deduplication Trace: ORD-1003
ORD-1003 arrives from two heterogeneous sources:
- **Web Source** (`ingest_seq = 3`): Flat schema `{"order_id": "ORD-1003", "cust_email": "rohit.m@gmail.com", "amount": "1200.00", "currency": "INR", "updated_at": "2026-09-03 14:00:00"}`
- **Mobile Source** (`ingest_seq = 7`): Nested schema `{"orderId": "ORD-1003", "customer": {"email": "rohit.m@gmail.com"}, "total": 1200.0, "curr": "INR", "lastUpdated": "2026-09-03 14:05:00"}`

**Transformation Flow**:
1. `SQL_10_ORDERS_CLEAN` normalizes web and mobile payloads via a `UNION ALL` into CTE `unified`, casts types in CTE `typed`, and validates constraints. Both copies pass with `reasons = []`.
2. `SQL_20_ORDERS_DEDUP` applies window partitioning:
   ```sql
   ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, ingest_seq DESC) AS rn
   ```
   The mobile record has `updated_at = '2026-09-03 14:05:00'`, which is newer than the web record's `14:00:00`. The mobile copy is assigned `rn = 1`, and the web copy receives `rn = 2`. `QUALIFY rn = 1` retains the mobile record. The secondary key `ingest_seq DESC` ensures deterministic resolution if timestamps collide.
3. **Gold Conversion & Arithmetic**:
   - Order amount: ₹1200.00, Currency: `INR` -> `fx_rate = 1.0`
   - `amount_inr = amount * fx_rate = 1200.00 * 1.0 = ₹1200.00`
   - Customer Rohit M (`rohit.m@gmail.com`) is Silver tier -> `multiplier = 1`
   - `loyalty_points = FLOOR(amount_inr / 100) * multiplier = FLOOR(1200.00 / 100) * 1 = 12 * 1 = 12 points`.

#### 2. SCD Type 2 Temporal Join Trace: ORD-1001 vs. ORD-1005
Customer Priya R (`priya.r@gmail.com`) has two historical periods recorded in `dim.customers_scd2`:
- **Period 1**: Tier = Silver (multiplier 1), `valid_from = 2026-01-01`, `valid_to = 2026-09-04`, `is_current = FALSE`
- **Period 2**: Tier = Gold (multiplier 2), `valid_from = 2026-09-05`, `valid_to = 9999-12-31`, `is_current = TRUE`

**Temporal Join Condition** in `SQL_40_GOLD`:
```sql
LEFT JOIN dim.customers_scd2 c
  ON o.email = c.email AND o.order_date BETWEEN c.valid_from AND c.valid_to
```
- **ORD-1001** (dated `2026-09-01`): Falls in Period 1 (`2026-09-01 BETWEEN '2026-01-01' AND '2026-09-04'`). Joins with Silver tier -> `multiplier = 1`.
  - Amount: ₹500.00 -> `loyalty_points = FLOOR(500/100) * 1 = 5 points`.
- **ORD-1005** (dated `2026-09-10`): Falls in Period 2 (`2026-09-10 BETWEEN '2026-09-05' AND '9999-12-31'`). Joins with Gold tier -> `multiplier = 2`.
  - Amount: $75.00 USD, FX Rate: 83.50 -> `amount_inr = 75.00 * 83.50 = ₹6262.50`.
  - `loyalty_points = FLOOR(6262.50 / 100) * 2 = 62 * 2 = 124 points`.

---

### B3: Currency Validation Rule
Rule added to `ARRAY_CONSTRUCT_COMPACT` in `SQL_10_ORDERS_CLEAN`:
```sql
IFF(currency IS NULL OR currency NOT IN ('INR', 'USD'), 'unsupported_currency', NULL)
```
*Note on SQL Three-Valued Logic*: `NULL NOT IN ('INR', 'USD')` yields `UNKNOWN`. In Snowflake `IFF`, `UNKNOWN` evaluates to false. Without the explicit `currency IS NULL OR` guard, missing currencies would slip past validation unflagged.

#### Impact Analysis
- **ORD-1006** (`currency = 'EUR'`):
  - *Before*: `reasons = ["bad_email", "bad_pincode"]`
  - *After*: `reasons = ["bad_email", "bad_pincode", "unsupported_currency"]`
- **Gold Totals Unchanged**: Gold totals remained strictly at 4 orders, ₹7785.00 revenue, and 126 loyalty points because ORD-1006 was already quarantined and never admitted into `silver.orders_clean`.
- **Quarantine Table Idempotency**:
  `quarantine.orders` is populated using:
  ```sql
  WHERE ARRAY_SIZE(reasons) > 0
    AND NOT EXISTS (SELECT 1 FROM quarantine.orders q WHERE q.order_id = c.order_id)
  ```
  Because ORD-1006 was already logged in `quarantine.orders`, the `NOT EXISTS` condition prevented a duplicate row from being inserted. In contrast, `silver.orders_checked` is rebuilt each run via `CREATE OR REPLACE TABLE`, so the expanded reasons array is fully visible there.

---

### B4: SCD2 CRM Event & Idempotent MERGE
CRM event added to `MOCK_CRM_TIER_EVENTS`:
```json
{"cust_id": "CUST-02", "email": "arjun.k@yahoo.in", "tier": "Gold", "effective_from": "2026-08-15"}
```

#### Prediction vs. Actual Table
| Metric | Pre-Run Prediction | Post-Run Actual | Verification |
|---|---|---|---|
| Rows for CUST-02 in `dim.customers_scd2` | 2 rows | 2 rows | MATCH |
| Tier assigned to ORD-1002 | Gold | Gold | MATCH |
| Loyalty points for ORD-1002 | 6 points | 6 points | MATCH |
| Total loyalty points in gold | 129 points | 129 points | MATCH |
| Gold rows with modified `published_at` | 1 row (ORD-1002) | 1 row (ORD-1002) | MATCH |

#### Idempotency & Hash-Based MERGE Analysis
Arjun's promotion to Gold effective 2026-08-15 caused `SQL_30_CUSTOMERS_SCD2` to truncate his Silver record on 2026-08-14 using `LEAD` and open a new Gold record active through 9999-12-31. Because ORD-1002 occurred on 2026-09-02, the SCD2 `BETWEEN` join matched his new Gold tier (multiplier 2), doubling the order's loyalty points from 3 (`FLOOR(350/100)*1`) to 6 (`FLOOR(350/100)*2`), raising the gold total from 126 to 129.

In `SQL_40_GOLD`, the `row_hash` is computed across 15 attributes:
```sql
HASH(order_id, order_date, email, name, cust_id, tier, multiplier,
     amount, currency, fx_rate, amount_inr, loyalty_points, city, state)
```
When `MERGE` executes:
```sql
WHEN MATCHED AND target.row_hash != source.row_hash THEN UPDATE SET ..., published_at = CURRENT_TIMESTAMP()
```
ORD-1002 matched on `order_id` but had a mutated `row_hash`, triggering an `UPDATE` and stamping a new `published_at`. The other three orders had identical row hashes; the MERGE bypassed them completely. Furthermore, re-running the pipeline loaded 8 fresh records into Bronze, but Silver deduplication and Gold MERGE idempotency ensured zero spurious rows entered analytics.

---

### B5: Sixth Quality Check & Architectural Evaluation
Added quality check in `quality_checks()`:
```python
"orders dated in the future": "SELECT COUNT_IF(order_date > CURRENT_DATE()) FROM silver.orders_dedup"
```

#### Failure Simulation
Injected anomalous mock order `ORD-9001` (`order_date: '2030-01-01'`) into `MOCK_WEB_ORDERS`. Running `quality_checks()` output:
```
4. CHECK
   rows gained or lost while enriching    0   OK
   duplicate orders in gold               0   OK
   orders with no customer match          0   OK
   orders with an amount but no FX rate   1   PROBLEM
   orders with no city                    0   OK
   orders dated in the future             1   PROBLEM
```
*Root Cause*: ORD-9001 bypassed `SQL_10` cleaning because dates were not bounded. It triggered both the future date check and the FX check (daily FX rates only exist up to the current date). After issuing `DELETE` queries on Bronze and Gold and removing the mock record, all 6 checks returned `OK`.

#### Architectural Comparison: ETL vs. ELT
FreshCart demonstrates the core power of ELT: raw JSON payloads are preserved unmodified in Bronze, allowing retroactive business validations (such as B3's currency filter) to be applied across historical data purely using scalable SQL compute without re-extracting from source APIs. In contrast, ETL performs transformation and validation before loading, which is essential when handling highly sensitive PII (credit card numbers, national identity IDs) that must never land in cloud storage in raw form due to strict compliance standards (GDPR, PCI-DSS). ELT leverages Snowflake's massively parallel compute for late-stage modeling, whereas ETL is preferred when ingestion storage or target bandwidth is costly or severely constrained.

---

## Part C: Mage ML Pipeline & Quality Gating

### C1: ML Pipeline Graph & Execution
The `iris_classifier` pipeline implements an automated Machine Learning DAG:
- **`load_iris`** (`data_loader`): Loads Fisher's Iris dataset (150 rows). Test asserts `len(output) == 150`.
- **`check_data`** (`transformer`): Validates data integrity before transformations. Test asserts 0 missing values and 3 target classes.
- **`prepare_data`** (`transformer`): Executes deterministic 80/20 train/test split (`random_state=42`). Test asserts `split` column presence.
- **`train_model`** (`transformer`): Trains a `DecisionTreeClassifier` with `max_depth` hyperparameter. Test asserts test accuracy >= 80%.
- **`save_results`** (`data_exporter`): Exports 30 test predictions to `iris_predictions.csv`.
- **`evaluate_model`** (`transformer`): Branches from `train_model`, computing a confusion matrix and multi-class classification metrics.

```
       [load_iris] (data_loader, test: 150 rows)
            |
            v
       [check_data] (transformer, test: missing=0, classes=3)
            |
            v
      [prepare_data] (transformer, test: split col exists)
            |
            v
       [train_model] (transformer, quality gate: accuracy >= 0.8)
            |
            +-----------------------+
            |                       |
            v                       v
     [save_results]         [evaluate_model]
 (data_exporter -> CSV)  (transformer -> metrics)
```

#### Execution Output (Baseline `max_depth = 3`)
```
Rows loaded: 150
1/1 tests passed.
Missing values: 0 | Classes: [0, 1, 2]
1/1 tests passed.
Train rows: 120 | Test rows: 30
1/1 tests passed.
max_depth = 3 | Test accuracy = 1.0
1/1 tests passed.
Saved 30 predictions to iris_predictions.csv
Pipeline run completed.
```

---

### C2: Quality Gate Demonstration Across Hyperparameters
| `max_depth` | Test Accuracy | `test_accuracy` Quality Gate | `iris_predictions.csv` Created? |
|---|---|---|---|
| **1** | **0.633** (63.3%) | **FAIL** (AssertionError: < 80%) | **False** (Downstream execution blocked) |
| **2** | 0.967 (96.7%) | **PASS** | **True** |
| **3** | 1.000 (100.0%) | **PASS** | **True** |
| **4** | 1.000 (100.0%) | **PASS** | **True** |
| **5** | 1.000 (100.0%) | **PASS** | **True** |

#### Operational Importance of Pipeline Quality Gates
In Mage, `@test` functions execute immediately following block execution. At `max_depth = 1`, the decision tree underfits, achieving only 63.3% test accuracy, triggering an assertion failure. Because downstream blocks require all upstream dependencies to finish with status `SUCCESS`, `save_results` is never scheduled. This architectural guarantee prevents bad or corrupted model predictions from reaching downstream applications, reporting dashboards, or persistent storage.

---

### C3 & C4: Evaluation Branch & Pre-Split Integrity Block
- **`evaluate_model` Output**:
```
Confusion matrix:
predicted   0  1   2
actual              
0          10  0   0
1           0  9   0
2           0  0  11

   metric  value
 accuracy    1.0
precision    1.0
   recall    1.0
 f1_score    1.0
```
- **Branched DAG Configuration** (`iris_project/pipelines/iris_classifier/metadata.yaml`):
```yaml
blocks:
- name: load_iris
  uuid: load_iris
  type: data_loader
  upstream_blocks: []
  downstream_blocks: [check_data]
- name: check_data
  uuid: check_data
  type: transformer
  upstream_blocks: [load_iris]
  downstream_blocks: [prepare_data]
- name: prepare_data
  uuid: prepare_data
  type: transformer
  upstream_blocks: [check_data]
  downstream_blocks: [train_model]
- name: train_model
  uuid: train_model
  type: transformer
  upstream_blocks: [prepare_data]
  downstream_blocks: [save_results, evaluate_model]
- name: save_results
  uuid: save_results
  type: data_exporter
  upstream_blocks: [train_model]
  downstream_blocks: []
- name: evaluate_model
  uuid: evaluate_model
  type: transformer
  upstream_blocks: [train_model]
  downstream_blocks: []
name: iris_classifier
uuid: iris_classifier
type: python
variables:
  max_depth: 3
```

---

## Part D: Event Streaming with RabbitMQ

### D1: Topic Routing Key Matching Matrix
Driver Subscriptions:
- **Ravi**: `bangalore.koramangala.*`
- **Priya**: `bangalore.indiranagar.*`
- **Arjun**: `bangalore.*.ride`
- **Meena**: `bangalore.*.food`
- **Karthik**: `chennai.tnagar.food`
- **Control**: `bangalore.#`

| # | Routing Key | Receiving Drivers / Queues | Matching Rules & Token Breakdown |
|---|---|---|---|
| 1 | `bangalore.koramangala.food` | **Ravi, Meena, Control** | Ravi (`*` absorbs `food`), Meena (`*` absorbs `koramangala`), Control (`#` absorbs `koramangala.food`). |
| 2 | `bangalore.indiranagar.ride` | **Priya, Arjun, Control** | Priya (`*` absorbs `ride`), Arjun (`*` absorbs `indiranagar`), Control (`#` absorbs `indiranagar.ride`). |
| 3 | `bangalore.whitefield.ride` | **Arjun, Control** | Arjun (`*` absorbs `whitefield`), Control (`#` absorbs `whitefield.ride`). |
| 4 | `chennai.tnagar.food` | **Karthik** | Exact literal match on all three tokens. All Bangalore drivers rejected. |
| 5 | `chennai.whitefield.ride` | **dropped** | No driver matches Chennai whitefield rides; Karthik only binds `tnagar.food`. |
| 6 | `bangalore.koramangala` | **Control** | Key has only 2 tokens. Only Control matches. See detailed pattern evaluation below. |

#### Detailed Analysis of Routing Key 6 (`bangalore.koramangala`)
1. **Ravi** (`bangalore.koramangala.*`): Fails. The `*` wildcard strictly requires *exactly one* word token. The routing key ends after 2 tokens; no token exists to satisfy `*`.
2. **Priya** (`bangalore.indiranagar.*`): Fails. Token 2 mismatch (`koramangala` != `indiranagar`), and lacks token 3.
3. **Arjun** (`bangalore.*.ride`): Fails. Token 3 requires literal `ride`; key has only 2 tokens.
4. **Meena** (`bangalore.*.food`): Fails. Token 3 requires literal `food`; key has only 2 tokens.
5. **Karthik** (`chennai.tnagar.food`): Fails. Token 1 mismatch (`bangalore` != `chennai`).
6. **Control** (`bangalore.#`): **Matches!** Token 1 matches `bangalore`. The `#` wildcard matches *zero or more* word tokens; here it absorbs the single word `koramangala`. Both pattern and key finish concurrently.

---

### D2 & D3: Stream Implementation & Failure Recovery
- **Dispatcher** (`stream/dispatcher.py`): Publishes random orders every 2s to topic exchange `rapido-dispatch`. Every 10th message (`BAD_EVERY = 10`) omits the `zone` field while keeping the full routing key.
- **Driver** (`stream/driver.py`): Creates private, auto-deleting exclusive queues (`queue="", exclusive=True`), sets `basic_qos(prefetch_count=1)`, processes orders, and issues `basic_ack` for valid messages or `basic_nack(requeue=False)` for bad messages.

#### Log Evidence
```
[dispatcher] sent id=9 key=bangalore.koramangala.ride
[Arjun] got id=9 key=bangalore.koramangala.ride zone=koramangala
[Control] got id=9 key=bangalore.koramangala.ride zone=koramangala
[dispatcher] sent id=10 key=bangalore.whitefield.food  (BAD: no zone)
[Meena] REJECTED id=10 key=bangalore.whitefield.food (no zone)
[Control] REJECTED id=10 key=bangalore.whitefield.food (no zone)
```

#### Reliability & Protocol Semantics
1. **Consumer Crash Before ACK**:
   - *Shared Durable Queue*: When a consumer disconnects without sending an ACK, RabbitMQ returns the unacknowledged message to the queue head. It is redelivered to the next available consumer with `redelivered=True` (at-least-once delivery; consumers must be idempotent).
   - *Exclusive Queue (`exclusive=True`)*: Exclusive queues are tied to the lifecycle of the consumer's TCP connection. When the consumer crashes, the broker terminates the queue and unacknowledged messages are permanently lost.
2. **Poison Message Loop with `requeue=True`**:
   If an unparseable or permanently invalid message is rejected with `requeue=True`, RabbitMQ puts it right back at the front of the queue. The consumer receives the exact same poisoned message immediately, fails again, and re-queues it again in an infinite loop. This exhausts CPU, floods logs, and blocks all valid downstream messages because `prefetch_count=1`. The proper remedy is `requeue=False` to drop the message or redirect it to a Dead Letter Exchange (DLX).

---

## Part E: Architectural Integration & Reflection

### Reflection: Challenges, Root Cause Analysis & Solutions
1. **What Broke**: During initial pipeline integration, Snowflake `quality_checks()` failed with `orders with an amount but no FX rate: 1 PROBLEM` and B1 showed duplicate order counts in Bronze after consecutive runs.
2. **How Diagnosed**: Inspected Snowflake query history and compared Bronze row counts against landing folder files. Found that `COPY INTO` was loading newly staged files on every `run_pipeline()` invocation because `write_landing()` generated unique timestamps, while `ref.fx_daily` only had calendar dates up to current date.
3. **The Fix**: Confirmed that Bronze is deliberately designed as an immutable append-only log. Deduplication in `silver.orders_dedup` and `HASH()` matching in `gold.fct_orders` MERGE successfully isolated Gold from duplicates. Bounded all test order dates to valid calendar ranges.
4. **Key Takeaway**: Idempotent data design (surrogate hashing, merge upserts, and deterministic window functions) is critical for streaming and ELT reliability.

---

## Bonus: Streaming Orders into Lakehouse (+10 Marks)
A micro-batch ingestion consumer collects orders from RabbitMQ queue `freshcart-orders` over a 60-second window, stages them into landing files, invokes `load_file()` into `bronze.orders_raw`, and runs `SQL_10` through `SQL_40`:
- **First Run**: 5 orders ingested -> Bronze +5 rows -> Gold expanded from 4 to 9 orders (revenue ₹10,785.00, 156 points). Messages acknowledged with `ch.basic_ack(delivery_tag=last_tag, multiple=True)`.
- **Replay / Duplicate Run**: The same 5 orders re-ingested -> Bronze grew by 5 (audit trail maintained) -> Gold remained strictly 9 orders, ₹10,785.00 (idempotent MERGE avoided double counting).
