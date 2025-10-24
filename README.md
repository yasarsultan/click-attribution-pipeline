# Near Real-Time Attribution Pipeline

A marketing attribution system built with **dbt**, **BigQuery**, and **Python streaming samples** that demonstrates First-Click and Last-Click attribution models with near real-time data processing.

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation & Setup](#installation--setup)
- [Usage](#usage)

---

## Overview

This project implements an attribution pipeline that:

- **Processes historical GA4 data** from BigQuery public dataset (Jan 2021)
- **Performs sessionization** with configurable timeout (default: 30 minutes)
- **Calculates attribution** using First-Click and Last-Click models
- **Supports incremental processing** to handle new data efficiently
- **Provides Looker Studio dashboards** for visualization

---

## Features

### Data Processing
- **Dual Data Sources**: Historical GA4 + Real-time streaming
- **Sessionization**: 30-minute timeout, cross-source session stitching
- **Identity Resolution**: Unified user tracking across sessions
- **Incremental Materialization**: Only processes new data

### Attribution Models
- **First-Click**: Credits the first touchpoint in user journey (14-day window)
- **Last-Click**: Credits last non-direct touchpoint (Direct fallback)

### Channel Classification
- Paid Search (CPC, PPC)
- Organic Search
- Social (Facebook, Twitter, LinkedIn, Instagram)
- Email
- Referral
- Direct
- Other

---

<!-- ## Architecture -->

<!-- ### High-Level Architecture -->

## 📁 Project Structure

```
click-attribution-pipeline/
├── models/
│   ├── staging/
│   │   ├── stg_ga_events.sql
│   │   ├── stg_streamed_events.sql
│   │   └── schema.yml
│   ├── intermediate/
│   │   ├── int_sessions.sql
│   │   └── schema.yml
│   ├── marts/
│   │   ├── mart_attribution_first.sql
│   │   ├── mart_attribution_last.sql
│   │   └── schema.yml
│   └── sources.yml
├── macros/
│   └── get_channel.sql
├── tests/
├── stream_events.py
├── requirements.txt
├── dbt_project.yml
├── profiles.yml
└── README.md
```

---

## 🔧 Prerequisites

### Required Software
- **Python 3.8+** ([Download](https://www.python.org/downloads/))
- **dbt-bigquery 1.5+** (installed via pip)
- **Google Cloud BigQuery** (installed via pip)

### Google Cloud Platform
- **GCP Project**
- **BigQuery API** enabled
- **Permissions:**
  - `BigQuery Data Editor` (to create tables)
  - `BigQuery Job User` (to run queries)
  - `BigQuery Data Viewer` (to read public dataset)

---

## 📦 Installation & Setup

### Step 1: Clone Repository

```bash
git clone https://github.com/yasarsultan/click-attribution-pipeline.git
cd attribution_pipeline
```

### Step 2: Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Step 3: Setup Google Cloud credentials and Environment variables
 - Create service account with appropriate permissions and download the JSON key file
 - Place key in the project directory and rename it to `service-account-file.json`
 - Put environment variables in `.env` file like `PROJECT_ID` and `DATASET_ID`

### Step 4: Configure dbt Profile

Create/edit `~/.dbt/profiles.yml`:

```yaml
attribution_pipeline:
  outputs:
    dev:
      dataset: attribution_dataset
      job_execution_timeout_seconds: 300
      job_retries: 1
      keyfile: ./service-account-file.json
      location: US
      method: service-account
      priority: interactive
      project: Google-Cloud-Project-ID
      threads: 3
      type: bigquery
  target: dev
```

### Step 5: Test dbt Connection

```bash
cd click-attribution-pipeline
dbt debug
```

**Expected output:**
```
Connection test: [OK connection ok]
```

---


## 🚀 Usage

### Initial Setup (First Time Only)

#### 1. Full Refresh - Load Historical Data

```bash
dbt run --full-refresh
```

**What happens:**
- Creates all tables in BigQuery
- Loads ~1 month of GA4 data (Jan 2021)
- Calculates ~5,000+ conversions
- Runtime: ~2-5 minutes

---

### Real-Time Streaming Demo

#### 2. Stream Sample Events

```bash
python stream_events.py
```

**Output:**
```
Streaming 20 events...
  - Purchases: 3
  - Revenue: $567.89
```

#### 3. Process New Events (Incremental)

```bash
dbt run
```

**What happens:**
- `stg_streamed_events`: Processes ONLY the 20 new events
- `int_sessions`: Creates sessions from last 2 days of data
- `mart_*`: Calculates attribution for new conversions only
- Runtime: ~10-30 seconds
```

---

### Continuous Operation

#### 4. Stream More Events (Demonstrate Incremental)

```bash
python stream_events.py

sleep 30

dbt run
```

**Data accumulates:**
- Run 1: 15 events total
- Run 2: 30 events total (15 + 15)
- Run 3: 45 events total (30 + 15)

**Incremental logic prevents duplicates!**

---

### Complete Demo Workflow

```bash
dbt run --full-refresh

python stream_events.py
sleep 30
dbt run

python stream_events.py
sleep 30
dbt run

python stream_events.py
sleep 30
dbt run
```

---

## Project Summary

**Tech Stack:** dbt + BigQuery + Python + Looker Studio  
**Attribution Models:** First-Click, Last-Click  
**Data Sources:** GA4 Public Dataset + Real-time Streaming  
**Key Feature:** Near real-time incremental processing  
**Use Case:** Marketing attribution and channel performance analysis  