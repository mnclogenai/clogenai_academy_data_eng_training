# Capstone Project Specification: Data Pipeline Engineering (SDLC-Aligned)

## 📋 Project Objective
The Capstone Project evaluates your ability to build a production-grade, end-to-end data platform using the Medallion Architecture on Databricks. Evaluation is based on **Engineering Discipline** across the full Software Development Life Cycle (SDLC).

---

## 🏗️ 1. Requirements & Design (100 Score - FP_RD)
*Objective: Demonstrate clear planning, architectural design, and technical justification.*

- **Problem Definition:** Define the scope and business requirements for the ShopFast Enterprise Data Platform.
- **Architecture Design:** Implement a 3-layer Medallion Architecture (Bronze, Silver, Gold).
- **Data Models:** Define clear schemas and API contracts for your Delta tables.
- **Trade-offs:** Document why you chose specific ingestion methods, storage patterns (Delta), or orchestration strategies.

---

## 💻 2. Implementation Quality (150 Score - FP_IMPL)
*Objective: Build robust, modular, and maintainable pipelines.*

- **Layer 1: Ingestion (Bronze):**
    - Streaming simulation for `orders.json`.
    - Batch load for `customers.csv` and `products.csv`.
    - Metadata capture (timestamps, source) and schema enforcement.
- **Layer 2: Curated Data (Silver):**
    - **CDC Implementation:** Execute `MERGE` logic for updates/deletes in Customers and Products.
    - **Data Cleaning:** Handle nulls, standardize formats, and correct currency types.
    - **PII Security:** Mask `email` and `phone` columns in Silver/Gold for non-admin roles.
- **Layer 3: Business Ready (Gold):**
    - Enriched `fact_orders` and `dim_customers_summary` (LTV, frequency).
    - Aggregated `sales_by_category_daily`.
- **AI Integration:** 
    - Implement **Anomaly Detection** scores for fraud flagging.
    - Use AI functions for product description classification.

---

## ⚙️ 3. Version Control & Collaboration (75 Score - FP_VC)
*Objective: Evidence of professional development practices through Git.*

- **Iterative Commits:** Small, descriptive, and frequent commits (Daily cadence required).
- **Git Workflow:** Proper branching and descriptive messages (e.g., `feat: silver layer cdc logic`).
- **History:** Professional repository reflecting the evolution from initial design to final delivery.

---

## 🧪 4. Testing & Automation (100 Score - FP_AUTO)
*Objective: Ensure reliability through validation and automated orchestration.*

- **Data Quality Framework:** Integrate **Great Expectations** or **Delta Constraints**.
- **Quarantine Logic:** Automatically handle failed records without breaking the pipeline.
- **Orchestration:** A single **Master Notebook** parameterised with **Widgets** (Catalog, Environment).
- **Performance Optimization:** 
    - **Z-Ordering** by `order_date` for data skipping.
    - **Broadcast Joins** for small reference tables.
    - Automated maintenance (`OPTIMIZE` / `VACUUM`).

---

## 📝 5. Documentation & Delivery (75 Score - FP_DOC)
*Objective: Professional presentation and reproduction readiness.*

- **Technical README:** Clear setup guide, design decisions, and SDLC walkthrough.
- **Implementation & Delivery Guide:** Summary of technical hurdles, trade-offs, and final results.
- **Final Demo:** A project that is ready to run and walk through in a live technical review.

---

## 📊 Submission Deliverables
Evaluation is based on your **Live Engineering Portfolio**. You must provide:

1.  **GitHub Repository Link**: Containing all modular notebooks, orchestration scripts, and the Technical README (Technical Report).
2.  **Databricks Pipeline Link**: A workspace link to your working Master Notebook and pipeline for the live technical demo.

---

## 📈 Evaluation Summary (SDLC-Based)
| Area | ID | Max Score | Key Deliverable |
|---|---|---|---|
| Requirements & Design | FP_RD | 100 | Architecture Diagram & Technical Report |
| Implementation Quality | FP_IMPL | 150 | Modular Pipelines & AI Integration |
| Version Control | FP_VC | 75 | Commit History in GitHub Repository |
| Testing & Automation | FP_AUTO | 100 | Validation Framework & Orchestrator |
| Documentation | FP_DOC | 75 | Setup Instructions & Project Summary |

---
*Clogenai Academy – Data Pipeline Engineering Track*
