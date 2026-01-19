# ClogenAI Academy - Data Pipeline Engineering Training

This repository contains training materials, sample code, and session notes for the ClogenAI Academy Data Pipeline Engineering Training program.

## Repository Structure

```
DataPipelineEngineering/
├── README.md           # This file
├── pyproject.toml      # Python project configuration
├── poetry.lock         # Dependency lock file
├── week1/             # Session 1: Introduction & Fundamentals
│   └── week1.md
├── week2/             # Session 2: Data Ingestion & Storage
│   ├── week2.md
│   ├── Week2_Ingest_Explore_PySpark.ipynb
│   └── Week2_Ingest_Explore_PySpark.py
├── week3/             # Session 3: Data Cleaning & Transformation
│   ├── week3.md
│   └── Week3_Spark_PySpark_Programming.ipynb
├── week4/             # Session 4: PySpark Transformations & Modeling
│   ├── week4.md
│   ├── week4-extra.md          # Additional code samples
│   └── Week4_PySpark_Transformations_Modeling.ipynb
├── week5/             # Session 5: Data Quality & Validation
│   ├── week5.md
│   ├── week5-extra.md          # Additional code samples
│   └── Week5_Data_Quality_Validations.ipynb
├── week6/             # Session 6: Dataflows, Lineage & Orchestration
│   ├── week6.md
│   └── Week6_Dataflows_Lineage_Orchestration_Automation.ipynb
├── week7/             # Session 7: CDC, Idempotency & Incremental Loads
│   ├── Week7_Live_Session.md
│   ├── Week7_Live_Demo_Notebook.py
│   └── diagrams (cdc, watermark, merge idempotency)
├── week8/             # Session 8: Data Warehousing & BI Integration
│   ├── week8_live_session_guide.md
│   ├── week8_live_demo.py
│   ├── week8_dashboard_complete.py
│   └── diagrams (fact vs dimension, query optimization, dashboard workflow)
├── week9/             # Session 9: Data Governance & Security
│   ├── week9_live_session_guide.md
│   ├── week9_live_demo.py
│   └── diagrams (RBAC, encryption, compliance)
├── week10/            # Session 10: AI Features in Data Pipeline Engineering
│   ├── week10_live_session_guide.md
│   ├── week10_live_demo.py
│   └── diagrams (anomaly detection, feature store, LLM pipeline)
├── week11/            # Session 11: Advanced Performance Optimization & Utilities
│   ├── week11_live_session_guide.md
│   ├── week11_live_demo.py
│   └── diagrams (query plans, caching, dbutils)
├── week12-13/         # Capstone Project: End-to-End Data Pipeline Engineering
│   ├── mentor/                 # Grading rubrics and mentor guides
│   ├── capstone_success_guide.md # Student-friendly scoring guide
│   ├── final_certification_checklist.md
│   ├── week12-13_individual_scenarios.md
│   ├── week12-13_kickoff_session_guide.md
│   ├── week12-13_project_specification.md
│   └── week12-13_submission_guidelines.md
├── code/              # Shared code and utilities
├── datasets/          # Training datasets
└── resources/         # Additional learning materials
```

## Training Program Overview

This comprehensive training program covers modern data pipeline engineering using Apache Spark, Delta Lake, and the Medallion Architecture. Students will learn to build scalable, reliable data pipelines from ingestion to analytics-ready datasets.

### Key Technologies Covered
- **Apache Spark & PySpark** - Distributed data processing
- **Delta Lake** - ACID transactions and data versioning
- **Medallion Architecture** - Bronze, Silver, Gold data layers
- **Databricks** - Unified analytics platform
- **Unity Catalog** - Data governance and security
- **ETL/ELT Patterns** - Modern data transformation approaches
- **Data Quality & Validation** - Great Expectations, Delta constraints, monitoring
- **Security & Compliance** - RBAC, encryption, GDPR, audit trails
- **ML & AI Integration** - Anomaly detection, feature engineering, LLMs
- **Performance Optimization** - Broadcast joins, Z-Ordering, AQE, dbutils

### Learning Path
1. **Week 1**: Foundations of data pipeline engineering
2. **Week 2**: Data ingestion strategies and storage patterns
3. **Week 3**: Data cleaning, transformation, and quality management
4. **Week 4**: PySpark transformations, schema evolution, and Gold layer modeling
5. **Week 5**: Data quality dimensions, validation frameworks, and monitoring
6. **Week 6**: Dataflows, lineage tracking, orchestration, and automation
7. **Week 7**: CDC, idempotency, and incremental loads
8. **Week 8**: Data warehousing and BI integration
9. **Week 9**: Data governance, security, and compliance
10. **Week 10**: AI features in data pipeline engineering
11. **Week 11**: Performance optimization and Databricks utilities
12. **Weeks 12-13**: Capstone Project - End-to-End Data Pipeline Engineering

## Session Details

### Week 4: PySpark Transformations & Modeling
- **Focus**: Schema evolution, incremental processing, analytical transformations
- **Key Topics**: MERGE operations, window functions, Gold layer design
- **Deliverables**: Customer LTV analysis, revenue metrics, retention analysis

### Week 5: Data Quality & Validation
- **Focus**: Data quality dimensions, validation frameworks, monitoring
- **Key Topics**: Great Expectations, Delta constraints, quality pipelines
- **Deliverables**: Comprehensive data quality framework, automated validation

### Week 6: Dataflows, Lineage & Orchestration
- **Focus**: Pipeline orchestration, data lineage tracking, workflow automation
- **Key Topics**: Databricks workflows, data lineage, orchestration patterns, automation strategies
- **Deliverables**: End-to-end orchestrated pipelines, lineage documentation, automated workflows

### Week 7: CDC, Idempotency & Incremental Loads
- **Focus**: Change Data Capture, idempotent pipelines, incremental processing
- **Key Topics**: Watermarks, MERGE operations, late-arriving data, deduplication
- **Deliverables**: Incremental CDC pipeline, idempotency testing, cost optimization analysis

### Week 8: Data Warehousing & BI Integration
- **Focus**: Gold-layer modeling, business analytics, dashboard creation
- **Key Topics**: Fact/dimension tables, star schema, SQL analytics, Databricks dashboards
- **Deliverables**: Sales analytics solution, interactive dashboards, business KPIs

### Week 9: Data Governance & Security
- **Focus**: Data security, privacy, compliance, and governance
- **Key Topics**: RBAC, data masking, encryption, GDPR compliance, Unity Catalog, audit trails
- **Deliverables**: Secure e-commerce platform, GDPR compliance implementation, comprehensive audit system

### Week 10: AI Features in Data Pipeline Engineering
- **Focus**: ML/AI integration in data pipelines
- **Key Topics**: Anomaly detection, feature engineering, LLM integration, MLflow, model deployment
- **Deliverables**: Fraud detection system, churn prediction model, AI-powered product classification

### Week 11: Advanced Performance Optimization & Utilities
- **Focus**: Making pipelines faster, cheaper, and production-ready
- **Key Topics**: Query plans (EXPLAIN), Broadcast Joins, Z-Ordering, Databricks Utilities (dbutils)
- **Deliverables**: Optimized ShopFast analytics pipeline, parameterized orchestration notebooks

### Weeks 12-13: Capstone Project
- **Focus**: Engineering Discipline & Lead Time to Value (SDLC Mastery)
- **Key Topics**: Medallion Architecture, CDC logic, Data Quality (GX), AI Anomaly Detection, PII Masking, Performance Tuning, Version Control (Git), Documentation.
- **Deliverables**: End-to-end production-grade data platform, GitHub repository with iterative history, Comprehensive technical report.
- **Scoring**: 500 Score (25% of total course score) based on 5 SDLC disciplines.



## Getting Started

1. Clone this repository
2. Review the weekly session notes in order
3. Practice with the provided code examples
4. Complete hands-on exercises using the sample datasets
5. Use the tech guides for comprehensive implementation reference

## Prerequisites
- Basic Python programming knowledge
- Understanding of SQL fundamentals
- Access to Databricks workspace (provided during training)

## Support
For questions or support, contact the ClogenAI Academy training team.