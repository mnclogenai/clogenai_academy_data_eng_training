# Capstone Project: Final Submission & Documentation Guide

This guide ensures your final project is submitted in a professional format and meets all technical and business requirements, aligning with the SDLC engineering disciplines.

---

## 📦 Submission Portfolio

Your project is evaluated as a **live engineering portfolio**. You must submit the following two links for assessment:

1.  **GitHub Repository URL**: Contains all code, orchestration notebooks (exported as source), and your Technical README. This is used to evaluate your **Version Control (FP_VC)** and **Clean Code (FP_IMPL)**.
2.  **Databricks Pipeline/Notebook URL**: A direct link to your working environment on Databricks. This is used for the **Final Technical Demo** and to evaluate your **Testing & Automation (FP_AUTO)**.

---

## 📝 The Technical Report: SDLC Alignment

Your report should focus on the "How" and "Why" behind your engineering choices, structured by the following scoring areas:

### 1. Requirements & Design (100 Score)
- **Problem Statement:** Clear definition of the business problem.
- **Architecture Diagram:** Visual flow of the Medallion pipeline (Bronze → Silver → Gold).
- **Design Decisions:** Explanation of কেন (why) you chose specific data models or orchestration patterns.

### 2. Implementation Quality (150 Score)
- **Pipeline Components:** Briefly describe your CDC logic, AI Integration (Anomaly Detection), and Performance Tuning.
- **Code Standards:** Link to specific notebooks demonstrating modularity and error handling.

### 3. Version Control & Collaboration (75 Score)
- **Git Repository Link:** Link to your GitHub repo.
- **Iteration History:** Your score depends on showing granular, descriptive commits throughout the project lifecycle.

### 4. Testing & Automation (100 Score)
- **Validation Framework:** Description of data quality checks (e.g., GX, Delta Constraints).
- **Reliability:** How you handled schema changes or invalid data (Quarantine/Rejection).
- **Automation:** Evidence of a repeatable, automated refresh process (Master Notebook/Job).

### 5. Documentation & Delivery (75 Score)
- **Setup & Run Instructions:** Steps to reproduce your work instantly.
- **Final Result Summary:** A concise overview of the business value delivered (e.g., Gold layer insights).

---

## 🎯 Presentation & Demo
Prepare a 10-minute technical walkthrough covering:
- Your **Master Notebook** execution.
- A deep dive into your **most complex transformation** or AI logic.
- Evidence of **testing results** and validated data in the Gold layer.

---

## ✅ Final Pre-Submission Checklist
- [ ] **Git History:** I have at least daily commits with descriptive messages.
- [ ] **Secrets:** No passwords, keys, or personal tokens are hardcoded.
- [ ] **Orchestration:** The Master notebook runs exactly as documented without manual intervention.
- [ ] **Quality:** Validation checks are functional and visible in the final output.
- [ ] **README:** Setup instructions have been verified and are easy to follow.

---
*Clogenai Academy – Data Pipeline Engineering Track*
