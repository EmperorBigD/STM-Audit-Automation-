# STM-Audit-Automation: Citation & Permissions Intelligence Engine

![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Library-Pandas-orange?logo=pandas&logoColor=white)
![Regex](https://img.shields.io/badge/Logic-Regex-red)

## 📌 Project Overview
In the **Science, Technology, and Medicine (STM)** publishing sector, auditing content usage is a critical compliance hurdle. This project automates the identification of **"Excess STM"** usage by programmatically distinguishing between **Self-Citations** (authors citing their own previous work) and **Third-Party Content** (which requires legal permissions).

By applying **MSc Data Science** methodologies to a manual Project Management workflow, I reduced a **3-hour manual audit** to just **30 minutes**, achieving an **80% efficiency gain** while eliminating human fatigue errors.

## 🚀 Key Features
* **ETL Pipeline:** Automates the extraction, cleaning, and deduplication of bibliographic data directly within Excel environments.
* **Fuzzy Author Matching:** A robust Regex-driven engine that identifies authors across 5 distinct naming conventions to handle citation variance.
* **Rule-Based Classification:** Implemented logic gates to instantly segment "Self" vs. "Non-Self" citations for compliance flagging.
* **Production-Ready Reporting:** Generates structured outputs optimized for both Rights Holders and Editorial Review.

## 🛠️ Technical Deep Dive

### 1. The "Fuzzy Matcher" Logic
The core engine utilizes the `re` library to validate five specific patterns against the source text. This ensures that variations in academic citations do not result in "False Positives" for permissions.

| Pattern | Logic | Example Match |
| :--- | :--- | :--- |
| **P1** | `Surname, First Initial` | "Joshi, M" |
| **P2** | `First Initial. Surname` | "M. Joshi" |
| **P3** | `Full Name` | "Mayank Joshi" |
| **P4** | `Double Initials` | "M. J." |
| **P5** | `First Name + Last Initial` | "Mayank J." |

### 2. Data Flow Architecture
The pipeline follows a rigorous data transformation sequence:



1.  **Ingestion:** Extracts permissions and author metadata from Excel tables.
2.  **Vectorized Cleaning:** Uses `pandas` to extract figure/table tags (e.g., `Figure 1.2`) and remove duplicates.
3.  **Pattern Application:** Executes the `is_author_in_source_robust` function across the merged dataset.
4.  **Aggregation:** Groups results by Rights Holder and Chapter to highlight "Non-Self" sources.

## 📈 Business Impact
* **Time Savings:** Manual audit time slashed from **~180 mins** to **~30 mins** per project.
* **Scalability:** The system handles hundreds of references across multiple chapters in seconds.
* **Accuracy:** Significant reduction in human error related to name inversions and middle initials.

## 📂 File Descriptions
* `STM_By_Source.py`: Focuses on grouping unique sources to identify permission requirements for specific Rights Holders.
* `STM_By_Chapter.py`: Provides an editorial overview, counting self-citations per chapter to ensure content balance.

---
**Developed by:** Mayank Joshi  
*MSc Data Science Candidate | MSc Physics | Project Manager @ Lumina Datamatics*
