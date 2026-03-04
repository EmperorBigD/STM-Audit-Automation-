# STM Permissions & Self-Citation Data Pipeline

This folder contains an automated pipeline that analyses "Weekly Status" Excel reports and
determines which assets (figures, tables, images) require formal permission requests according
to the **STM Permissions Guidelines**.

---

## 1. The Problem We Are Solving

When an academic book is published, authors often reuse figures or tables from other works.
Two rules govern whether fees are owed:

1. **Self-Citation is Free:** If the chapter author is also the source author, no permission fee is required.
2. **The "Rule of 3" (Gratis Limit):** Up to 3 non-self-citation assets per rights-holder per chapter are free. A 4th triggers a fee.

**The Goal:** Identify self-citations, deduct them, and highlight where the "Rule of 3" is breached — automatically.

---

## 2. Architecture

The pipeline is split into four modules. Shared logic lives in one place; each analysis module contains only its unique logic.

```text
Pipeline.py              ← CLI entry-point, Excel writer, formatting
    ├── STM_Core.py      ← Shared pre-processing & name-matching (NEW)
    ├── STM_By_Chapter.py← Chapter-level aggregation (PART C + D only)
    └── STM_By_Source.py ← Source-level grouping    (PART C + D + E only)
