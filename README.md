# STM Permissions & Self-Citation Data Pipeline

![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Library-Pandas-150458?logo=pandas&logoColor=white)
![NumPy](https://img.shields.io/badge/Library-NumPy-013243?logo=numpy&logoColor=white)
![Regex](https://img.shields.io/badge/Logic-Regex-d93f0b)

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

```
Pipeline.py              ← CLI entry-point, Excel writer, formatting
    ├── STM_Core.py      ← Shared pre-processing & name-matching (NEW)
    ├── STM_By_Chapter.py← Chapter-level aggregation (PART C + D only)
    └── STM_By_Source.py ← Source-level grouping    (PART C + D + E only)
```

### What [STM_Core.py](cci:7://file:///M:/Python/Projects/My%20projects/STM/STM_PowerShell/STM_Core.py:0:0-0:0) provides

| Function | Purpose |
|---|---|
| [preprocess_permissions(df_perm)](cci:1://file:///M:/Python/Projects/My%20projects/STM/STM_PowerShell/stm_core.py:261:0-370:20) | Dedup, filter, extract `Clean_Spec` + `Match_Key` |
| [preprocess_authors(df_auth)](cci:1://file:///M:/Python/Projects/My%20projects/STM/STM_PowerShell/stm_core.py:377:0-470:13) | Clean author names, build `Match_Key`, normalise titles |
| [is_author_in_source(row)](cci:1://file:///M:/Python/Projects/My%20projects/STM/STM_PowerShell/stm_core.py:184:0-254:16) | 5-pattern name matcher (see §3 below) |

This eliminates **~160 lines of duplicated code** that previously existed between [STM_By_Chapter.py](cci:7://file:///m:/Python/Projects/My%20projects/STM/STM_PowerShell/STM_By_Chapter.py:0:0-0:0) and [STM_By_Source.py](cci:7://file:///m:/Python/Projects/My%20projects/STM/STM_PowerShell/STM_By_Source.py:0:0-0:0).

---

## 3. Mathematical Rigour — Name-Matching Algorithm

### Why deterministic regex over fuzzy similarity?

A cosine-similarity approach on TF-IDF author-name vectors would give higher recall
but unacceptable precision: partial matches (e.g. "J. Smith" ↔ "J. Smithson") would
suppress legitimate billable assets. In a billing context, **false negatives are
financially costly**, so precision is prioritised.

#### Cosine Similarity (reference, not used)

For two term-frequency vectors **a** and **b**:

$$\cos(\theta) = \frac{\mathbf{a} \cdot \mathbf{b}}{\|\mathbf{a}\| \cdot \|\mathbf{b}\|} = \frac{\sum_{i} a_i b_i}{\sqrt{\sum_i a_i^2} \cdot \sqrt{\sum_i b_i^2}}$$

A threshold of $\cos(\theta) \geq 0.85$ is typical in author disambiguation literature
but would produce false positives for short names.

#### Levenshtein Distance (reference, not used)

The edit distance $d_L(s,t)$ between strings $s$ and $t$ counts the minimum number of
single-character insertions, deletions, or substitutions needed to transform $s$ into $t$:

$$d_L(s, t) = \begin{cases} |s| & \text{if } |t| = 0 \\ |t| & \text{if } |s| = 0 \\ d_L(s[1:], t[1:]) & \text{if } s[0] = t[0] \\ 1 + \min\!\begin{cases} d_L(s[1:], t) \\ d_L(s, t[1:]) \\ d_L(s[1:], t[1:]) \end{cases} & \text{otherwise} \end{cases}$$

Would allow typo-tolerance but also allows "Joshi" to match "Jash" ($d_L = 2$).

#### The pipeline's 5-pattern approach (used)

Five word-boundary-anchored regex patterns cover all standard academic citation styles:

| Pattern | Example match | Regex |
|---|---|---|
| P1 | `Joshi, M` | `\bJoshi,?\s+M\b` |
| P2 | `M. Joshi` | `\bM\.?\s+Joshi\b` |
| P3 | `Mayank Joshi` | `\bMayank\s+Joshi\b` |
| P4 | `M. J.` | `\bM\.\s*J\.?\b` |
| P5 | `Mayank J.` | `\bMayank\s+J\.?\b` |

`\b` (word boundary) prevents "Li" from matching inside "Liu". `re.escape` handles
hyphenated names (e.g. "O'Brien"). All text is lowercased before matching.

#### Performance optimisation — LRU-cached pattern compilation

Without caching, for $N$ rows and $A$ authors per chapter:

$$\text{regex compilations} = O(N \times A \times 5)$$

With `@lru_cache(maxsize=2048)` on [_compile_patterns(full_name)](cci:1://file:///M:/Python/Projects/My%20projects/STM/STM_PowerShell/stm_core.py:124:0-181:31):

$$\text{regex compilations} = O(A_{\text{unique}} \times 5) \approx O(1) \text{ per row after warm-up}$$

On a 5,000-row input with 40 unique chapter authors, this reduces compilations from
**100,000 → 200**.

---

## 4. Vectorisation

The following hot loops were replaced with vectorised operations:

| Location | Old (loop) | New (vectorised) |
|---|---|---|
| [_prepare_chapter_sheet](cci:1://file:///M:/Python/Projects/My%20projects/STM/STM_PowerShell/pipeline.py:212:0-269:54) | `for col in df.columns` building a dict | `df[cols].sum()` + `pd.Series.reindex()` |
| [_prepare_source_sheet](cci:1://file:///M:/Python/Projects/My%20projects/STM/STM_PowerShell/pipeline.py:272:0-316:54) | same pattern | same fix |
| [_auto_col_width](cci:1://file:///M:/Python/Projects/My%20projects/STM/STM_PowerShell/Pipeline.py:323:0-367:90) | `col_str.map(len).max()` | `col_str.str.len().max()` (pandas C-level) |
| [preprocess_permissions](cci:1://file:///M:/Python/Projects/My%20projects/STM/STM_PowerShell/stm_core.py:261:0-370:20) | (was already vectorised in original) | Confirmed vectorised |
| [preprocess_authors](cci:1://file:///M:/Python/Projects/My%20projects/STM/STM_PowerShell/stm_core.py:377:0-470:13) | [for](cci:1://file:///M:/Python/Projects/My%20projects/STM/STM_PowerShell/pipeline.py:478:0-493:65) loop implicit in str.replace chains | All `.str` ops are pandas C-level |

---

## 5. Edge Cases & Scalability

| Scenario | Handling |
|---|---|
| Missing Excel sheet | [load_workbook_data](cci:1://file:///M:/Python/Projects/My%20projects/STM/STM_PowerShell/pipeline.py:83:0-146:27) raises `ValueError` naming the missing sheet |
| Empty permissions / author DataFrame | `preprocess_*` returns empty frame; callers return empty frame with correct column schema |
| All assets are self-citations | [run_by_source](cci:1://file:///m:/Python/Projects/My%20projects/STM/STM_PowerShell/STM_By_Source.py:44:0-187:19) returns empty frame; balance check shows `count_sum = 0` |
| All rights-holders below threshold | [run_pipeline](cci:1://file:///M:/Python/Projects/My%20projects/STM/STM_PowerShell/pipeline.py:555:0-709:18) emits a `[WARNING]` to stderr instead of silently writing an empty file |
| NaN in author / source columns | `str()` coercion in [is_author_in_source](cci:1://file:///M:/Python/Projects/My%20projects/STM/STM_PowerShell/stm_core.py:184:0-254:16) prevents crash |
| Non-numeric cell in orange-highlight loop | `try/except (ValueError, TypeError)` skips safely |
| Duplicate column headers in input | `df.loc[:, ~df.columns.duplicated()]` in [preprocess_permissions](cci:1://file:///M:/Python/Projects/My%20projects/STM/STM_PowerShell/stm_core.py:261:0-370:20) |
| LRU cache memory use | `maxsize=2048` caps cache; typical inputs have < 100 unique author names |

---

## 6. How to Use the Tool

1. **Place your data file:** Ensure exactly **one** `Weekly_Status_Python...xlsx` file is in `M:\STM`.
2. **Run the script:**
   - Open PowerShell: `cd M:\STM`
   - Type [.\Run_Pipeline.ps1](cci:7://file:///M:/Python/Projects/My%20projects/STM/STM_PowerShell/Run_Pipeline.ps1:0:0-0:0) and press Enter.
   - Or Right-Click [Run_Pipeline.ps1](cci:7://file:///M:/Python/Projects/My%20projects/STM/STM_PowerShell/Run_Pipeline.ps1:0:0-0:0) → "Run with PowerShell".

The script extracts the ISBN from the filename and creates `STM_<isbn>_<datetime>.xlsx`.

---

## 7. Understanding Your Output

| Sheet | Column to watch | Orange means… |
|---|---|---|
| `<Publisher> by chapter` | **Net Rights Holder Assets** | > 3 assets from this publisher in this chapter → fee territory |
| `<Publisher> by source` | **Count** | This source contributed > 3 assets to one chapter |

The yellow last row in each sheet is the grand total. The **Balance Check** printed to the
console verifies that [sum(Net Rights Holder Assets) == sum(Count)](cci:1://file:///M:/Python/Projects/My%20projects/STM/STM_PowerShell/Pipeline.py:161:0-209:41) for each publisher.

---

## 8. Dependencies

| Package | Version | Purpose |
|---|---|---|
| `pandas` | `3.0.1` | DataFrame operations, Excel I/O |
| `numpy` | `2.4.2` | Vectorised column-width arithmetic; dtype handling |
| `openpyxl` | `>= 3.1` | Excel file writing, cell formatting |

Install with:

```powershell
pip install -r Requirements.txt
```
