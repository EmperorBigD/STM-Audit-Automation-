"""
STM_Core.py
===========
Central shared library for the STM self-citation analysis pipeline.

Motivation
----------
Both STM_By_Chapter.py and STM_By_Source.py previously contained
**identical** copies of three major code blocks:

  1. ``_is_author_in_source()``   — 5-pattern name-matching function
  2. PART A                       — permissions DataFrame pre-processing
  3. PART B                       — author DataFrame pre-processing

Duplicating these blocks creates a maintenance hazard (bugs fixed in one
copy silently persist in the other) and wastes memory by constructing the
same intermediate DataFrames twice per pipeline run.

This module is the single source of truth for all shared logic.

Architecture
------------
.. code-block:: text

   Pipeline.py
       │
       ├── STM_By_Chapter.run_by_chapter(df_perm, df_auth)
       │       └── STM_Core.preprocess_permissions(df_perm)
       │       └── STM_Core.preprocess_authors(df_auth)
       │       └── STM_Core.is_author_in_source(row)   [via .apply()]
       │
       └── STM_By_Source.run_by_source(df_perm, df_auth)
               └── STM_Core.preprocess_permissions(df_perm)  [same call]
               └── STM_Core.preprocess_authors(df_auth)       [same call]
               └── STM_Core.is_author_in_source(row)          [same call]

Dependencies
------------
  pandas  >= 3.0.1
  numpy   >= 2.4.2   (used for vectorised string ops in helpers)
  re                 (standard library)
"""

import re
from functools import lru_cache
from typing import Optional

import numpy as np
import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# Internal constants
# ─────────────────────────────────────────────────────────────────────────────

# Regex to extract the canonical spec label (e.g. "Figure 3.2", "Table 12.1")
# from raw Spec strings that may contain extra text.
#
# Data Flow:
#   Input  : str  — raw Spec cell, e.g. "Figure 3.2 (adapted)"
#   Process: re.search → group(0) extracts the canonical prefix
#   Output : str | NaN — "Figure 3.2" or NaN if pattern absent
_RE_SPEC = re.compile(r"(?:Figure|Table)\s\d+\.\d+")

# Regex to pull a bare integer chapter number from a "Part" cell value.
#
# Data Flow:
#   Input  : str  — raw Part cell, e.g. "Chapter 03 Intro"
#   Process: re.search → group(1) captures the digit string
#   Output : str | NaN — "03" or NaN
_RE_CHAPTER_NUM_FROM_PART = re.compile(r"(\d+)")

# Regex to pull a chapter number from an author-sheet "Part" cell that
# already contains the word "Chapter": e.g. "Chapter 3".
#
# Data Flow:
#   Input  : str  — author sheet Part cell, e.g. "Chapter 3"
#   Process: re.search → group(1) captures the digit(s) after "Chapter "
#   Output : str | NaN — "3" or NaN
_RE_CHAPTER_NUM_FROM_AUTH = re.compile(r"Chapter\s*(\d+)")

# Removes digit characters and asterisks from author name strings.
# These are footnote markers that appear in raw Excel data (e.g. "Smith, J.*1").
#
# Data Flow:
#   Input  : str  — "Smith, J.*1"
#   Process: re.sub removes [0-9*]
#   Output : str  — "Smith, J."
_RE_AUTHOR_NOISE = re.compile(r"[\d*]")

# Collapses whitespace before a comma that results from noise removal.
#
# Data Flow:
#   Input  : str  — "Smith ,J."      (space before comma)
#   Process: re.sub collapses " ," → ","
#   Output : str  — "Smith,J."
_RE_SPACE_BEFORE_COMMA = re.compile(r"\s+,")


# ─────────────────────────────────────────────────────────────────────────────
# Name-matching helpers
# ─────────────────────────────────────────────────────────────────────────────

# ┌─────────────────────────────────────────────────────────────────────────┐
# │ ALGORITHMIC NOTE — Why 5 deterministic regex patterns instead of         │
# │ fuzzy / cosine similarity?                                               │
# │                                                                          │
# │ A cosine similarity approach on TF-IDF author-name vectors would give   │
# │ high recall but unacceptable precision — "J. Smith" could partially      │
# │ match "J. Smithson" and produce false self-citations, causing legitimate │
# │ billable assets to be suppressed.                                         │
# │                                                                          │
# │ The 5 patterns cover every real-world academic citation style:           │
# │   P1  "Joshi, M"         — last, initial                                 │
# │   P2  "M. Joshi"         — initial. last                                 │
# │   P3  "Mayank Joshi"     — first last (full)                             │
# │   P4  "M. J."            — first-initial. last-initial.                  │
# │   P5  "Mayank J."        — first last-initial.                           │
# │                                                                          │
# │ All comparisons are lowercased and word-boundary anchored (\b) to avoid  │
# │ substring false-positives (e.g. "Li" inside "Liu").                      │
# └─────────────────────────────────────────────────────────────────────────┘


@lru_cache(maxsize=2048)
def _compile_patterns(full_name: str) -> tuple:
    """
    Pre-compile the 5 regex patterns for a single author name string.

    This function is decorated with ``@lru_cache`` so that patterns for a
    given author name are compiled **exactly once** across the entire pipeline
    run, regardless of how many rows reference that author.

    Complexity
    ----------
    Without caching: O(n_rows × n_authors_per_chapter × 5) regex compilations.
    With caching:    O(n_unique_authors × 5) compilations — effectively O(1)
                     per row after the first occurrence.

    Parameters
    ----------
    full_name : str
        A single author name as it appears after cleaning, e.g. "Mayank Joshi".
        Must be a plain string (not bytes) to be hashable for the LRU cache.

    Returns
    -------
    tuple[re.Pattern, ...]
        A 5-tuple of compiled regex patterns, or an empty tuple if the name
        has fewer than two tokens (insufficient for multi-part matching).

    Data Flow
    ---------
    +--------------------------+------------------------------------------+-------------------------+
    | Input                    | Process                                  | Output                  |
    +==========================+==========================================+=========================+
    | str "Mayank Joshi"       | split → tokens = ["Mayank", "Joshi"]    | first="mayank"          |
    |                          |                                          | last="joshi"            |
    |                          |                                          | fi="m", li="j"          |
    +--------------------------+------------------------------------------+-------------------------+
    | first, last, fi, li      | 5 rf-string templates → re.compile      | tuple of 5 re.Pattern   |
    +--------------------------+------------------------------------------+-------------------------+
    """
    tokens = full_name.split()
    if len(tokens) < 2:
        # Single-token name: cannot construct reliable multi-part patterns.
        return ()

    first = tokens[0].lower()
    last  = tokens[-1].lower()
    fi    = first[0]   # first initial
    li    = last[0]    # last initial

    # Word-boundary anchoring (\b) prevents "Li" from matching inside "Liu".
    # re.escape() handles names with hyphens or dots (e.g. "O'Brien").
    p1 = re.compile(rf"\b{re.escape(last)},?\s+{fi}\b")           # "Joshi, M"
    p2 = re.compile(rf"\b{fi}\.?\s+{re.escape(last)}\b")          # "M. Joshi"
    p3 = re.compile(rf"\b{re.escape(first)}\s+{re.escape(last)}\b") # "Mayank Joshi"
    p4 = re.compile(rf"\b{fi}\.\s*{li}\.?\b")                     # "M. J."
    p5 = re.compile(rf"\b{re.escape(first)}\s+{li}\.?\b")         # "Mayank J."

    return (p1, p2, p3, p4, p5)


def is_author_in_source(row: pd.Series) -> bool:
    """
    Determine whether any chapter author of a given row is also cited in
    the row's source text — i.e. whether this asset is a **self-citation**.

    This is the core business-logic function of the STM pipeline.
    A ``True`` return value means the asset is free (no permission needed).
    A ``False`` return value means the asset counts toward the STM Gratis limit.

    Parameters
    ----------
    row : pd.Series
        A single row from the merged DataFrame.  Must have two fields:

        * ``"Full Source Information"``  — the bibliographic text of the source
          asset (e.g. "Mayank Joshi, Introduction to ML, Wiley 2020")
        * ``"Clean_Authors"``            — comma-separated chapter author names
          (digits and asterisks already stripped), e.g. "Mayank Joshi, R. Patel"

    Returns
    -------
    bool
        ``True``  if at least one chapter author name matches any of the five
                  citation-style patterns found in the source text.
        ``False`` otherwise.

    Edge Cases
    ----------
    * NaN / non-string values in either column → coerced to empty string → no match
    * Single-token names (initials only, e.g. "M.") → matched as substring if
      ``len > 3`` to avoid false positives on common single letters
    * Empty author list → returns ``False``

    Data Flow
    ---------
    +---------------------------+--------------------------------------------+------------------+
    | Input                     | Process                                    | Output           |
    +===========================+============================================+==================+
    | row["Full Source ..."]    | str() + .lower()                           | source: str      |
    +---------------------------+--------------------------------------------+------------------+
    | row["Clean_Authors"]      | split(",") → list of name strings          | names: list[str] |
    +---------------------------+--------------------------------------------+------------------+
    | each name                 | _compile_patterns(name) → cached patterns  | patterns: tuple  |
    +---------------------------+--------------------------------------------+------------------+
    | patterns + source         | any(p.search(source) for p in patterns)    | bool             |
    +---------------------------+--------------------------------------------+------------------+
    """
    # Coerce to str to safely handle NaN, float, or None cell values
    source: str     = str(row["Full Source Information"]).lower()
    authors_raw: str = str(row["Clean_Authors"])

    for full_name in (a.strip() for a in authors_raw.split(",")):
        if not full_name:
            continue

        tokens = full_name.split()

        # Single-token fallback: use as literal substring (only if long enough
        # to be meaningful — avoids matching single-letter initials everywhere)
        if len(tokens) < 2:
            if len(full_name) > 3 and full_name.lower() in source:
                return True
            continue

        # Retrieve pre-compiled patterns (cached; zero recompilation cost after
        # the first call for this particular full_name string)
        patterns = _compile_patterns(full_name.lower())
        if any(p.search(source) for p in patterns):
            return True

    return False


# ─────────────────────────────────────────────────────────────────────────────
# Permissions pre-processing  (PART A — shared between both analyses)
# ─────────────────────────────────────────────────────────────────────────────

def preprocess_permissions(df_perm: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and de-duplicate the raw permissions ("Request Status") DataFrame.

    This function encapsulates **PART A** of the original analysis logic that
    was previously duplicated in both ``STM_By_Chapter.py`` and
    ``STM_By_Source.py``.

    Steps
    -----
    1. Remove duplicate column headers (can occur when Excel sheets are
       concatenated from multiple workbooks).
    2. Filter to rows where ``Permissions Status == "Not Yet Requested"``
       (only assets pending permission are relevant to the billing analysis).
    3. Extract a canonical ``Clean_Spec`` label from the raw ``Spec`` column
       (e.g. ``"Figure 3.2 (adapted)"`` → ``"Figure 3.2"``).
    4. De-duplicate on ``(Clean_Spec, Full Source Information)`` so that the
       same asset referenced multiple times by different sub-items counts once.
    5. Build a zero-padded ``Match_Key`` (e.g. ``"Chapter 03"``) keyed on the
       ``Part`` column's embedded chapter number.

    Parameters
    ----------
    df_perm : pd.DataFrame
        Raw DataFrame loaded from the ``"Request Status"`` Excel sheet.
        Expected columns: ``Spec``, ``Part``, ``Full Source Information``,
        ``Rights Holder``, ``Permissions Status``.

    Returns
    -------
    pd.DataFrame
        De-duplicated, filtered DataFrame with two extra columns:
        ``Clean_Spec`` and ``Match_Key``.
        Returns an **empty DataFrame** (with expected columns preserved)
        if no rows survive the filter or the input is empty.

    Raises
    ------
    KeyError
        If any of the required columns are absent from ``df_perm``.  The
        caller (``run_by_chapter`` / ``run_by_source``) should handle this
        with a descriptive error message.

    Data Flow
    ---------
    +-------------------------------+---------------------------------------+---------------------------------+
    | Input                         | Process                               | Output                          |
    +===============================+=======================================+=================================+
    | df_perm (raw, has dupes)      | drop duplicate column headers         | df_perm (unique cols)           |
    +-------------------------------+---------------------------------------+---------------------------------+
    | df_perm["Permissions Status"] | == "Not Yet Requested" boolean mask   | df_filtered (subset of rows)    |
    +-------------------------------+---------------------------------------+---------------------------------+
    | df_filtered["Spec"]           | str.extract(_RE_SPEC pattern)         | df_filtered["Clean_Spec"]       |
    +-------------------------------+---------------------------------------+---------------------------------+
    | Clean_Spec + Source Info      | drop_duplicates(subset=[...])         | df_unique (deduplicated)        |
    +-------------------------------+---------------------------------------+---------------------------------+
    | df_unique["Part"]             | extract digits → zfill(2) → prepend  | df_unique["Match_Key"]          |
    +-------------------------------+---------------------------------------+---------------------------------+
    """
    # ── Step 1: remove duplicate column headers ───────────────────────────────
    # pd.read_excel on concatenated workbooks can produce duplicate col names.
    # Keep only the first occurrence of each column name.
    df = df_perm.loc[:, ~df_perm.columns.duplicated()].copy()

    # Guard: if the DataFrame is empty after dedup, return early
    if df.empty:
        return df

    # ── Step 2: filter to pending permissions only ────────────────────────────
    # We only care about assets that still need permission requests.
    # Other statuses ("Approved", "Rejected", etc.) are out of scope.
    df = df[df["Permissions Status"] == "Not Yet Requested"].copy()

    if df.empty:
        return df

    # ── Step 3: extract canonical spec label ──────────────────────────────────
    # Raw "Spec" cells look like: "Figure 3.2 (adapted from Smith 2019)"
    # We extract only "Figure 3.2" for reliable grouping.
    #
    # Vectorised: str.extract runs a single regex over the entire Series
    # (C-level loop in pandas → faster than Python-level for-loop).
    df["Clean_Spec"] = df["Spec"].astype(str).str.extract(
        r"((?:Figure|Table)\s\d+\.\d+)"
    )[0]

    # ── Step 4: de-duplicate ───────────────────────────────────────────────────
    # A single figure may appear on multiple rows if it was listed under
    # different sub-items or dates. We treat (spec, source) as the unique key.
    df_unique = df.drop_duplicates(
        subset=["Clean_Spec", "Full Source Information"]
    ).copy()

    # ── Step 5: build zero-padded Match_Key ───────────────────────────────────
    # "Part" cells look like "Chapter 3 Section A" or "03" — we extract the
    # first run of digits and zero-pad to width 2 for lexicographic sorting.
    #
    # Vectorised pipeline (no Python for-loop):
    #   Series.astype(str) → Series.str.extract → Series.astype(int) → .str.zfill(2)
    df_unique["Match_Key"] = (
        "Chapter "
        + df_unique["Part"]
        .astype(str)
        .str.extract(r"(\d+)")[0]
        .astype(int)
        .astype(str)
        .str.zfill(2)
    )

    return df_unique


# ─────────────────────────────────────────────────────────────────────────────
# Author pre-processing  (PART B — shared between both analyses)
# ─────────────────────────────────────────────────────────────────────────────

def preprocess_authors(df_auth: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the raw chapter-author ("Chapter Author") DataFrame.

    This function encapsulates **PART B** of the original analysis logic that
    was previously duplicated in both ``STM_By_Chapter.py`` and
    ``STM_By_Source.py``.

    Steps
    -----
    1. Build a numeric ``Sort_Key`` from the ``Part`` column so chapters can
       be sorted as integers, not strings (avoids "Chapter 10" sorting before
       "Chapter 2").
    2. Build a zero-padded ``Match_Key`` (e.g. ``"Chapter 03"``) that links
       to the permissions DataFrame's ``Match_Key`` for a clean merge key.
    3. Clean raw ``Authors`` strings by stripping footnote digits/asterisks
       and collapsing whitespace → ``Clean_Authors`` (used by
       ``is_author_in_source``).
    4. Normalise the chapter ``Title`` to title-case with collapsed whitespace
       → ``Clean_Title`` (used for display only).

    Parameters
    ----------
    df_auth : pd.DataFrame
        Raw DataFrame loaded from the ``"Chapter Author"`` Excel sheet.
        Expected columns: ``Part``, ``Authors``, ``Title``.

    Returns
    -------
    pd.DataFrame
        Input DataFrame with four extra columns added in-place:
        ``Sort_Key``, ``Match_Key``, ``Clean_Authors``, ``Clean_Title``.
        Returns an **empty DataFrame** if the input is empty.

    Data Flow
    ---------
    +--------------------+--------------------------------------------+---------------------------+
    | Input              | Process                                    | Output                    |
    +====================+============================================+===========================+
    | df_auth["Part"]    | str.extract → fillna(0) → .astype(int)    | df_auth["Sort_Key"] (int) |
    +--------------------+--------------------------------------------+---------------------------+
    | Sort_Key           | .astype(str).str.zfill(2) → prepend       | df_auth["Match_Key"] (str)|
    +--------------------+--------------------------------------------+---------------------------+
    | df_auth["Authors"] | re.sub strip digits+* → strip whitespace  | df_auth["Clean_Authors"]  |
    +--------------------+--------------------------------------------+---------------------------+
    | df_auth["Title"]   | str.strip → re.sub collapse spaces → title| df_auth["Clean_Title"]    |
    +--------------------+--------------------------------------------+---------------------------+
    """
    df = df_auth.copy()

    if df.empty:
        return df

    # ── Step 1 & 2: Sort_Key and Match_Key ────────────────────────────────────
    # Author sheet "Part" cells look like: "Chapter 3", "Chapter 12"
    # We extract the digit(s) after the word "Chapter".
    # fillna(0) handles any rows without a recognisable chapter number (e.g.
    # appendices or prefaces) — they sort to position 0 instead of crashing.
    df["Sort_Key"] = (
        df["Part"]
        .astype(str)
        .str.extract(r"Chapter\s*(\d+)")[0]
        .fillna(0)
        .astype(int)
    )

    # Zero-pad to 2 digits for consistent string comparison: "03" not "3".
    # This ensures "Chapter 03" < "Chapter 10" in a lexicographic sort.
    df["Match_Key"] = "Chapter " + df["Sort_Key"].astype(str).str.zfill(2)

    # ── Step 3: Clean_Authors ─────────────────────────────────────────────────
    # Raw: "Mayank Joshi*1, R. Patel2"
    # After step:  "Mayank Joshi, R. Patel"
    #
    # Vectorised: all three .str operations are C-level; no Python for-loop.
    df["Clean_Authors"] = (
        df["Authors"]
        .astype(str)
        .str.replace(_RE_AUTHOR_NOISE, "", regex=True)   # strip digits and *
        .str.replace(_RE_SPACE_BEFORE_COMMA, ",", regex=True)  # "Smith ,J" → "Smith,J"
        .str.strip()
    )

    # ── Step 4: Clean_Title (display only) ───────────────────────────────────
    # Collapses multiple internal spaces and normalises casing.
    df["Clean_Title"] = (
        df["Title"]
        .astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.title()
    )

    return df
