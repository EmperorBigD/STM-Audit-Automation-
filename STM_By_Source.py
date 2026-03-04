"""
STM_By_Source.py
================
Runs the 'by source' self-citation analysis for the STM pipeline.

Input
-----
df_perm  : pandas DataFrame — the "Request Status" sheet (Table 1).
df_auth  : pandas DataFrame — the "Chapter Author" sheet (Table 2).

Output
------
final_df : pandas DataFrame with columns:
    Rights Holder | Non-Self Source Texts | Count | Chapter Number

Architecture
------------
All shared pre-processing (permissions dedup, author cleaning, name-matching)
is delegated to :mod:`stm_core`.  This module contains **only** the
source-specific filtering and grouping logic (PART C, PART D, PART E).

Data Flow Overview
------------------
df_perm  ──► preprocess_permissions()  ──► df_unique      (deduplicated assets)
df_auth  ──► preprocess_authors()      ──► df_auth_clean  (with Match_Key etc.)
df_unique × df_auth_clean  ──► merge  ──► merged_check    (one row per asset × chapter-author pair)
merged_check  ──► is_author_in_source()  ──► Is_Self flag per row
Is_Self == False  ──► df_non_self        (non-self-citations only)
df_non_self  ──► groupby(RH × chapter × source) ──► grouped_df (counts per source)
grouped_df  ──► rename + reorder  ──► final_df
"""

import pandas as pd

# Import shared helpers from the central stm_core module.
# This eliminates ~80 lines of code that used to be duplicated between
# this file and STM_By_Chapter.py.
from STM_Core import (
    is_author_in_source,
    preprocess_authors,
    preprocess_permissions,
)


def run_by_source(df_perm: pd.DataFrame, df_auth: pd.DataFrame) -> pd.DataFrame:
    """
    Run the 'by source' non-self-citation analysis.

    Unlike ``run_by_chapter`` (which aggregates to one row per chapter),
    this function keeps each **unique source text** as a separate row and
    counts how many non-self-citation assets it contributed within each chapter.

    Parameters
    ----------
    df_perm : pd.DataFrame
        Raw DataFrame from the ``"Request Status"`` Excel sheet.  Must contain
        columns: ``Spec``, ``Part``, ``Full Source Information``,
        ``Rights Holder``, ``Permissions Status``.

    df_auth : pd.DataFrame
        Raw DataFrame from the ``"Chapter Author"`` Excel sheet.  Must contain
        columns: ``Part``, ``Authors``, ``Title``.

    Returns
    -------
    pd.DataFrame
        One row per **Rights Holder × Chapter × Source Text** combination.
        Columns:

        ==============================  ===============================================
        Column                          Description
        ==============================  ===============================================
        ``Rights Holder``               Publisher / rights-holder name
        ``Non-Self Source Texts``       Full bibliographic source string
        ``Count``                       Number of non-self-citation assets from source
        ``Chapter Number``              Zero-padded, e.g. ``"Chapter 03"``
        ==============================  ===============================================

    Edge Cases
    ----------
    * All assets in ``df_perm`` are self-citations → returns an empty DataFrame
      (caller should handle gracefully; balance check will show count_sum = 0).
    * Source texts with NaN values → treated as the literal string "nan" by
      pandas groupby (no crash; unusual but surfaced to the user via Excel).
    * Chapters in ``df_unique`` with no matching author entry → ``Is_Self``
      defaults to ``False`` (no author match = not a self-citation = billable).
    """

    # ── PART A: permissions pre-processing ────────────────────────────────────
    # Delegates to stm_core.preprocess_permissions():
    #   • removes duplicate column headers
    #   • filters to "Not Yet Requested" status
    #   • extracts Clean_Spec and builds zero-padded Match_Key
    #
    # Data Flow:
    #   Input : df_perm (raw, may have duplicates)   → pd.DataFrame (many rows)
    #   Output: df_unique (deduplicated by spec+src)  → pd.DataFrame (fewer rows)
    df_unique = preprocess_permissions(df_perm)

    if df_unique.empty:
        return pd.DataFrame(columns=[
            "Rights Holder", "Non-Self Source Texts", "Count", "Chapter Number"
        ])

    # ── PART B: author pre-processing ─────────────────────────────────────────
    # Delegates to stm_core.preprocess_authors():
    #   • builds Sort_Key (int) and Match_Key (str, zero-padded)
    #   • strips noise from Authors → Clean_Authors (for name matching)
    #   • normalises Title → Clean_Title (not used in the source view, but
    #     retained to keep the DataFrame schema consistent with the chapter view)
    #
    # Data Flow:
    #   Input : df_auth (raw)                → pd.DataFrame
    #   Output: df_auth (enriched, same rows) → pd.DataFrame (4 extra columns)
    df_auth_clean = preprocess_authors(df_auth)

    # ── PART C: filter — keep only non-self-citations ─────────────────────────

    # C.1 — Merge each unique asset with its chapter's author metadata.
    # LEFT join ensures that assets without a matching author entry are kept
    # (they will default to Is_Self=False → counted as billable).
    #
    # Data Flow:
    #   Input : df_unique (assets) × df_auth_clean (authors) [Match_Key join]
    #   Output: merged_check — one row per (asset × chapter-author pairing)
    merged_check = pd.merge(df_unique, df_auth_clean, on="Match_Key", how="left")

    # C.2 — Classify each row as a self-citation or not.
    # is_author_in_source() uses LRU-cached compiled regex patterns, so the
    # performance cost of repeated regex compilation is avoided on large inputs.
    #
    # Data Flow:
    #   Input : merged_check (one row per asset with Clean_Authors + source)
    #   Process: .apply(is_author_in_source, axis=1) → bool per row
    #   Output: merged_check["Is_Self"] — bool column
    merged_check["Is_Self"] = merged_check.apply(is_author_in_source, axis=1)

    # C.3 — Retain only rows that are NOT self-citations.
    # These are the billable assets that count toward the STM Gratis limit.
    #
    # Data Flow:
    #   Input : merged_check [Is_Self == True | False rows]
    #   Process: boolean mask ~Is_Self
    #   Output: df_non_self — subset with only non-self-citation rows
    df_non_self = merged_check[~merged_check["Is_Self"]].copy()

    # ── PART D: group by Rights Holder × Chapter × Source Text ───────────────
    # Count how many non-self-citation assets each source contributed within
    # each chapter for each rights-holder.
    #
    # .size() here counts the **number of rows** in each group, which equals
    # the number of unique asset entries (already deduplicated in PART A).
    #
    # Data Flow:
    #   Input : df_non_self [Rights Holder, Match_Key, Full Source Information]
    #   Process: .groupby([...]).size()
    #   Output: grouped_df [Rights Holder, Match_Key, Full Source Info, Count]
    grouped_df = (
        df_non_self
        .groupby(["Rights Holder", "Match_Key", "Full Source Information"])
        .size()
        .reset_index(name="Count")
    )

    # ── PART E: cleanup & format ──────────────────────────────────────────────

    # E.1 — Rename columns to the canonical output schema.
    final_df = grouped_df.rename(columns={
        "Match_Key"              : "Chapter Number",
        "Full Source Information": "Non-Self Source Texts",
    })

    # E.2 — Select and order the output columns explicitly.
    final_df = final_df[["Rights Holder", "Non-Self Source Texts", "Count", "Chapter Number"]]

    # E.3 — Sort: primary by Rights Holder (alphabetical), secondary by chapter
    # number (integer to avoid lexicographic mis-sorting of "Chapter 10").
    final_df["_sort"] = (
        final_df["Chapter Number"].str.extract(r"(\d+)")[0].astype(int)
    )
    final_df = (
        final_df
        .sort_values(["Rights Holder", "_sort"])
        .drop(columns=["_sort"])
        .reset_index(drop=True)
    )

    return final_df