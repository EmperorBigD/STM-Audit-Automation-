"""
STM_By_Chapter.py
=================
Runs the 'by chapter' self-citation analysis for the STM pipeline.

Input
-----
df_perm  : pandas DataFrame — the "Request Status" sheet (Table 1).
df_auth  : pandas DataFrame — the "Chapter Author" sheet (Table 2).

Output
------
final_df : pandas DataFrame with columns:
    Rights Holder | Chapter Number | Chapter Author Name | Chapter Title
    | Total Rights holder Assets | Chapter Author is Source Author

Architecture
------------
All shared pre-processing (permissions dedup, author cleaning, name-matching)
is delegated to :mod:`stm_core`.  This module contains **only** the
chapter-specific aggregation logic (PART C and PART D).

Data Flow Overview
------------------
df_perm  ──► preprocess_permissions()  ──► df_unique     (deduplicated assets)
df_auth  ──► preprocess_authors()      ──► df_auth_clean (with Match_Key etc.)
df_unique × df_auth_clean  ──► merge  ──► merged_check   (one row per asset × chapter-author pair)
merged_check  ──► is_author_in_source()  ──► Is_Self flag per row
Is_Self == True  ──► self_counts        (self-citation tally per RH × chapter)
Is_Self == False ──► total_counts       (all assets, via groupby on df_unique)
total_counts + self_counts  ──► stats   (joined)
stats × df_auth_clean  ──► final_df    (with author names and titles)
"""

import pandas as pd

# Import shared helpers from the central stm_core module.
# This eliminates ~80 lines of code that used to be duplicated between
# this file and STM_By_Source.py.
from STM_Core import (
    is_author_in_source,
    preprocess_authors,
    preprocess_permissions,
)


def run_by_chapter(df_perm: pd.DataFrame, df_auth: pd.DataFrame) -> pd.DataFrame:
    """
    Run the 'by chapter' self-citation analysis.

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
        One row per **Rights Holder × Chapter** combination.  Columns:

        ==========================================  =================================
        Column                                      Description
        ==========================================  =================================
        ``Rights Holder``                           Publisher / rights-holder name
        ``Chapter Number``                          Zero-padded, e.g. ``"Chapter 03"``
        ``Chapter Author Name``                     Raw author string from source data
        ``Chapter Title``                           Title-cased chapter title
        ``Total Rights holder Assets``              Count of unique assets (after dedup)
        ``Chapter Author is Source Author``         Count of self-citations detected
        ==========================================  =================================

    Edge Cases
    ----------
    * Empty ``df_perm`` or ``df_auth`` → returns an empty DataFrame with the
      expected columns schema so the caller can handle it gracefully.
    * Chapters in ``df_unique`` with no matching entry in ``df_auth`` → merged
      with ``how="left"``; author fields are NaN (shown as blank in Excel).
    * Self-citation count defaults to 0 for chapters with no self-citations
      (left-join fills NaN → ``fillna(0).astype(int)``).
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
        # Return empty frame with the correct column schema so callers can
        # safely check `len(result) == 0` without KeyError on column names.
        return pd.DataFrame(columns=[
            "Rights Holder", "Chapter Number", "Chapter Author Name",
            "Chapter Title", "Total Rights holder Assets",
            "Chapter Author is Source Author",
        ])

    # ── PART B: author pre-processing ─────────────────────────────────────────
    # Delegates to stm_core.preprocess_authors():
    #   • builds Sort_Key (int) and Match_Key (str, zero-padded)
    #   • strips noise from Authors → Clean_Authors (for name matching)
    #   • normalises Title → Clean_Title (for display)
    #
    # Data Flow:
    #   Input : df_auth (raw)                → pd.DataFrame
    #   Output: df_auth (enriched, same rows) → pd.DataFrame (4 extra columns)
    df_auth_clean = preprocess_authors(df_auth)

    # ── PART C: self-citation detection ───────────────────────────────────────

    # C.1 — Total assets per (Rights Holder × Chapter):
    # Group the deduplicated permissions DataFrame by RH + Match_Key and count
    # how many unique assets each chapter has from each publisher.
    #
    # Data Flow:
    #   Input : df_unique [Rights Holder, Match_Key, ...]  → pd.DataFrame
    #   Process: .groupby([...]).size()                    → pd.Series (counts)
    #   Output: total_counts [Rights Holder, Match_Key, Total Assets] → pd.DataFrame
    total_counts = (
        df_unique
        .groupby(["Rights Holder", "Match_Key"])
        .size()
        .reset_index(name="Total Assets")
    )

    # C.2 — Detect self-citations by merging each asset row with its chapter's
    # author list, then applying the 5-pattern name matcher row-by-row.
    #
    # The merge is LEFT so that assets with no matching author entry are kept
    # (they default to Is_Self=False, i.e. counted as billable).
    #
    # Data Flow:
    #   Input : df_unique × df_auth_clean [Match_Key join] → merged_check
    #   Process: .apply(is_author_in_source, axis=1)       → bool Series
    #   Output: merged_check["Is_Self"]                    → bool column
    merged_check = pd.merge(df_unique, df_auth_clean, on="Match_Key", how="left")
    merged_check["Is_Self"] = merged_check.apply(is_author_in_source, axis=1)

    # C.3 — Count self-citations per (Rights Holder × Chapter):
    # Filter to only the self-citation rows, then group-count exactly as above.
    #
    # Data Flow:
    #   Input : merged_check [Is_Self == True rows]       → pd.DataFrame
    #   Process: .groupby([...]).size()                   → pd.Series
    #   Output: self_counts [Rights Holder, Match_Key, Self_Count] → pd.DataFrame
    self_counts = (
        merged_check[merged_check["Is_Self"]]
        .groupby(["Rights Holder", "Match_Key"])
        .size()
        .reset_index(name="Self_Count")
    )

    # ── PART D: build final output table ──────────────────────────────────────

    # D.1 — Join total counts with self-citation counts.
    # LEFT join preserves chapters that have zero self-citations (no row in
    # self_counts); fillna(0) converts those NaN entries to integer 0.
    #
    # Data Flow:
    #   Input : total_counts + self_counts [RH, Match_Key join] → stats
    #   Process: fillna(0).astype(int) on Self_Count            → clean int col
    stats = pd.merge(
        total_counts, self_counts,
        on=["Rights Holder", "Match_Key"],
        how="left",
    )
    stats["Self_Count"] = stats["Self_Count"].fillna(0).astype(int)

    # D.2 — Join with author metadata (author names + chapter titles).
    # We take only the first author-row per chapter (drop_duplicates on
    # Match_Key) because the 'by chapter' view shows one row per chapter.
    #
    # Data Flow:
    #   Input : stats × df_auth_clean [Match_Key join]
    #   Process: left merge; duplicate Match_Keys dropped on the right side
    #   Output: final_df (one row per RH × chapter with full metadata)
    final_df = pd.merge(
        stats,
        df_auth_clean[["Match_Key", "Authors", "Clean_Title"]].drop_duplicates("Match_Key"),
        on="Match_Key",
        how="left",
    )

    # D.3 — Rename columns to the canonical output schema.
    final_df = final_df.rename(columns={
        "Match_Key"   : "Chapter Number",
        "Authors"     : "Chapter Author Name",
        "Clean_Title" : "Chapter Title",
        "Total Assets": "Total Rights holder Assets",
        "Self_Count"  : "Chapter Author is Source Author",
    })

    # D.4 — Select and order the output columns explicitly.
    final_df = final_df[[
        "Rights Holder",
        "Chapter Number",
        "Chapter Author Name",
        "Chapter Title",
        "Total Rights holder Assets",
        "Chapter Author is Source Author",
    ]]

    # D.5 — Sort: primary by Rights Holder (alphabetical), secondary by chapter
    # number (integer, so "Chapter 10" sorts after "Chapter 9" — not before).
    # A temporary integer sort key is extracted and then dropped.
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