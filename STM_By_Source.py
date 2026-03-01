import re
# ==========================================
# PART A: LOAD & PROCESS PERMISSIONS
# ==========================================
df_perm = xl("Table1[#All]", headers=True) # need to Format in Table.
df_perm = df_perm.loc[:, ~df_perm.columns.duplicated()]
df_perm = df_perm[df_perm['Permissions Status'] == 'Not Yet Requested'].copy()

# Clean Spec & Deduplicate (To ensure we count unique Assets, not just rows)
df_perm['Clean_Spec'] = df_perm['Spec'].astype(str).str.extract(r'((?:Figure|Table)\s\d+\.\d+)')[0]
df_unique = df_perm.drop_duplicates(subset=['Clean_Spec', 'Full Source Information'])

# Create Match Key (Chapter 01)
df_unique['Match_Key'] = df_unique['Part'].astype(str).str.extract(r'(\d+)')[0].astype(int).astype(str).str.zfill(2)
df_unique['Match_Key'] = "Chapter " + df_unique['Match_Key']

# ==========================================
# PART B: LOAD AUTHORS
# ==========================================
df_auth = xl("Table2[#All]", headers=True)  # need to Format in Table.
df_auth['Sort_Key'] = df_auth['Title'].astype(str).str.extract(r'Chapter\s*(\d+)')[0].fillna(0).astype(int)
df_auth['Match_Key'] = "Chapter " + df_auth['Sort_Key'].astype(str).str.zfill(2)

# Clean Names
df_auth['Clean_Authors'] = df_auth['Authors'].astype(str).str.replace(r'[\d\*]', '', regex=True).str.replace(r'\s+,', ',', regex=True).str.strip()

# ==========================================
# PART C: FILTER FOR NON-SELF CITATIONS
# ==========================================
merged_check = pd.merge(df_unique, df_auth, on='Match_Key', how='left')

def is_author_in_source_robust(row):
    source = str(row['Full Source Information']).lower()
    authors_raw = str(row['Clean_Authors'])
    author_list = [a.strip() for a in authors_raw.split(',')]
    
    for full_name in author_list:
        if not full_name: continue
        tokens = full_name.split()
        if len(tokens) < 2: 
            if len(full_name) > 3 and full_name.lower() in source: return True
            continue
            
        first_name = tokens[0].lower()
        last_name = tokens[-1].lower()
        first_init = first_name[0]
        last_init = last_name[0]
        
        # 5 Patterns
        p1 = rf"\b{re.escape(last_name)},?\s+{first_init}\b"
        p2 = rf"\b{first_init}\.?\s+{re.escape(last_name)}\b"
        p3 = rf"\b{re.escape(first_name)}\s+{re.escape(last_name)}\b"
        p4 = rf"\b{first_init}\.\s*{last_init}\.?\b"
        p5 = rf"\b{re.escape(first_name)}\s+{last_init}\.?\b"

        if (re.search(p1, source) or re.search(p2, source) or re.search(p3, source) or re.search(p4, source) or re.search(p5, source)):
            return True
    return False

# Apply Logic
merged_check['Is_Self'] = merged_check.apply(is_author_in_source_robust, axis=1)

# FILTER: Keep ONLY Non-Self Citations
df_non_self = merged_check[~merged_check['Is_Self']].copy()


# ==========================================
# PART D: GROUP INDIVIDUALLY (The Change)
# ==========================================
# We group by THREE things: Rights Holder + Chapter + Source Text
# This creates a unique row for every specific source in every specific chapter.
grouped_df = df_non_self.groupby(['Rights Holder', 'Match_Key', 'Full Source Information']).size().reset_index(name='Count')

# ==========================================
# PART E: CLEANUP & FORMAT
# ==========================================
final_df = grouped_df.rename(columns={
    'Match_Key': 'Chapter Number',
    'Full Source Information': 'Non-Self Source Texts'
})

# Reorder Columns
final_df = final_df[[
    'Rights Holder', 
    'Non-Self Source Texts', 
    'Count', 
    'Chapter Number'
]]

# Sort
final_df['Sort_Num'] = final_df['Chapter Number'].str.extract(r'(\d+)')[0].astype(int)
final_df = final_df.sort_values(['Rights Holder', 'Sort_Num']).drop(columns=['Sort_Num'])

final_df