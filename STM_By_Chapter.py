import re #df = xl("Table1[#All]", headers=True)
# ==========================================
# PART A: LOAD & PROCESS PERMISSIONS (The Assets)
# ==========================================
df_perm = xl("Table1[#All]", headers=True) # need to Format in Table.
df_perm = df_perm.loc[:, ~df_perm.columns.duplicated()] # Remove dupes

# Filter
df_perm = df_perm[df_perm['Permissions Status'] == 'Not Yet Requested'].copy()

# Clean Spec & Deduplicate
df_perm['Clean_Spec'] = df_perm['Spec'].astype(str).str.extract(r'((?:Figure|Table)\s\d+\.\d+)')[0]
df_unique = df_perm.drop_duplicates(subset=['Clean_Spec', 'Full Source Information'])

# Create Match Key (Chapter 01)
df_unique['Match_Key'] = df_unique['Part'].astype(str).str.extract(r'(\d+)')[0].astype(int).astype(str).str.zfill(2)
df_unique['Match_Key'] = "Chapter " + df_unique['Match_Key']


# ==========================================
# PART B: LOAD & PROCESS AUTHORS (The Metadata)
# ==========================================
df_auth = xl("Table2[#All]", headers=True)  # need to Format in Table.

# Create Match Key
df_auth['Sort_Key'] = df_auth['Title'].astype(str).str.extract(r'Chapter\s*(\d+)')[0].fillna(0).astype(int)
df_auth['Match_Key'] = "Chapter " + df_auth['Sort_Key'].astype(str).str.zfill(2)

# Clean Names (INTERNAL USE ONLY - For the Fuzzy Match Math)
df_auth['Clean_Authors'] = df_auth['Authors'].astype(str).str.replace(r'[\d\*]', '', regex=True).str.replace(r'\s+,', ',', regex=True).str.strip()

# Clean Title (For Display)
df_auth['Clean_Title'] = df_auth['Title'].str.replace(r'^Chapter\s*\d+\.?\s*', '', regex=True)


# ==========================================
# PART C: ROBUST LOGIC (Added Initials Check)
# ==========================================

# 1. Total Assets
total_counts = df_unique.groupby(['Rights Holder', 'Match_Key']).size().reset_index(name='Total Assets')

# 2. Self-Citations (Using Clean_Authors for logic)
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
        
        # --- THE 5 PATTERNS ---
        # 1. "Joshi, M" (Surname, First Initial)
        p1 = rf"\b{re.escape(last_name)},?\s+{first_init}\b"
        
        # 2. "M. Joshi" (First Initial, Surname)
        p2 = rf"\b{first_init}\.?\s+{re.escape(last_name)}\b"
        
        # 3. "Mayank Joshi" (Full Name)
        p3 = rf"\b{re.escape(first_name)}\s+{re.escape(last_name)}\b"
        
        # 4. "M. J." or "J. M." (Both Initials)
        # We REQUIRE a dot after the first initial to avoid matching words like "is" or "at"
        p4 = rf"\b{first_init}\.\s*{last_init}\.?\b"
        
        # 5. "Mayank J." (First Name, Last Initial)
        p5 = rf"\b{re.escape(first_name)}\s+{last_init}\.?\b"

        if (re.search(p1, source) or 
            re.search(p2, source) or 
            re.search(p3, source) or
            re.search(p4, source) or
            re.search(p5, source)):
            return True
            
    return False

merged_check['Is_Self'] = merged_check.apply(is_author_in_source_robust, axis=1)
self_counts = merged_check[merged_check['Is_Self']].groupby(['Rights Holder', 'Match_Key']).size().reset_index(name='Self_Count')

# ==========================================
# PART D: BUILD THE FINAL TABLE (The Merge)
# ==========================================

# 1. Join Total + Self Counts
stats = pd.merge(total_counts, self_counts, on=['Rights Holder', 'Match_Key'], how='left')
stats['Self_Count'] = stats['Self_Count'].fillna(0).astype(int)

# 2. Join with Author Metadata
# CHANGE: We now select 'Authors' (The Original) instead of 'Clean_Authors'
final_df = pd.merge(stats, df_auth[['Match_Key', 'Authors', 'Clean_Title']], on='Match_Key', how='left')

# 3. Rename
final_df = final_df.rename(columns={
    'Match_Key': 'Chapter Number',
    'Authors': 'Chapter Author Name',  # Maps the raw column to your name
    'Clean_Title': 'Chapter Title',
    'Total Assets': 'Total Rights holder Assets',
    'Self_Count': 'Chapter Author is Source Author'
})

# 4. Reorder
final_df = final_df[[
    'Rights Holder', 
    'Chapter Number', 
    'Chapter Author Name', 
    'Chapter Title', 
    'Total Rights holder Assets', 
    'Chapter Author is Source Author'
]]

# 5. Sort
final_df['Sort_Num'] = final_df['Chapter Number'].str.extract(r'(\d+)')[0].astype(int)
final_df = final_df.sort_values(['Rights Holder', 'Sort_Num']).drop(columns=['Sort_Num'])

final_df