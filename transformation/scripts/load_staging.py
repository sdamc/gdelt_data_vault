#!/usr/bin/env python3
"""
Load bronze CSV files into PostgreSQL staging table.
Run this BEFORE dbt to populate staging.stg_gkg_news
"""

import pandas as pd
import glob
from datetime import datetime
import os
from sqlalchemy import create_engine

def load_bronze_to_staging():
    """Read bronze CSV files and load into PostgreSQL staging table"""
    
    # Database connection from environment variables
    db_user = os.getenv('POSTGRES_USER')
    db_pass = os.getenv('POSTGRES_PASSWORD')
    db_host = os.getenv('POSTGRES_HOST', 'postgres')
    db_name = os.getenv('POSTGRES_DB', 'gdelt')
    
    # Validate required credentials
    if not db_user or not db_pass:
        raise EnvironmentError(
            "Missing required environment variables: POSTGRES_USER and/or POSTGRES_PASSWORD. "
            "Please set them in docker-compose.yml environment."
        )
    
    engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:5432/{db_name}')
    
    # Find all bronze files (2026+ only to avoid processing old data)
    bronze_path = '/workspace/data/bronze'
    file_pattern = f'{bronze_path}/*.bronze.csv'
    all_files = sorted(glob.glob(file_pattern))
    
    # Filter to only 2026+ files
    files = [f for f in all_files if os.path.basename(f).startswith('2026')]
    
    if not files:
        raise FileNotFoundError(f"No 2026+ bronze files found at {bronze_path}")
    
    print(f"📁 Found {len(files)} bronze files from 2026+ (skipped {len(all_files) - len(files)} older files)")
    
    # Read all CSV files
    dfs = []
    for file_path in files:
        try:
            df = pd.read_csv(
                file_path,
                sep='\t',
                header=None,
                encoding='utf-8',
                encoding_errors='ignore',
                low_memory=False,
                dtype=str
            )
            df['source_file'] = file_path
            dfs.append(df)
        except Exception as e:
            print(f"⚠️  Warning: Could not read {file_path}: {e}")
            continue
    
    if not dfs:
        raise ValueError("No data loaded from bronze files")
    
    all_data = pd.concat(dfs, ignore_index=True)
    
    # Parse GDELT GKG columns
    all_data.columns = [
        'col_0', 'col_1', 'col_2', 'col_3', 'col_4', 'col_5', 'col_6', 
        'col_7', 'col_8', 'col_9', 'col_10', 'col_11', 'col_12', 'col_13',
        'col_14', 'col_15', 'col_16', 'col_17', 'col_18', 'col_19', 'col_20',
        'col_21', 'col_22', 'col_23', 'col_24', 'col_25', 'col_26'
    ] + ['source_file']
    
    # Extract relevant columns
    parsed = pd.DataFrame({
        'gkg_record_id': all_data['col_0'],
        'event_datetime': pd.to_datetime(all_data['col_1'], format='%Y%m%d%H%M%S', errors='coerce'),
        'source_domain': all_data['col_3'],
        'source_url': all_data['col_4'],
        'locations_raw': all_data['col_9'],  # V1 Locations column
        'tone_raw': all_data['col_15'],
        'source_file': all_data['source_file']
    })
    
    # Parse tone scores
    tone_split = parsed['tone_raw'].str.split(',', expand=True, n=6)
    parsed['tone_overall'] = pd.to_numeric(tone_split[0], errors='coerce')
    parsed['tone_positive'] = pd.to_numeric(tone_split[1], errors='coerce')
    parsed['tone_negative'] = pd.to_numeric(tone_split[2], errors='coerce')
    parsed['tone_polarity'] = pd.to_numeric(tone_split[3], errors='coerce')
    parsed['tone_activity'] = pd.to_numeric(tone_split[4], errors='coerce')
    parsed['word_count'] = pd.to_numeric(tone_split[6], errors='coerce').fillna(0).astype(int)
    
    # Extract countries from locations (column 9 format: type#name#COUNTRY#region#lat#lon#feature_id)
    def extract_countries(locations_str):
        if pd.isna(locations_str) or not locations_str:
            return []
        countries = set()
        for location in str(locations_str).split(';'):
            parts = location.split('#')
            if len(parts) >= 3:  # Need at least 3 parts to get country code at index 2
                country_code = parts[2].strip()
                if country_code and len(country_code) == 2 and country_code.isalpha():
                    countries.add(country_code)
        return list(countries)
    
    parsed['countries'] = parsed['locations_raw'].apply(extract_countries)
    
    # Convert Python list to PostgreSQL array format: ['US', 'CA'] -> '{US,CA}'
    def list_to_pg_array(lst):
        if not lst:
            return None
        return '{' + ','.join(lst) + '}'
    
    parsed['countries'] = parsed['countries'].apply(list_to_pg_array)
    
    # Add metadata
    parsed['load_datetime'] = datetime.now()
    parsed['record_source'] = 'bronze_csv'
    
    # Remove rows with no record ID
    parsed = parsed[parsed['gkg_record_id'].notna()]
    
    # Create schema if not exists
    with engine.begin() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    
    # Load to PostgreSQL
    print(f"📤 Loading {len(parsed):,} rows to staging.stg_gkg_news...")
    parsed.to_sql(
        'stg_gkg_news',
        engine,
        schema='staging',
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    # Statistics
    def count_countries(pg_array_str):
        if pd.isna(pg_array_str) or pg_array_str is None:
            return 0
        # '{US,CA,GB}' -> 3 countries
        return len(pg_array_str.strip('{}').split(',')) if pg_array_str.strip('{}') else 0
    
    total_countries = parsed['countries'].apply(count_countries).sum()
    print(f"✅ Loaded {len(parsed):,} news records")
    print(f"📅 Date range: {parsed['event_datetime'].min()} to {parsed['event_datetime'].max()}")
    print(f"🌍 Extracted {total_countries:,} country mentions")
    print(f"✅ Table created: staging.stg_gkg_news")

if __name__ == '__main__':
    load_bronze_to_staging()
