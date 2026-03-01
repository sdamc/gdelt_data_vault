"""
Export Gold Layer Tables to CSV for Tableau Public
"""
import os
import psycopg2
import csv
from datetime import datetime

# Database connection from environment variables
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    database=os.getenv("POSTGRES_DB", "gdelt"),
    user=os.getenv("POSTGRES_USER", "gdelt_user"),
    password=os.getenv("POSTGRES_PASSWORD", "gdelt_pass")
)

# Output directory
output_dir = "/workspace/data/gold"
os.makedirs(output_dir, exist_ok=True)

def export_table_to_csv(query, filename):
    """Execute query and export results to CSV"""
    print(f"Exporting {filename}...")
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        
        filepath = os.path.join(output_dir, filename)
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(colnames)
            writer.writerows(rows)
        
        print(f"✅ Exported {len(rows):,} rows to {filename}")
        return len(rows)

# Export fact_ai_news_insights
total_insights = export_table_to_csv(
    "SELECT * FROM main_gold.fact_ai_news_insights ORDER BY event_datetime DESC",
    "fact_ai_news_insights.csv"
)

# Export fact_weekly_country_sentiment
total_weekly = export_table_to_csv(
    "SELECT * FROM main_gold.fact_weekly_country_sentiment ORDER BY week_date DESC, country_name",
    "fact_weekly_country_sentiment.csv"
)

# Export dim_country
total_countries = export_table_to_csv(
    "SELECT * FROM main_gold.dim_country ORDER BY country_name",
    "dim_country.csv"
)

conn.close()

print("\n" + "="*60)
print(f"✅ Gold Layer Export Complete!")
print(f"   AI News Insights: {total_insights:,} articles")
print(f"   Weekly Sentiment: {total_weekly:,} aggregations")
print(f"   Countries: {total_countries:,} countries")
print(f"   Location: {output_dir}")
print(f"   Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*60)
