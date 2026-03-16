"""
AI-Powered News Insights Generator
Fetches articles from silver tables, gets AI summaries, and populates gold insights table.
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import execute_values
import ollama
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': os.getenv('POSTGRES_PORT'),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD')
}

if not DB_CONFIG['user'] or not DB_CONFIG['password']:
    raise EnvironmentError(
        "Missing required environment variables: POSTGRES_USER and/or POSTGRES_PASSWORD. "
        "Please set them in .env file or docker-compose environment."
    )

OLLAMA_HOST = os.getenv('OLLAMA_HOST')
OLLAMA_MODEL = os.getenv('OLLAMA_MODEL')

ollama_client = ollama.Client(host=OLLAMA_HOST)


def fetch_article_text(url: str, timeout: int = 10) -> Optional[str]:
    """
    Fetch and extract article text from URL.
    Returns None if fetch fails or if URL is inaccessible.
    """
    try:
        headers = {            
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        for script in soup(["script", "style", "nav", "footer", "header"]):
            script.decompose()
        
        article = soup.find('article') or soup.find('div', class_='article-body') or soup.find('main')
        
        if article:
            text = article.get_text(separator=' ', strip=True)
        else:
            paragraphs = soup.find_all('p')
            text = ' '.join([p.get_text(strip=True) for p in paragraphs])
        
        text = ' '.join(text.split())
        
        # Return only if substantial content (at least 100 chars)
        return text if len(text) > 100 else None
        
    except Exception as e:
        logger.warning(f"Failed to fetch {url}: {str(e)}")
        return None


def validate_and_summarize_aml(article_text: str, tone: float, max_words: int = 40) -> Tuple[bool, Optional[str]]:
    """
    OPTIMIZED: Single API call to validate AML relevance AND generate summary.
    Returns: (is_aml_relevant, summary_or_none)
    
    This cuts API calls in half compared to separate validation + summarization.
    """
    try:
        # Truncate article if too long (to avoid token limits)
        max_chars = 4000
        if len(article_text) > max_chars:
            article_text = article_text[:max_chars] + "..."
        
        prompt = f"""Task: Analyze if this article is about money laundering, anti-money laundering (AML), financial crimes, corruption, fraud, or illegal financial activities.

Instructions:
1. If the article IS primarily about AML/financial crimes: Write ONLY a {max_words}-word maximum abstract. DO NOT include phrases like "Here is a summary", "Summary:", "This article is about", or any meta-text. Start directly with the content.
2. If the article IS NOT about AML/financial crimes: Respond with exactly "NOT RELEVANT"

Sentiment tone score: {tone:.2f} (range: -10 to +10)

Article:
{article_text}

Response:"""
        
        response = ollama_client.chat(
            model=OLLAMA_MODEL,
            messages=[{'role': 'user', 'content': prompt}]
        )
        
        content = response['message']['content'].strip()
        
        if 'NOT RELEVANT' in content.upper():
            return (False, None)
        
        words = content.split()
        if len(words) > max_words:
            summary = ' '.join(words[:max_words]) + '...'
        else:
            summary = content
        
        return (True, summary)
        
    except Exception as e:
        logger.warning(f"AML validation+summary failed: {str(e)} - defaulting to relevant")
        return (True, f"Summary unavailable. Tone: {tone:.2f}")


def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(**DB_CONFIG)


def ensure_state_table():
    """Create state tracking table if it doesn't exist."""
    create_table_sql = """
    CREATE SCHEMA IF NOT EXISTS meta;
    
    CREATE TABLE IF NOT EXISTS meta.ai_insights_watermark (
        process_name TEXT PRIMARY KEY,
        last_processed_date DATE NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Could not create state table: {e}")


def get_last_processed_date() -> datetime:
    """
    Get the date that was last successfully processed.
    Returns the NEXT date to process (last processed + 1 day).
    If no date processed yet, returns 2026-01-01.
    """
    ensure_state_table()
    
    query = """
    SELECT last_processed_date + INTERVAL '1 day' as next_date
    FROM meta.ai_insights_watermark
    WHERE process_name = 'daily_ai_insights'
    """
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchone()
        cur.close()
        conn.close()
        
        if result:
            next_date = result[0]
            logger.info(f"Next date to process: {next_date.date()}")
            return next_date
        else:
            logger.info("No watermark found, starting from 2026-01-01")
            return datetime(2026, 1, 1)
    except Exception as e:
        logger.warning(f"Could not retrieve last processed date: {e}")
        logger.info("Defaulting to 2026-01-01")
        return datetime(2026, 1, 1)


def update_processed_date(processed_date: datetime):
    """Update the watermark with the last successfully processed date."""
    upsert_query = """
    INSERT INTO meta.ai_insights_watermark (process_name, last_processed_date, updated_at)
    VALUES ('daily_ai_insights', %s::date, NOW())
    ON CONFLICT (process_name) 
    DO UPDATE SET 
        last_processed_date = EXCLUDED.last_processed_date,
        updated_at = NOW();
    """
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(upsert_query, (processed_date,))
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"✅ Updated watermark: {processed_date.date()}")
    except Exception as e:
        logger.error(f"Failed to update watermark: {e}")


def query_articles_for_date(target_date: datetime) -> List[Dict]:
    """
    Query ALL articles for a single date (article-level, no country dimension).
    This processes one complete day at a time for comprehensive coverage.
    
    Returns all articles from target_date (00:00:00 to 23:59:59)
    Country relationships are handled separately in fact_countries via dbt.
    """
    query = """
    SELECT 
        s.news_hkey,
        s.source_domain,
        s.source_url,
        s.tone_overall,
        s.tone_positive,
        s.tone_negative,
        s.tone_polarity,
        s.event_datetime
    FROM main_raw_vault.sat_news_tone s
    WHERE s.event_datetime::date = %s::date
      AND s.source_url IS NOT NULL
      AND s.tone_overall IS NOT NULL
    ORDER BY s.tone_overall ASC  -- Process most negative first for priority
    """
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query, (target_date,))
    
    results = []
    for row in cur.fetchall():
        results.append({
            'news_hkey': row[0],
            'source': row[1],
            'url': row[2],
            'tone': row[3],
            'tone_positive': row[4],
            'tone_negative': row[5],
            'tone_polarity': row[6],
            'event_datetime': row[7]
        })
    
    cur.close()
    conn.close()
    return results


def insert_insights(insights: List[Dict]):
    """Insert insights into main_gold.fact_ai_news_insights table."""
    if not insights:
        logger.info("No insights to insert")
        return
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    insert_query = """
    INSERT INTO main_gold.fact_ai_news_insights 
        (news_hkey, event_datetime, source_domain, source_url,
         tone_overall, tone_positive, tone_negative, tone_polarity,
         ai_abstract, generated_at)
    VALUES %s
    ON CONFLICT (news_hkey) DO NOTHING;
    """
    
    values = [
        (
            insight['news_hkey'],
            insight['event_datetime'],
            insight['source'],
            insight['url'],
            insight['tone'],
            insight.get('tone_positive'),
            insight.get('tone_negative'),
            insight.get('tone_polarity'),
            insight['summary'],
            datetime.now()
        )
        for insight in insights
    ]
    
    execute_values(cur, insert_query, values)
    conn.commit()
    
    logger.info(f"✅ Inserted {len(insights)} insights into main_gold.fact_ai_news_insights")
    
    cur.close()
    conn.close()


def main():
    """
    Main execution function.
    Processes ALL articles from ONE DAY at a time, chronologically.
    Each run processes the next unprocessed day starting from 2026-01-01.
    """
    logger.info("="*60)
    logger.info("🚀 Starting AI Insights Generator (Daily Batch Mode)")
    logger.info("="*60)
    
    # Get next date to process
    target_date = get_last_processed_date()
    logger.info(f"📅 Processing date: {target_date.date()}")
    
    # Check if we've caught up to today
    if target_date.date() > datetime.now().date():
        logger.info("\n✅ All dates processed! System is up to date.")
        logger.info(f"   Last processed: {(target_date - timedelta(days=1)).date()}")
        return
    
    # Query ALL articles for this date
    logger.info("\n📊 Querying all articles for this date...")
    all_news = query_articles_for_date(target_date)
    
    if not all_news:
        logger.info(f"\n⚠️  No articles found for {target_date.date()}")
        logger.info("   Marking date as processed and moving to next day.")
        update_processed_date(target_date)
        return
    
    logger.info(f"  └─ Found {len(all_news)} articles total")
    logger.info("="*60)
    
    # Process each article with incremental saving
    saved_count = 0
    skipped_not_aml = 0
    skipped_fetch_failed = 0
    
    for i, news in enumerate(all_news, 1):
        logger.info(f"\n[{i}/{len(all_news)}] {target_date.date()} - Tone: {news['tone']:.2f}")
        logger.info(f"  URL: {news['url'][:80]}...")
        
        article_text = fetch_article_text(news['url'])
        
        if article_text:
            logger.info(f"  🤖 AI validation + summarization...")
            is_aml_relevant, summary = validate_and_summarize_aml(article_text, news['tone'], max_words=40)
            
            if not is_aml_relevant:
                logger.warning(f"  ✗ Not AML-relevant")
                skipped_not_aml += 1
                time.sleep(1)
                continue
            
            logger.info(f"  ✓ AML-relevant")
            news['summary'] = summary
            
            # ✅ Save IMMEDIATELY to database (crash-resistant)
            try:
                insert_insights([news])
                saved_count += 1
                logger.info(f"  ✅ Saved to DB ({saved_count} total)")
            except Exception as e:
                logger.error(f"  ❌ Failed to save: {e}")
            
            logger.info(f"  ✓ {summary[:60]}...")
        else:
            logger.warning(f"  ✗ Could not fetch")
            skipped_fetch_failed += 1
        
        time.sleep(1)    
    
    logger.info("\n" + "="*60)
    logger.info(f"✅ Date {target_date.date()} Processing Complete!")
    logger.info("="*60)
    logger.info(f"📊 Statistics for {target_date.date()}:")
    logger.info(f"  ├─ Total articles scanned: {len(all_news)}")
    logger.info(f"  ├─ AML-relevant articles saved: {saved_count}")
    logger.info(f"  ├─ Not AML-relevant: {skipped_not_aml}")
    logger.info(f"  └─ Fetch failed: {skipped_fetch_failed}")
    logger.info("="*60)
    
    # Update watermark to mark this date as processed
    update_processed_date(target_date)
    
    if saved_count > 0:
        logger.info(f"\n💡 Next run will process: {(target_date + timedelta(days=1)).date()}")
    else:
        logger.info(f"\n⚠️  No AML articles found for {target_date.date()}")
        logger.info(f"   Next run will process: {(target_date + timedelta(days=1)).date()}")


if __name__ == "__main__":
    main()
