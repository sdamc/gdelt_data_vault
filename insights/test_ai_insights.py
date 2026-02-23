"""
Test AI Insights Generator - Dry Run (No Database Save)
Displays ranked news and LLM-generated insights without persisting to database.
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, List, Optional
import requests
from bs4 import BeautifulSoup
import psycopg2
import ollama
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection parameters
# Note: Queries PostgreSQL Data Vault directly (NOT DuckDB)
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'gdelt'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD')
}

# Validate required environment variables
if not DB_CONFIG['user'] or not DB_CONFIG['password']:
    raise EnvironmentError(
        "Missing required environment variables: POSTGRES_USER and/or POSTGRES_PASSWORD. "
        "Please check your docker-compose environment settings."
    )

# Ollama configuration
OLLAMA_HOST = os.getenv('OLLAMA_HOST', 'http://ollama:11434')
OLLAMA_MODEL = os.getenv('OLLAMA_MODEL', 'llama3.2:3b')

# Initialize Ollama client
ollama_client = ollama.Client(host=OLLAMA_HOST)


def fetch_article_text(url: str, timeout: int = 10) -> Optional[str]:
    """Fetch and extract article text from URL."""
    try:
        headers = {
            # Modern User-Agent: Chrome 120 on Windows 11
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        response = requests.get(url, timeout=timeout, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Try different article selectors
        article = soup.find('article') or soup.find('div', class_='article-body') or soup.find('main')
        
        if article:
            paragraphs = article.find_all('p')
            text = ' '.join([p.get_text().strip() for p in paragraphs])
        else:
            paragraphs = soup.find_all('p')
            text = ' '.join([p.get_text().strip() for p in paragraphs[:10]])
        
        return text if text else None
        
    except Exception as e:
        logger.warning(f"Failed to fetch {url}: {str(e)}")
        return None


def validate_aml_relevance(article_text: str) -> tuple[bool, str]:
    """
    Use LLM to validate if article is truly about money laundering/AML.
    Returns (is_relevant, explanation).
    """
    try:
        sample = article_text[:1000] if len(article_text) > 1000 else article_text
        
        prompt = f"""Is this article primarily about money laundering, anti-money laundering (AML), 
financial crimes, corruption, fraud, or illegal financial activities?

Answer with YES or NO, followed by a brief 1-sentence explanation.

Article excerpt:
{sample}

Answer:"""
        
        response = ollama_client.chat(
            model=OLLAMA_MODEL,
            messages=[{'role': 'user', 'content': prompt}]
        )
        
        answer = response['message']['content'].strip()
        is_relevant = 'YES' in answer.upper()
        
        return is_relevant, answer
        
    except Exception as e:
        logger.warning(f"AML validation failed: {str(e)}")
        return True, f"Validation error: {str(e)}"


def generate_ai_summary(article_text: str, tone: float, max_words: int = 40) -> str:
    """Generate AI summary of article using Ollama."""
    try:
        max_chars = 4000
        if len(article_text) > max_chars:
            article_text = article_text[:max_chars] + "..."
        
        prompt = f"""Summarize the following news article in a maximum of {max_words} words. 
Focus on the key facts and events. Be concise and factual. Do not exceed {max_words} words.

Sentiment tone score: {tone:.2f} (range: -10 to +10)

Article:
{article_text}

Summary (maximum {max_words} words):"""
        
        response = ollama_client.chat(
            model=OLLAMA_MODEL,
            messages=[{'role': 'user', 'content': prompt}]
        )
        
        summary = response['message']['content'].strip()
        
        # Ensure word limit
        words = summary.split()
        if len(words) > max_words:
            summary = ' '.join(words[:max_words]) + '...'
        
        return summary
        
    except Exception as e:
        logger.error(f"AI summary generation failed: {str(e)}")
        return f"Summary unavailable. Tone: {tone:.2f}"


def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(**DB_CONFIG)


def query_top_negative_news(days: int = 7, limit: int = 10) -> List[Dict]:
    """Query top 10 most negative news from last N days."""
    query = """
    SELECT 
        s.news_hkey,
        s.source_domain,
        s.source_url,
        s.tone_overall,
        s.tone_negative,
        s.event_datetime
    FROM main_raw_vault.sat_news_tone s
    WHERE s.event_datetime >= '2026-01-01'::date
      AND s.source_url IS NOT NULL
      AND s.tone_overall IS NOT NULL
      AND s.tone_overall < 0
    ORDER BY s.tone_overall ASC
    LIMIT %s;
    """
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query, (limit,))
    
    results = []
    for row in cur.fetchall():
        results.append({
            'news_hkey': row[0],
            'source': row[1],
            'url': row[2],
            'tone': row[3],
            'event_date': row[5],
            'category': 'NEGATIVE'
        })
    
    cur.close()
    conn.close()
    return results


def query_top_positive_news(days: int = 7, limit: int = 10) -> List[Dict]:
    """Query top 10 most positive news from last N days."""
    query = """
    SELECT 
        s.news_hkey,
        s.source_domain,
        s.source_url,
        s.tone_overall,
        s.tone_positive,
        s.event_datetime
    FROM main_raw_vault.sat_news_tone s
    WHERE s.event_datetime >= '2026-01-01'::date
      AND s.source_url IS NOT NULL
      AND s.tone_overall IS NOT NULL
      AND s.tone_overall > 0
    ORDER BY s.tone_overall DESC
    LIMIT %s;
    """
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query, (limit,))
    
    results = []
    for row in cur.fetchall():
        results.append({
            'news_hkey': row[0],
            'source': row[1],
            'url': row[2],
            'tone': row[3],
            'event_date': row[5],
            'category': 'POSITIVE'
        })
    
    cur.close()
    conn.close()
    return results


def query_polarizing_by_continent(days: int = 7) -> List[Dict]:
    """Query most polarizing news per continent."""
    query = """
    WITH news_with_continent AS (
        SELECT 
            s.news_hkey,
            s.source_domain,
            s.source_url,
            s.tone_overall,
            s.tone_polarity,
            s.event_datetime,
            dc.continent,
            dc.country_name
        FROM main_raw_vault.sat_news_tone s
        INNER JOIN main_raw_vault.link_news_country l ON s.news_hkey = l.news_hkey
        INNER JOIN main_raw_vault.hub_country h ON l.country_hkey = h.country_hkey
        INNER JOIN main_gold.dim_country dc ON h.country_code = dc.country_code
        WHERE s.event_datetime >= '2026-01-01'::date
          AND s.source_url IS NOT NULL
          AND s.tone_polarity IS NOT NULL
          AND dc.continent IS NOT NULL
    ),
    ranked AS (
        SELECT 
            news_hkey,
            source_domain,
            source_url,
            tone_overall,
            tone_polarity,
            continent,
            country_name,
            event_datetime,
            ROW_NUMBER() OVER (PARTITION BY continent ORDER BY tone_polarity DESC) as rn
        FROM news_with_continent
    )
    SELECT 
        news_hkey,
        source_domain,
        source_url,
        tone_overall,
        tone_polarity,
        continent,
        country_name,
        event_datetime
    FROM ranked
    WHERE rn = 1;
    """
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query)
    
    results = []
    for row in cur.fetchall():
        results.append({
            'news_hkey': row[0],
            'source': row[1],
            'url': row[2],
            'tone': row[3],
            'polarity': row[4],
            'continent': row[5],
            'country': row[6],
            'event_date': row[7],
            'category': f'POLARIZING_{row[5]}'
        })
    
    cur.close()
    conn.close()
    return results


def print_separator():
    print("\n" + "="*100 + "\n")


def main():
    """Main execution function - TEST MODE."""
    print("\n" + "🧪 AI INSIGHTS GENERATOR - TEST MODE (DRY RUN)".center(100, "="))
    print("📝 Testing LLM validation and summary generation without saving to database\n")
    
    # Collect all news to process
    logger.info("Querying top negative news...")
    negative_news = query_top_negative_news(days=7, limit=10)
    
    logger.info("Querying top positive news...")
    positive_news = query_top_positive_news(days=7, limit=10)
    
    logger.info("Querying polarizing news by continent...")
    polarizing_news = query_polarizing_by_continent(days=7)
    
    all_news = negative_news + positive_news + polarizing_news
    logger.info(f"Total news items to process: {len(all_news)}")
    
    print_separator()
    
    # Process each news item
    insights = []
    skipped_not_aml = 0
    skipped_fetch_failed = 0
    
    for i, news in enumerate(all_news, 1):
        print(f"\n📰 [{i}/{len(all_news)}] {news['category']} | Tone: {news['tone']:.2f}")
        print(f"🔗 {news['url']}")
        print(f"📅 {news['event_date']}")
        
        # Fetch article text
        article_text = fetch_article_text(news['url'])
        
        if not article_text:
            print(f"❌ FAILED: Could not fetch article")
            skipped_fetch_failed += 1
            time.sleep(1)
            continue
        
        print(f"✅ Fetched {len(article_text)} characters")
        
        # Validate AML relevance with LLM
        print(f"🤖 Validating AML relevance...")
        is_aml_relevant, validation_explanation = validate_aml_relevance(article_text)
        
        print(f"📊 LLM Validation: {validation_explanation}")
        
        if not is_aml_relevant:
            print(f"⛔ SKIPPED: Not AML-relevant")
            skipped_not_aml += 1
            time.sleep(1)
            continue
        
        print(f"✅ AML-Relevant - Proceeding to summarize...")
        
        # Generate AI summary
        summary = generate_ai_summary(article_text, news['tone'], max_words=40)
        news['summary'] = summary
        news['validation'] = validation_explanation
        insights.append(news)
        
        print(f"📝 SUMMARY: {summary}")
        print_separator()
        
        # Rate limiting
        time.sleep(2)
    
    # Final summary
    print("\n" + "📊 FINAL RESULTS".center(100, "=") + "\n")
    print(f"Total items processed: {len(all_news)}")
    print(f"✅ AML-Relevant insights: {len(insights)}")
    print(f"⛔ Skipped (not AML-relevant): {skipped_not_aml}")
    print(f"❌ Skipped (fetch failed): {skipped_fetch_failed}")
    
    print_separator()
    print("✅ CURATED INSIGHTS (AML-Relevant Only)".center(100, "="))
    print_separator()
    
    for idx, insight in enumerate(insights, 1):
        print(f"\n🏆 INSIGHT #{idx}: {insight['category']}")
        print(f"   Tone: {insight['tone']:.2f}")
        print(f"   URL: {insight['url']}")
        print(f"   Summary: {insight['summary']}")
        if 'polarity' in insight:
            print(f"   Polarity: {insight['polarity']:.2f}")
        if 'continent' in insight:
            print(f"   Location: {insight['country']}, {insight['continent']}")
    
    print_separator()
    print("✅ Test complete! No data was saved to the database.\n")


if __name__ == "__main__":
    main()
