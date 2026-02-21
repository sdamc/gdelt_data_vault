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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection parameters
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'gdelt'),
    'user': os.getenv('POSTGRES_USER', 'gdelt_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'gdelt_pass')
}

# Ollama configuration
OLLAMA_HOST = os.getenv('OLLAMA_HOST', 'http://ollama:11434')
OLLAMA_MODEL = os.getenv('OLLAMA_MODEL', 'llama3.2:3b')

# Initialize Ollama client
ollama_client = ollama.Client(host=OLLAMA_HOST)


def fetch_article_text(url: str, timeout: int = 10) -> Optional[str]:
    """
    Fetch and extract article text from URL.
    Returns None if fetch fails or if URL is inaccessible.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style", "nav", "footer", "header"]):
            script.decompose()
        
        # Try to find article content
        article = soup.find('article') or soup.find('div', class_='article-body') or soup.find('main')
        
        if article:
            text = article.get_text(separator=' ', strip=True)
        else:
            # Fallback: get all paragraph text
            paragraphs = soup.find_all('p')
            text = ' '.join([p.get_text(strip=True) for p in paragraphs])
        
        # Clean up whitespace
        text = ' '.join(text.split())
        
        # Return only if substantial content (at least 100 chars)
        return text if len(text) > 100 else None
        
    except Exception as e:
        logger.warning(f"Failed to fetch {url}: {str(e)}")
        return None


def validate_aml_relevance(article_text: str) -> bool:
    """
    Use LLM to validate if article is truly about money laundering/AML.
    Returns True if article is relevant to AML/financial crimes.
    """
    try:
        # Truncate to first 1000 chars for quick validation
        sample = article_text[:1000] if len(article_text) > 1000 else article_text
        
        prompt = f"""Is this article primarily about money laundering, anti-money laundering (AML), 
financial crimes, corruption, fraud, or illegal financial activities?

Answer ONLY with YES or NO.

Article excerpt:
{sample}

Answer:"""
        
        response = ollama_client.chat(
            model=OLLAMA_MODEL,
            messages=[{'role': 'user', 'content': prompt}]
        )
        
        answer = response['message']['content'].strip().upper()
        return 'YES' in answer
        
    except Exception as e:
        logger.warning(f"AML validation failed: {str(e)} - defaulting to True")
        return True  # Default to True if validation fails


def generate_ai_summary(article_text: str, tone: float, max_words: int = 40) -> str:
    """
    Generate AI summary of article using Ollama.
    Limits output to max_words.
    """
    try:
        # Truncate article if too long (to avoid token limits)
        max_chars = 4000
        if len(article_text) > max_chars:
            article_text = article_text[:max_chars] + "..."
        
        prompt = f"""Summarize the following news article in exactly {max_words} words or fewer. 
Focus on the key facts and events. Be concise and factual.

Sentiment tone score: {tone:.2f} (range: -10 to +10)

Article:
{article_text}

Summary (max {max_words} words):"""
        
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
    WHERE s.event_datetime >= NOW() - INTERVAL '%s days'
      AND s.source_url IS NOT NULL
      AND s.tone_overall IS NOT NULL
      AND s.tone_overall < 0
    ORDER BY s.tone_overall ASC
    LIMIT %s;
    """
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query, (days, limit))
    
    results = []
    for row in cur.fetchall():
        results.append({
            'news_hkey': row[0],
            'source': row[1],
            'url': row[2],
            'tone': row[3],
            'category': 'negative'
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
    WHERE s.event_datetime >= NOW() - INTERVAL '%s days'
      AND s.source_url IS NOT NULL
      AND s.tone_overall IS NOT NULL
      AND s.tone_overall > 0
    ORDER BY s.tone_overall DESC
    LIMIT %s;
    """
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query, (days, limit))
    
    results = []
    for row in cur.fetchall():
        results.append({
            'news_hkey': row[0],
            'source': row[1],
            'url': row[2],
            'tone': row[3],
            'category': 'positive'
        })
    
    cur.close()
    conn.close()
    return results


def query_polarizing_by_continent(days: int = 7) -> List[Dict]:
    """Query most polarizing news per continent (highest tone_polarity)."""
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
        WHERE s.event_datetime >= NOW() - INTERVAL '%s days'
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
        country_name
    FROM ranked
    WHERE rn = 1;
    """
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query, (days,))
    
    # Return one news per continent
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
            'category': f'polarizing_{row[5]}'
        })
    
    cur.close()
    conn.close()
    return results


def insert_insights(insights: List[Dict]):
    """Insert insights into gold.ai_news_insights table."""
    if not insights:
        logger.info("No insights to insert")
        return
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    insert_query = """
    INSERT INTO gold.ai_news_insights 
        (source_domain, source_url, sentiment_score, ai_summary, category, generated_at)
    VALUES %s
    ON CONFLICT (source_url, category) 
    DO UPDATE SET
        sentiment_score = EXCLUDED.sentiment_score,
        ai_summary = EXCLUDED.ai_summary,
        generated_at = EXCLUDED.generated_at;
    """
    
    values = [
        (
            insight['source'],
            insight['url'],
            insight['tone'],
            insight['summary'],
            insight['category'],
            datetime.now()
        )
        for insight in insights
    ]
    
    execute_values(cur, insert_query, values)
    conn.commit()
    
    logger.info(f"Inserted {len(insights)} insights into database")
    
    cur.close()
    conn.close()


def main():
    """Main execution function."""
    logger.info("Starting AI Insights Generator")
    
    # Collect all news to process
    logger.info("Querying top negative news...")
    negative_news = query_top_negative_news(days=7, limit=10)
    
    logger.info("Querying top positive news...")
    positive_news = query_top_positive_news(days=7, limit=10)
    
    logger.info("Querying polarizing news by continent...")
    polarizing_news = query_polarizing_by_continent(days=7)
    
    all_news = negative_news + positive_news + polarizing_news
    logger.info(f"Total news items to process: {len(all_news)}")
    
    # Process each news item
    insights = []
    skipped_not_aml = 0
    
    for i, news in enumerate(all_news, 1):
        logger.info(f"Processing {i}/{len(all_news)}: {news['url']}")
        
        # Fetch article text
        article_text = fetch_article_text(news['url'])
        
        if article_text:
            # Validate AML relevance with LLM
            logger.info(f"Validating AML relevance...")
            is_aml_relevant = validate_aml_relevance(article_text)
            
            if not is_aml_relevant:
                logger.warning(f"✗ Article not AML-relevant, skipping")
                skipped_not_aml += 1
                time.sleep(1)
                continue
            
            logger.info(f"✓ Article is AML-relevant")
            
            # Generate AI summary
            summary = generate_ai_summary(article_text, news['tone'], max_words=40)
            news['summary'] = summary
            insights.append(news)
            logger.info(f"✓ Generated summary: {summary[:100]}...")
        else:
            # Fallback summary
            news['summary'] = f"Article unavailable. Sentiment: {news['tone']:.2f}"
            insights.append(news)
            logger.warning(f"✗ Could not fetch article, using fallback")
        
        # Rate limiting
        time.sleep(1)
    
    # Insert into database
    logger.info("Inserting insights into database...")
    insert_insights(insights)
    
    logger.info("✅ AI Insights generation complete!")
    logger.info(f"Processed: {len(insights)} items")
    logger.info(f"Skipped (not AML-relevant): {skipped_not_aml} items")


if __name__ == "__main__":
    main()
