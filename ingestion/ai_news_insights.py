"""
AI News Insights Generator
Fetches articles from GDELT data, generates AI summaries using Ollama
"""
import os
import sys
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup
from newspaper import Article
import psycopg2
from psycopg2.extras import execute_values
import ollama

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AINewsInsightsGenerator:
    """Generate AI-powered insights from GDELT news articles"""
    
    def __init__(self, 
                 db_host='postgres',
                 db_port=5432,
                 db_name='gdelt',
                 db_user='gdelt_user',
                 db_password='gdelt_pass',
                 ollama_host='http://ollama:11434'):
        
        self.db_config = {
            'host': db_host,
            'port': db_port,
            'database': db_name,
            'user': db_user,
            'password': db_password
        }
        self.ollama_host = ollama_host
        self.ollama_client = ollama.Client(host=ollama_host)
        
    def get_db_connection(self):
        """Create database connection"""
        return psycopg2.connect(**self.db_config)
    
    def fetch_news_for_analysis(self) -> List[Dict]:
        """
        Fetch news articles based on criteria:
        - Last 7 days
        - Top 10 most negative
        - Top 10 most positive
        - Most polarizing by continent
        """
        query = """
        WITH last_7_days AS (
            SELECT 
                s.news_hkey,
                s.source_domain,
                s.source_url,
                s.event_datetime,
                s.tone_overall,
                s.tone_positive,
                s.tone_negative,
                s.tone_polarity,
                c.country_code,
                d.continent,
                d.country_name
            FROM pg_vault.main_raw_vault.sat_news_tone s
            INNER JOIN pg_vault.main_raw_vault.link_news_country l 
                ON s.news_hkey = l.news_hkey
            INNER JOIN pg_vault.main_raw_vault.hub_country c 
                ON l.country_hkey = c.country_hkey
            INNER JOIN pg_vault.dbt_seeds.dim_country d 
                ON c.country_code = d.country_code
            WHERE s.event_datetime >= CURRENT_DATE - INTERVAL '7 days'
                AND s.source_url IS NOT NULL
                AND s.tone_overall IS NOT NULL
        ),
        top_negative AS (
            SELECT DISTINCT ON (source_url)
                news_hkey, source_domain, source_url, event_datetime,
                tone_overall, tone_positive, tone_negative, tone_polarity,
                country_code, continent, country_name,
                'top_negative' as category,
                COUNT(*) OVER (PARTITION BY source_url) as report_count
            FROM last_7_days
            WHERE tone_overall < 0
            ORDER BY source_url, tone_overall ASC
            LIMIT 10
        ),
        top_positive AS (
            SELECT DISTINCT ON (source_url)
                news_hkey, source_domain, source_url, event_datetime,
                tone_overall, tone_positive, tone_negative, tone_polarity,
                country_code, continent, country_name,
                'top_positive' as category,
                COUNT(*) OVER (PARTITION BY source_url) as report_count
            FROM last_7_days
            WHERE tone_overall > 0
            ORDER BY source_url, tone_overall DESC
            LIMIT 10
        ),
        most_polarizing_by_continent AS (
            SELECT DISTINCT ON (continent, source_url)
                news_hkey, source_domain, source_url, event_datetime,
                tone_overall, tone_positive, tone_negative, tone_polarity,
                country_code, continent, country_name,
                'most_polarizing' as category,
                COUNT(*) OVER (PARTITION BY source_url) as report_count
            FROM last_7_days
            WHERE tone_polarity IS NOT NULL
            ORDER BY continent, source_url, ABS(tone_polarity) DESC
        ),
        top_polarizing_per_continent AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY continent ORDER BY ABS(tone_polarity) DESC) as rn
            FROM most_polarizing_by_continent
        )
        SELECT * FROM top_negative
        UNION ALL
        SELECT * FROM top_positive
        UNION ALL
        SELECT news_hkey, source_domain, source_url, event_datetime,
               tone_overall, tone_positive, tone_negative, tone_polarity,
               country_code, continent, country_name, category, report_count
        FROM top_polarizing_per_continent
        WHERE rn = 1
        ORDER BY category, tone_overall;
        """
        
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                columns = [desc[0] for desc in cur.description]
                results = [dict(zip(columns, row)) for row in cur.fetchall()]
        
        logger.info(f"Fetched {len(results)} articles for analysis")
        return results
    
    def fetch_article_content(self, url: str, max_retries: int = 2) -> Optional[str]:
        """
        Fetch and extract article content from URL
        Returns article text or None if failed
        """
        for attempt in range(max_retries):
            try:
                # Try newspaper3k first (better extraction)
                article = Article(url)
                article.download()
                article.parse()
                
                if article.text and len(article.text) > 200:
                    logger.info(f"✓ Fetched article: {url[:60]}...")
                    return article.text[:5000]  # Limit to first 5000 chars
                
            except Exception as e:
                logger.warning(f"Newspaper3k failed for {url}: {e}")
            
            try:
                # Fallback to BeautifulSoup
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Remove script and style elements
                for script in soup(["script", "style"]):
                    script.decompose()
                
                # Try to find article content
                article_text = ""
                for tag in soup.find_all(['article', 'div'], class_=['article', 'content', 'story']):
                    article_text = tag.get_text(separator=' ', strip=True)
                    if len(article_text) > 200:
                        break
                
                if not article_text:
                    article_text = soup.get_text(separator=' ', strip=True)
                
                if len(article_text) > 200:
                    logger.info(f"✓ Fetched article (BS4): {url[:60]}...")
                    return article_text[:5000]
                
            except Exception as e:
                logger.warning(f"BeautifulSoup failed for {url}: {e}")
            
            if attempt < max_retries - 1:
                time.sleep(2)  # Wait before retry
        
        logger.error(f"✗ Failed to fetch: {url}")
        return None
    
    def generate_abstract(self, article_text: str, tone: float, max_words: int = 40) -> str:
        """
        Generate AI abstract using Ollama
        Returns summary limited to max_words
        """
        try:
            prompt = f"""You are a news analyst. Summarize this article in EXACTLY {max_words} words or less. 
Focus on the main point and key facts. Do not include phrases like "This article discusses" or "The text talks about".
Be direct and factual.

Article text:
{article_text[:3000]}

Summary ({max_words} words max):"""
            
            response = self.ollama_client.generate(
                model='llama3.2:3b',  # Using smaller, faster model
                prompt=prompt,
                options={
                    'temperature': 0.3,
                    'num_predict': 60  # Roughly 40 words
                }
            )
            
            abstract = response['response'].strip()
            
            # Ensure word count limit
            words = abstract.split()
            if len(words) > max_words:
                abstract = ' '.join(words[:max_words]) + '...'
            
            logger.info(f"✓ Generated abstract ({len(words)} words)")
            return abstract
            
        except Exception as e:
            logger.error(f"Ollama generation failed: {e}")
            return f"Unable to generate summary. Article discusses news with tone score {tone:.2f}."
    
    def process_and_save_insights(self):
        """Main process: fetch news, get content, generate insights, save to DB"""
        
        logger.info("=" * 60)
        logger.info("Starting AI News Insights Generation")
        logger.info("=" * 60)
        
        # Step 1: Fetch news articles
        articles = self.fetch_news_for_analysis()
        
        if not articles:
            logger.warning("No articles found for analysis")
            return
        
        # Step 2: Process each article
        insights = []
        for idx, article in enumerate(articles, 1):
            logger.info(f"\n[{idx}/{len(articles)}] Processing: {article['category']}")
            
            # Fetch article content
            content = self.fetch_article_content(article['source_url'])
            
            if not content:
                # Use fallback summary if fetch fails
                abstract = f"Unable to fetch content. Sentiment: {article['tone_overall']:.2f}"
            else:
                # Generate AI abstract
                abstract = self.generate_abstract(content, article['tone_overall'])
            
            insights.append({
                'news_hkey': article['news_hkey'],
                'source_domain': article['source_domain'],
                'source_url': article['source_url'],
                'event_datetime': article['event_datetime'],
                'category': article['category'],
                'continent': article['continent'],
                'country_name': article['country_name'],
                'tone_overall': article['tone_overall'],
                'tone_positive': article['tone_positive'],
                'tone_negative': article['tone_negative'],
                'tone_polarity': article['tone_polarity'],
                'report_count': article['report_count'],
                'ai_abstract': abstract,
                'generated_at': datetime.now()
            })
            
            # Rate limiting
            time.sleep(1)
        
        # Step 3: Save to database
        self.save_insights_to_db(insights)
        
        logger.info("=" * 60)
        logger.info(f"✓ Successfully processed {len(insights)} articles")
        logger.info("=" * 60)
    
    def save_insights_to_db(self, insights: List[Dict]):
        """Save insights to gold table"""
        
        insert_query = """
        INSERT INTO pg_vault.gold.fact_ai_news_insights (
            news_hkey, source_domain, source_url, event_datetime,
            category, continent, country_name,
            tone_overall, tone_positive, tone_negative, tone_polarity,
            report_count, ai_abstract, generated_at
        ) VALUES %s
        ON CONFLICT (news_hkey, category) 
        DO UPDATE SET
            ai_abstract = EXCLUDED.ai_abstract,
            generated_at = EXCLUDED.generated_at;
        """
        
        values = [
            (
                i['news_hkey'], i['source_domain'], i['source_url'], i['event_datetime'],
                i['category'], i['continent'], i['country_name'],
                i['tone_overall'], i['tone_positive'], i['tone_negative'], i['tone_polarity'],
                i['report_count'], i['ai_abstract'], i['generated_at']
            )
            for i in insights
        ]
        
        with self.get_db_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_query, values)
            conn.commit()
        
        logger.info(f"✓ Saved {len(insights)} insights to database")


def main():
    """Main entry point"""
    
    # Get config from environment
    db_host = os.getenv('POSTGRES_HOST', 'postgres')
    db_user = os.getenv('POSTGRES_USER', 'gdelt_user')
    db_password = os.getenv('POSTGRES_PASSWORD', 'gdelt_pass')
    ollama_host = os.getenv('OLLAMA_HOST', 'http://ollama:11434')
    
    # Create generator
    generator = AINewsInsightsGenerator(
        db_host=db_host,
        db_user=db_user,
        db_password=db_password,
        ollama_host=ollama_host
    )
    
    # Run process
    try:
        generator.process_and_save_insights()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
