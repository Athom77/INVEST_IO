import requests
import pandas as pd
from datetime import datetime
from utils.db_utils import *

def fetch_news(api_key, topic, language='en', page_size=100):
    """
    Fetch news articles about a specified topic using NewsAPI.

    Parameters:
    - api_key: Your API key for NewsAPI.
    - topic: The topic you want to fetch news about.
    - language: The language of articles to fetch. Default is English.
    - page_size: The number of results to return per page. Maximum is 100.

    Returns:
    A DataFrame containing articles about the specified topic.
    """
    base_url = "https://newsapi.org/v2/everything"
    params = {
        "q": topic,
        "language": language,
        "pageSize": page_size,
        "apiKey": api_key
    }
    
    response = requests.get(base_url, params=params)
    
    if response.status_code == 200:
        articles = response.json().get('articles', [])
        df = pd.DataFrame(articles)
        return df
    else:
        print(f"Failed to fetch news: {response.status_code}")
        return pd.DataFrame()

def save_to_csv(df, topic):
    """
    Saves the DataFrame to a CSV file with a dynamic name that includes the topic and timestamp.

    Parameters:
    - df: The DataFrame to save.
    - topic: The news topic, used in the filename.
    """
    if not df.empty:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{topic}_{timestamp}.csv"
        df.to_csv(filename, index=False)
        print(f"Saved to {filename}")
    else:
        print("DataFrame is empty. No CSV file was created.")

# Example usage
if __name__ == "__main__":
    YOUR_API_KEY = "your_api_key_here"  # Replace with your actual NewsAPI key
    TOPIC = "Unicredit"  # Replace with your topic of interest
    df_articles = fetch_news(YOUR_API_KEY, TOPIC)
    
    # Optionally save to database
    # db_utils.insert_dataframe_into_table(df_articles, "sr_news_newsapi", db_config)
    db_config = {
        "host": "pi",
        "user": "root",
        "password": "password",
        "database": "investio"
    }
    
    df_articles = fetch_news(YOUR_API_KEY, TOPIC)
    if not df_articles.empty:
        db_utils.create_table_from_df(df_articles, "sr_news_newsapi", db_config)
        db_utils.insert_dataframe_into_table(df_articles, "sr_news_newsapi", db_config)

    # Save to CSV
    save_to_csv(df_articles, TOPIC)
