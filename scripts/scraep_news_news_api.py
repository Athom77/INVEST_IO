import requests

def fetch_news(api_key, topic, language='en', page_size=100):
    """
    Fetch news articles about a specified topic using NewsAPI.

    Parameters:
    - api_key: Your API key for NewsAPI.
    - topic: The topic you want to fetch news about.
    - language: The language of articles to fetch. Default is English.
    - page_size: The number of results to return per page. Maximum is 100.

    Returns:
    A list of articles about the specified topic.
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
        return articles
    else:
        print(f"Failed to fetch news: {response.status_code}")
        return []

# Example usage
if __name__ == "__main__":
    YOUR_API_KEY = "9339c10752464c27994cb567c2ca9bfd" # Replace with your actual NewsAPI key
    TOPIC = "Unicredit" # Replace with your topic of interest
    articles = fetch_news(YOUR_API_KEY, TOPIC)
    
    for article in articles:
        print(f"Title: {article['title']}")
        print(f"Description: {article['description']}")
        print(f"URL: {article['url']}")
        print("-" * 80)
