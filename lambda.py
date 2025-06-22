import requests
import json
import os

# Fetch the API Key from environment variables for security
API_KEY = os.getenv('NEWS_API_KEY', 'd58785f9a6da4e6c86a29e6fc233fc4b')  # Replace with a secure environment variable
BASE_URL = 'https://newsapi.org/v2/top-headlines'

def fetch_news(country='us', category='business'):
    params = {
        'country': country,
        'category': category,
        'apiKey': API_KEY
    }

    response = requests.get(BASE_URL, params=params, timeout=2)  # Set a timeout for requests

    if response.status_code == 200:
        news_data = response.json()
        return news_data.get('articles', [])
    else:
        return []

def lambda_handler(event, context):
    try:
        # Get country and category from the event
        country = event.get('country', 'us')
        category = event.get('category', 'business')

        # Fetch news articles
        articles = fetch_news(country, category)

        # Format response
        if articles:
            formatted_articles = [
                {
                    "title": article.get("title", "No Title"),
                    "description": article.get("description", "No Description"),
                    "url": article.get("url", "No URL")
                }
                for article in articles
            ]
            return {
                "statusCode": 200,
                "body": json.dumps(formatted_articles)
            }
        else:
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "No articles found."})
            }

    except requests.exceptions.RequestException as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Request failed: {str(e)}"})
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

