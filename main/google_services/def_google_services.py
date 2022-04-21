import json
from google.cloud import language_v1
import pandas as pd
import os

# google sentimental detection initiate
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"/home/naya/Final_Project/main/google_services/smiling-pact-342521-3bc85395c28c.json"
client = language_v1.LanguageServiceClient()


def sentimental_api(txt):
    document = language_v1.Document(
        content=txt, type_=language_v1.Document.Type.PLAIN_TEXT
    )
    sentiment = client.analyze_sentiment(
        request={"document": document}
    ).document_sentiment
    if sentiment.score > 0.5:
        sentiment_category = "POSITIVE"
    if sentiment.score < 0:
        sentiment_category = "NEGATIVE"
    else:
        sentiment_category = "NETURAL"
    # return ([sentiment_category.capitalize(), abs(sentiment.score)])
    return (sentiment_category.capitalize())
    # return (sentiment)

def sentimental_api_score(txt):
    document = language_v1.Document(
        content=txt, type_=language_v1.Document.Type.PLAIN_TEXT
    )
    sentiment = client.analyze_sentiment(
        request={"document": document}
    ).document_sentiment
    if sentiment.score > 0.5:
        sentiment_category = "POSITIVE"
    if sentiment.score < 0:
        sentiment_category = "NEGATIVE"
    else:
        sentiment_category = "NETURAL"
    return abs(sentiment.score)
