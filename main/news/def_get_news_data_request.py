import json
from datetime import datetime
from aws_services.def_boto3_aws import write_log_dynamodb
import requests
import pandas as pd


api_key = "*******************"


def get_news_api_request(dict_input, counter):
    keywords = dict_input['keywords']
    country = dict_input['country']

    try:
        id = []
        title = []
        news_time = []
        source = []
        likes = []
        if country == "":
            country = 'us'
        else:
            country = country

        if keywords == "":
            return ("Keyword are required")
        else:

            url = "https://newsdata.io/api/1/news"

            params = {"apikey": api_key,
                      "q": keywords,
                      "language": 'en',
                      "from_date": dict_input['From Date'],
                      "country": country,
                      "page": counter + 1
                      }

            resp = requests.get(url, params=params)
            data = json.dumps(resp.json(), indent=6, sort_keys=True)
            data = json.loads(data)

            for article in data["results"]:
                id.append(123456789)
                title.append(article["title"])
                news_time.append(article["pubDate"])
                source.append('news_' + article["source_id"])
                likes.append(1)


            # df = pd.DataFrame({'news_time': news_time, 'title_text': title, 'likes': likes, 'source': source})
            df = pd.DataFrame({'id': id, 'time': news_time, 'likes': likes, 'text': title, 'source': source})
            data = df.to_json(orient='index', indent=4)
            return data
    except Exception as e:
        write_log_dynamodb('get_news_api_request', e)
        return f"get_news_api went wrong: {e}"

