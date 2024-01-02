import requests
import json
import urllib
from datetime import datetime
from main import *


@lynx()
def lambda_handler(event, context):
    tickers = ['CA.PA', 'SU.PA', 'GE', 'SNY']

    for ticker in tickers:
        params = urllib.parse.urlencode({
            'api_token': 'F7dEBWGdlEQnqbSRHF8VTQ7Avso1lWfJUncRFKf5',
            'symbols': ticker,
            'limit': 50,
        })
        res = requests.get(
            f"""https://api.marketaux.com/v1/news/all?{params}""").json()

        event.change("""
        INSERT INTO `schema`.PublicCompaniesNewsArticles (ticker, payload, timestamp)
        VALUES (:ticker, :payload, :timestamp)
        """, {
            'ticker': ticker,
            'payload': json.dumps(res),
            'timestamp': datetime.utcnow().isoformat()
        })
