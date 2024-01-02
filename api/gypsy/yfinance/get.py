import requests
import json
import urllib
from datetime import datetime
import yfinance as yf
from main import *


def read_data(ticker):
    obj = yf.Ticker(ticker)
    info = obj.info
    history = obj.history(period="max")

    h = history.reset_index()
    h['Date'] = h['Date'].map(lambda x: x.isoformat()[:10])
    h['Ticker'] = ticker
    hj = h.to_dict(orient='records')

    return info, hj


def prepare_data(ticker, data):
    return {
        'ticker': ticker,
        'payload': json.dumps(data),
        'timestamp': datetime.utcnow().isoformat()
    }


def prepare_history(ticker, data):
    return {
        'ticker': ticker,
        'date': data.get('Date'),
        'closeValue': data.get('Close'),
        'timestamp': datetime.utcnow().isoformat()
    }


def put_all_data(event, data):
    print(data)
    event.change("""
    INSERT INTO `schema`.PublicCompaniesMetrics (ticker, payload, retrievedAt)
    VALUES (:ticker, :payload, :timestamp)
    ON DUPLICATE KEY UPDATE retrievedAt = VALUES(retrievedAt), payload = VALUES(payload)
    """, data, debug=True)


def put_all_history(event, data):
    event.change("""
    INSERT INTO `schema`.PublicCompaniesHistory (ticker, date, closeValue, retrievedAt)
    VALUES (:ticker, :date, :closeValue, :timestamp)
    ON DUPLICATE KEY UPDATE retrievedAt = (retrievedAt), closeValue = VALUES(closeValue), ticker = VALUES(ticker)
    """, data)



@lynx()
def lambda_handler(event, context):
    tickers = ['CA.PA', 'SU.PA', 'GE', 'SNY']

    tickers = ['CA.PA']  # , 'SU.PA', 'GE', 'SNY']
    all_data, all_history = [], []
    for ticker in tickers:
        data, history = read_data(ticker)
        all_data.append(prepare_data(ticker, data))
        for h in history:
            all_history.append(prepare_history(ticker, h))

    put_all_data(event, all_data)
    put_all_history(event, all_history)