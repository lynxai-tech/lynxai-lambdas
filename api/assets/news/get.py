from main import *
import json
from itertools import groupby
import datetime

@lynx()
def lambda_handler(event, context):
    res_yf = event.select("""
    SELECT A.*, M.*
      FROM `schema`.asset A
      LEFT JOIN `schema`.PublicCompaniesMetrics M
        ON A.ticker = M.ticker
     WHERE A.id = (:assetId)
    """, {
        'assetId': event.param('id')
    }).list()

    res_history = event.select("""
    SELECT H.*
      FROM `schema`.asset A
      LEFT JOIN `schema`.PublicCompaniesHistory H
        ON A.ticker = H.ticker
     WHERE A.id = (:assetId)
    """, {
        'assetId': event.param('id')
    }).list()


    res_mx = event.select("""
    SELECT N.*
      FROM `schema`.asset A
      LEFT JOIN `schema`.PublicCompaniesNewsArticles N
        ON A.ticker = N.ticker
       AND N.retrievedAt > DATE_SUB(NOW(), INTERVAL 7 DAY)
     WHERE A.id = (:assetId)
     ORDER BY payload ->> '$.published_at' DESC""", {
        'assetId': event.param('id')
    }).list()

    res_esg = event.select("""
    SELECT A.*, N.*
      FROM `schema`.asset A
      LEFT JOIN `schema`.PublicCompaniesESGMetrics N
        ON A.ticker = N.ticker
     WHERE A.id = (:assetId)
     ORDER BY year, metric
    """, {
        'assetId': event.param('id')
    }).list()

    esg_data = {}
    for year, data in groupby(res_esg, lambda x: x['year']):
        esg_data[year] = [{'metric': x['metric'], 'value': x['value']} for x in data]

    return {
        'yfinance': json.loads(res_yf[0].get('payload')) if res_yf and res_yf[0].get('payload') else {},
        'history': [[datetime.datetime.fromisoformat(x['date']).timestamp() * 1000, x['closeValue']] for x in res_history],
        'news': [{**x, 'payload': json.loads(x.get('payload', '{}'))} for x in res_mx],
        'esg': esg_data
    }