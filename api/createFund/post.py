import json
from main import *


@lynx()
def lambda_handler(event, context):

    event_body = event.body()

    print(json.dumps(event_body))

    latest_investment_fund, latest_date = get_latest_investment_fund(
        event_body)

    fund_name = event_body['fundName']

    clientId = event.select("SELECT id FROM `schema`.client WHERE name = (:client_name)", {
                            'client_name': (event_body['clientName'])}).val()

    res = event.change("""INSERT INTO `schema`.main_fund 
                    (client_id, fund_name, fund_type, eu_sfdr_article, country, status, portfolio_weight_percentage, date) 
                    VALUES ((:client_id),(:fund_name),(:fund_type),(:eu_sfdr_article),(:country),(:status),(:portfolio_weight_percentage), (:date))""",
                       {
                           'client_id': clientId,
                           'fund_name': fund_name,
                           'fund_type': event_body['fundType'],
                           'eu_sfdr_article': '',
                           'country': event_body['country'],
                           'status': 'ready',
                           'portfolio_weight_percentage': '',
                           'date': latest_date
                       })

    main_fund_id = event.select(
        "SELECT id FROM `schema`.main_fund WHERE fund_name = (:fund_name)", {
            'fund_name': fund_name}).val()

    investment_fund_list = get_investment_fund_list(
        event_body, main_fund_id, fund_name)

    print(investment_fund_list)

    event.change("""INSERT INTO `schema`.investment_fund (fund_name, main_fund_id, date) 
                 VALUES ((:fund_name), (:main_fund_id), (:date))""", investment_fund_list)
    
    new_assets = insert_new_assets(event_body, event)

    return res
