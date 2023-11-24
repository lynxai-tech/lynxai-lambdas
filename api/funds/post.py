import json
from main import *


@lynx()
def lambda_handler(event, context):
    
    parsed_event_body = get_parsed_event_body(event.body())

    latest_investment_fund, latest_date = get_latest_investment_fund_by_post_body(
        parsed_event_body)

    fund_name = parsed_event_body['fundName']

    clientId = event.select("SELECT id FROM `schema`.client WHERE name = (:client_name)", {
                            'client_name': (parsed_event_body['clientName'])}).val()

    event.change("""INSERT INTO `schema`.main_fund 
                    (client_id, fund_name, fund_type, eu_sfdr_article, country, status, portfolio_weight_percentage, date) 
                    VALUES ((:client_id),(:fund_name),(:fund_type),(:eu_sfdr_article),(:country),(:status),(:portfolio_weight_percentage), (:date))""",
                 {
                     'client_id': clientId,
                     'fund_name': fund_name,
                     'fund_type': parsed_event_body['fundType'],
                     'eu_sfdr_article': '',
                     'country': parsed_event_body['country'],
                     'status': 'ready',
                     'portfolio_weight_percentage': '',
                     'date': latest_date
                 })

    main_fund_id = event.select(
        "SELECT id FROM `schema`.main_fund WHERE fund_name = (:fund_name)", {
            'fund_name': fund_name}).val()

    investment_fund_list = get_investment_fund_list(
        parsed_event_body, main_fund_id, fund_name)

    event.change("""INSERT INTO `schema`.investment_fund (fund_name, main_fund_id, date) 
                 VALUES ((:fund_name), (:main_fund_id), (:date))""", investment_fund_list)

    new_assets = get_new_assets(parsed_event_body, event)
    
    if new_assets:
        event.change("""INSERT INTO asset (name, ticker, public_asset, market_cap, type_of_issuer, financial_industry, country, pri_signatory, asset_type, isin, isSynced) 
                    VALUES ((:name),(:ticker),(:pulic_asset),(:market_cap),(:type_of:issuer),(:financial_industry),(:country),(:pri_signature),(:asset_type),(:isin),(:isSynced))""",
                    new_assets)


    investment_details = get_investment_details(
        parsed_event_body, main_fund_id, event)
    
    if investment_details:
        event.change("""INSERT INTO `schema`.investment_detail (fund_id, asset_id, investment_type, currency, nominal_amount, portfolio_asset_weight_non_financial, share_amount) 
                    VALUES ((:fund_id), (:asset_id), (investment_type), (:currency), (:nominal_amount), (:portfolio_asset_weight_non_financial), (:share_amount))""", investment_details)

    return "success"
