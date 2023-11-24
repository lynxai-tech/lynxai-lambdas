from main import *


@lynx()
def lambda_handler(event, context):
    fund_id = event.query('fundId')
    client_name = event.query('clientName')

    fund = get_fund(event, fund_id, client_name)

    latest_investment_fund = get_latest_investment_fund_by_get_fund(fund)

    latest_investment_fund_assets = latest_investment_fund.get('assets', [])

    return build_get_fund_response(fund, latest_investment_fund_assets)
