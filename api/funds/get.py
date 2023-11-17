from main import *


@lynx()
def lambda_handler(event, context):
    fund_id = event.query('fundId')
    client_name = event.query('clientName')

    fund = get_fund(event, fund_id, client_name)

    latest_investment_fund = get_latest_investment_fund_by_get_fund(fund)

    latest_investment_fund_assets = latest_investment_fund.get('assets', [])

    number_assets_history = {
        'value_private': len([item for item in latest_investment_fund_assets if item.get('public_asset') in ['Private', '0']]),
        'value_public': len([item for item in latest_investment_fund_assets if item.get('public_asset') in ['Public', '1']])
    }

    amount = sum([float(asset.get('nominal_amount', 0))
                 for asset in latest_investment_fund_assets]) or 0
    
    print(67)

    mapped_countries = map_countries_in_fund_asset(
        filter_countries(latest_investment_fund_assets))
    
    print(45)

    return {
        'id': fund.get('id', ''),
        'fund_name': fund.get('fund_name', ''),
        'weight_percentage': fund.get('portfolio_weight_percentage', ''),
        'number_assets_history': number_assets_history,
        'nominal_amount_history': amount,
        'countries_count_list': parse_countries_count_list(mapped_countries),
        'countries_amount_list': parse_countries_amount_list(mapped_countries, amount),
        'assets_industry_list': parse_types_list(latest_investment_fund_assets, 'financial_industry'),
        'asset_type_list': parse_types_list(latest_investment_fund_assets, 'asset_type'),
        'assets_list': fund.get('fund_history', [])
    }
