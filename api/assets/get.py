from main import *


@lynx()
def lambda_handler(event, context):
    asset_id = event.query('assetId')
    
    # get_stock_value('EUR')

    asset_info = event.select(
        "SELECT * FROM `schema`.asset WHERE id = (:asset_id)", {'asset_id': asset_id}).list()[0]

    investment_detail_info = get_investment_details_info(event, asset_id)
    
    return {
        'name': asset_info.get('name', ''),
        'public_asset': asset_info.get('public_asset', ''),
        'market_cap': asset_info.get('market_cap', ''),
        'ticker': asset_info.get('ticker', ''),
        'country': asset_info.get('country', ''),
        'financial_industry': asset_info.get('financial_industry', ''),
        'description': asset_info.get('description', ''),
        'isin': asset_info.get('isin', ''),        
        'asset_type': asset_info.get('asset_type', ''),
        'fund_present_number': len(investment_detail_info),
        'total_invested': sum([float(item['nominal_amount']) for item in investment_detail_info]),
        'invested_per_fund': get_invested_per_fund(investment_detail_info)
    }


def get_investment_details_info(event, asset_id):
    client_name = event.query('clientName')

    asset_info_in_fund_list = event.select("""SELECT ID.nominal_amount, ID.fund_id, IFD.main_fund_id, ID.currency
                                            FROM `schema`.investment_detail ID
                                            JOIN (SELECT main_fund_id, id, MAX(date) AS date
                                            FROM `schema`.investment_fund
                                            GROUP BY main_fund_id) AS IFD ON ID.fund_id = IFD.id
                                            WHERE ID.asset_id = (:asset_id)""",
                                           {'asset_id': asset_id}).list()
    
    
    return asset_info_in_fund_list


def get_invested_per_fund(investment_detail_info):
    return [
    {"name": item["fund_id"], "value": float(item["nominal_amount"])}
    for item in investment_detail_info
]
