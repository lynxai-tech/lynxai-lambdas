from main import *


@lynx()
def lambda_handler(event, context):
    client_name = event.query('clientName')

    fund_list = get_fund_list(event, client_name)

    fund_analytics_list = []

    for fund in fund_list:
        fund_id = fund['id']
        client_name = event.query('clientName')
        fund = get_fund(event, fund_id, client_name)
        latest_investment_fund = get_latest_investment_fund_by_get_fund(fund)
        latest_investment_fund_assets = latest_investment_fund.get(
            'assets', [])
        fund_analytics_list.append(build_get_fund_response(
            fund, latest_investment_fund_assets))

    return build_response(fund_analytics_list)


def build_response(fund_analytics_list):
    return {
        'number_funds': len(fund_analytics_list),
        'portfolio_amount':  sum([float(item['nominal_amount_history']) for item in fund_analytics_list]),
        'countries_amount_list': sum_countries_amount(fund_analytics_list),
        'countries_count_list': sum_countries_amount(fund_analytics_list),
        'assets_industry_list': sum_type_list(fund_analytics_list, 'assets_industry_list'),
        'asset_type_list': sum_type_list(fund_analytics_list, 'asset_type_list')
    }


def sum_countries_amount(response_array):
    result = {}
    summed_amounts = []

    # Calculate summed amounts for each country
    for response in response_array:
        for item in response['countries_amount_list']:
            label = item['label']
            value = item['value']

            if label in result:
                result[label] += value
            else:
                result[label] = value

    # Calculate total sum
    total_sum = sum(result.values())

    # Calculate percentage and construct final array
    for country, value in result.items():
        percentage = value / total_sum
        summed_amounts.append(
            {'label': country, 'value': value, 'percentage': percentage})

    return summed_amounts


def sum_countries_count(response_array):
    result = {}
    summed_counts = []

    # Calculate summed counts for each country
    for response in response_array:
        for item in response['countries_count_list']:
            label = item['label']
            value = item['value']

            if label in result:
                result[label] += value
            else:
                result[label] = value

    # Calculate total sum
    total_sum = sum(result.values())

    # Calculate percentage and construct final array
    for country, value in result.items():
        percentage = value / total_sum
        summed_counts.append(
            {'label': country, 'value': value, 'percentage': percentage})

    return summed_counts


def sum_type_list(response_array, type_key):
    result = {}

    for response in response_array:
        for item in response[type_key]:
            label = item['label']
            value = item.get('value', 0)
            count = item.get('count', 0)

            if label in result:
                result[label]['value'] += value
                result[label]['count'] += count
            else:
                result[label] = {'label': label,
                                 'value': value, 'count': count}

    summed_assets = list(result.values())

    return summed_assets
