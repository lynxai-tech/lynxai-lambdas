from fund_dictionaries import dictionary_countries, dictionary_asset_types, dictionary_investment_types

def get_fund_info(event, client_name, fund_id):
    return event.select("""
    SELECT
        i.id,
        i.date,
        if2.id AS if_id,
        i.fund_name,
        i.fund_type,
        i.eu_sfdr_article,
        i.country,
        i.status,
        i.amount,
        i.portfolio_weight_percentage,
        i.client_id
    FROM `schema`.main_fund i
    JOIN `schema`.client u 
    ON u.id = i.client_id
    JOIN `schema`.investment_fund if2 
    ON if2.main_fund_id = i.id
    LEFT JOIN `schema`.investment_detail id 
    ON id.fund_id = if2.id
    WHERE
        u.name = (:client_name)
        AND i.id = (:fund_id)
        AND i.date = if2.date
    GROUP BY
        if2.id;
    """, {'client_name': client_name, 'fund_id': fund_id}).list()


def get_fund(event, fund_name, client_name):
    fund = get_fund_info(event, client_name, fund_name)[0]

    print(fund)

    fund_history = get_fund_history(event, fund.get('id'))

    print(4)

    fund['fund_history'] = fund_history

    print(5)

    return fund


def get_fund_history(event, main_fund_id):
    investment_fund_list = event.select("SELECT id, date, amount FROM `schema`.investment_fund WHERE main_fund_id = (:main_fund_id)", {
                                        'main_fund_id': main_fund_id}).list()

    print(investment_fund_list)

    print(1)

    fund_history = [{'date': item.get('date'),
                     'amount': item.get('amount'),
                     'assets': get_assets_for_investment_fund(event, item.get('id'))
                     } for item in investment_fund_list]
    print(2)

    # print({'fund_history':fund_history})

    return fund_history


def get_assets_for_investment_fund(event, fund_id):
    assets = event.select("""SELECT a.id, a.name, a.ticker, a.public_asset, a.market_cap, a.type_of_issuer, a.financial_industry, a.country, a.pri_signatory, a.asset_type, id.nominal_amount, id.portfolio_asset_weight_non_financial, id.currency, id.investment_type, id.share_amount
    FROM `schema`.asset a
    JOIN `schema`.investment_detail id ON a.id = id.asset_id
    JOIN `schema`.investment_fund f ON f.id = id.fund_id
    WHERE f.id = (:fund_id)""", {'fund_id': fund_id}).list()

    print(3)
    # print(assets)

    return assets


def parse_types_list(types_list, property_key):
    count_map = {}
    for curr in types_list:
        value = getattr(curr, property_key, None)
        if value:
            count_map[value] = count_map.get(value, 0) + 1

    result = [
        {
            'label': name,
            'count': count,
            'value': sum(float(asset.nominal_amount) for asset in types_list if getattr(asset, property_key) == name),
        }
        for name, count in count_map.items()
    ]

    return result


def filter_countries(countries_list):
    new_list = []

    for item in countries_list:
        print(item)
        if item['country'] is None:
            pass
        elif item['country'] and "," in item['country']:
            split_items = item['country'].split(",")
            for split_item in split_items:
                new_list.append({**item, 'country': split_item.strip()})
        else:
            new_list.append(item)

    return new_list


def map_countries_in_fund_asset(countries_list):
    return [
        {**asset, 'country': dictionary_countries.get(
            asset.get('country'), asset.get('country'))}
        for asset in countries_list
    ]

def map_asset_types_in_fund_asset(asset_list):
    mapped_asset_list = [
        {**asset, 'asset_type': dictionary_asset_types.get(asset['asset_type'], asset['asset_type'])}
        for asset in asset_list
    ]

    return mapped_asset_list

def parse_countries_count_list(countries_list):
    total_count = len(countries_list)

    count_map = {}
    for curr in countries_list:
        country = curr.get('country')
        if country:
            count_map[country] = count_map.get(country, 0) + 1

    result = [
        {'label': country, 'value': count, 'percentage': count / total_count}
        for country, count in count_map.items()
    ]

    return result

def parse_countries_amount_list(countries_list, total_amount):
    count_map = {}
    for curr in countries_list:
        country = curr.get('country')
        if country:
            count_map[country] = count_map.get(country, 0) + 1

    result = [
        {
            'label': country,
            'value': sum(float(asset['nominal_amount']) for asset in countries_list if asset['country'] == country),
            'percentage': (
                sum(float(asset['nominal_amount']) for asset in countries_list if asset['country'] == country)
                / total_amount
            ),
        }
        for country, count in count_map.items()
    ]

    return result
