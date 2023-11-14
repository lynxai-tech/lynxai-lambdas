def print_title(title):
    print('title', title)
    print('title', title)
    print('title', title)


def get_latest_investment_fund(data):
    latest_date = max(data["fundHistory"].keys())

    latest_data = data["fundHistory"][latest_date]

    print("Latest Date:", latest_date)
    print("Latest Data:", latest_data)

    return latest_data, latest_date


def get_investment_fund_list(data, main_fund_id, fund_name):
    return [
        {"fund_name": fund_name, "main_fund_id": main_fund_id, "date": date}
        for date in data["fundHistory"]
    ]


def insert_new_assets(data, event):
    asset_list = []

    for date, entries in data["fundHistory"].items():
        for entry in entries:
            asset_list.append(entry)

    print({'asset_list': asset_list})

    asset_name_list = [{'asset_name': item['name']} for item in asset_list]

    print({'asset_name_list': asset_name_list})

    existing_assets = event.select(
        "SELECT * FROM `schema`.asset WHERE name IN (:asset_name)", asset_name_list).list()

    print({'existing_assets': existing_assets})

    new_assets = [item for item in asset_list if item["name"]
                  not in existing_assets]

    print({'new_assets': new_assets})

    parsed_assets = [{
        'name': item.get('name', ''),
        'ticker': item.get('ticker', ''),
        'public_asset': item.get('public_asset', ''),
        'market_cap': item.get('market_cap', ''),
        'type_of_issuer': item.get('type_of_issuer', ''),
        'financial_industry': item.get('financial_industry', ''),
        'pri_signatory': item.get('pri_signatory', ''),
        'asset_type': item.get('asset_type', ''),
        'isin': item.get('isin', ''),
        'isSynced': 0
    } for item in new_assets]

    print({'parsed_assets': parsed_assets})

    event.change("""INSERT INTO asset (name, ticker, public_asset, market_cap, type_of_issuer, financial_industry, country, pri_signatory, asset_type, isin, isSynced) 
                 VALUES ((:name),(:ticker),(:pulic_asset),(:market_cap),(:type_of:issuer),(:financial_industry),(:country),(:pri_signature),(:asset_type),(:isin),(:isSynced))""",
                 parsed_assets)
    
    
