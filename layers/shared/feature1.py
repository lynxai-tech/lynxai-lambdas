def get_latest_investment_fund_by_post_body(data):
    latest_date = max(data["fundHistory"].keys())

    latest_data = data["fundHistory"][latest_date]

    print("Latest Date:", latest_date)
    print("Latest Data:", latest_data)

    return latest_data, latest_date

def get_latest_investment_fund_by_get_fund(data):
    return max(data['fund_history'], key=lambda x: x['date'])


def get_investment_fund_list(data, main_fund_id, fund_name):
    return [
        {"fund_name": fund_name, "main_fund_id": main_fund_id, "date": date}
        for date in data["fundHistory"]
    ]


def get_asset_list_from_fund_history(data):
    asset_list = []

    for date, entries in data["fundHistory"].items():
        for entry in entries:
            asset_list.append(entry)
    return asset_list


def get_new_assets(data, event):
    asset_list = get_asset_list_from_fund_history(data)

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

    return parsed_assets


def get_investment_details(event_body, main_fund_id, event):
    investment_fund_list = event.select(
        "SELECT id, date FROM `schema`.investment_fund WHERE main_fund_id = (:main_fund_id)", {'main_fund_id': main_fund_id})
    print(investment_fund_list)

    asset_list = []

    for entries in event_body["fundHistory"].items():
        for entry in entries:
            asset_list.append(entry)

    date_to_id_mapping = {fund['date']: fund['id']
                          for fund in investment_fund_list}

    parsed_investment_details = [{
        'fund_id': date_to_id_mapping[item['date']],
        'asset_id': item.get('id', ''),
        'investment_type': item.get('investment_type', ''),
        'currency': item.get('currency', ''),
        'nominal_amount': item.get('nominal_amount', ''),
        'portfolio_asset_weight_non_financial': item.get('portfolio_asset_weight_non_financial', ''),
        'share_amount': item.get('share_amount', '')
    } for item in asset_list]

    return parsed_investment_details


def get_fund_list(event, client_name):
    return event.select("""
    SELECT i.id,
           i.date,
           if2.id if_id,
           i.fund_name,
           i.fund_type,
           i.eu_sfdr_article,
           i.country,
           i.status,
           i.amount,
           i.portfolio_weight_percentage,
           i.client_id,
           COUNT(id.asset_id) nr_assets
      FROM `schema`.main_fund i
      JOIN `schema`.client u
        ON u.id = i.client_id
      JOIN `schema`.investment_fund if2
        ON if2.main_fund_id = i.id
      LEFT JOIN `schema`.investment_detail id
        ON id.fund_id = if2.id
     WHERE u.name = (:client_name)
       AND i.date = if2.date
     GROUP BY if2.id;
    """, {'client_name': client_name}).list()
