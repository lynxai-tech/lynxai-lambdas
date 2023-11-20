def delete_date_list(event):

    event_body = event.body()
    fund_id = event_body['fundId']

    old_fund_date_list = event.select(
        "SELECT date FROM `schema`.investment_fund WHERE main_fund_id = (:fund_id)", {'fund_id': fund_id}).list()
    new_fund_date_list = event_body['fundHistory'].keys()
    deleted_date_list = [
        date for date in old_fund_date_list if date not in new_fund_date_list]

    date_main_fund_id_list = [
        {'main_fund_id': fund_id, 'date': date} for date in deleted_date_list]

    delete_id_list = event.select(
        "SELECT id FROM `schema`.investment_fund WHERE main_fund_id = (:main_fund_id) AND date = (:date)", date_main_fund_id_list).list()

    event.change(
        "DELETE FROM `schema`.investment_detail WHERE fund_id = (:id)", delete_id_list)

    event.change(
        "DELETE FROM `schema`.investment_fund main_fund_id = (:main_fund_id) AND date = (:date)", date_main_fund_id_list)


def edit_fund(event):

    event_body = event.body()
    fund_id = event_body['fundId']

    existing_dates_list = event.select(
        "SELECT date FROM `schema`.investment_fund WHERE main_fund_id = (:fund_id)", {'fund_id': fund_id}).list()

    fund_history = event_body['fundHistory']

    dates_list = fund_history.keys()

    for date in dates_list:
        if date in existing_dates_list:
            id = event.select("SELECT id FROM `schema`.investment_fund WHERE date = (:date) AND main_fund_id = (:fund_id)", {
                              'date': date, 'fund_id': fund_id}).list()['id']

            existing_asset_list = event.select("""SELECT a.id, a.name, a.ticker, a.public_asset, a.market_cap, a.type_of_issuer, a.financial_industry, a.country, a.pri_signatory, a.investment_type, id.nominal_amount, id.portfolio_asset_weight_non_financial, id.currency, id.asset_type, id.share_amount
                                        FROM asset a
                                        JOIN investment_detail id ON a.id = id.asset_id
                                        JOIN investment_fund f ON f.id = id.fund_id
                                        WHERE f.id = fund_id;""", {'fund_id': id})

            insert_or_update_assets_in_fund(
                event, id, existing_asset_list, fund_history[date])

            delete_assets_in_fund(
                event, id, existing_asset_list, fund_history[date])
        else:
            event.change("INSERT INTO `schema`.investment_fund(fund_name, main_fund_id, date) VALUES ((:fund_name), (:main_fund_id), (:date))", {
                'fund_name': event_body['fundName'],
                'main_fund_id': fund_id,
                'date': date
            })
            insert_assets_in_fund(event, id, fund_history[date])


def insert_or_update_assets_in_fund(event, fund_id, existing_assets, new_assets):
    for asset in new_assets:
        asset_id = event.select("SELECT id FROM `schema`.asset WHERE name = (:asset_name)", {
                                'asset_name': asset['name']}).val()
        if asset_id is None:
            event.change("""INSERT INTO `schema`.asset (name, ticker, public_asset, market_cap, type_of_issuer, financial_industry, country, pri_signatory, asset_type, isin, isSynced)
                         VALUES ((:name), (:ticker), (:public_asset), (:market_cap), (:type_of_issuer), (:financial_industry), (:country), (:pri_signatory), (:asset_type), (:isin), (:isSynced))""",
                         {'name': asset.get('name', ''),
                          'ticker': asset.get('ticker', ''),
                          'public_asset': asset.get('public_asset', ''),
                          'market_cap': asset.get('market_cap', ''),
                          'type_of_issuer': asset.get('type_of_issuer', ''),
                          'financial_industry': asset.get('financial_industry', ''),
                          'country': asset.get('country', ''),
                          'pri_signatory': asset.get('pri_signatory', ''),
                          'asset_type': asset.get('asset_type', ''),
                          'isin': asset.get('isin', ''),
                          'isSynced': asset.get('isSynced', '')})
        if asset in existing_assets:
            event.change("""INSERT INTO `schema`.investment_detail (fund_id, asset_id, investment_type, currency, nominal_amount, portfolio_asset_weight_non_financial, share_amount) 
                     VALUES ((:fund_id), (:asset_id), (:investment_type), (:currency), (:nominal_amount), (:portfolio_asset_weight_non_financial), (:share_amount))""",
                         {'fund_id': fund_id,
                          'asset_id': asset.get('id', ''),
                          'investment_type': asset.get('investment_type', ''),
                          'currency': asset.get('currency', ''),
                          'nominal_amount': asset.get('nominal_amount', ''),
                          'portfolio_asset_weight_non_financial': asset.get('portfolio_asset_weight_non_financial', ''),
                          'share_amount': asset.get('share_amount', '')})
        else:
            event.change("""UPDATE `schema`.investment_detail SET fund_id = (:fund_id), asset_id = (:asset_id), investment_type = (:investment_type), currency = (:currency), 
                         nominal_amount = (:nominal_amount), portfolio_asset_weight_non_financial = (:portfolio_asset_weight_non_financial), share_amount = (:share_amount)""",
                         {'fund_id': fund_id,
                          'asset_id': asset.get('id', ''),
                          'investment_type': asset.get('investment_type', ''),
                          'currency': asset.get('currency', ''),
                          'nominal_amount': asset.get('nominal_amount', ''),
                          'portfolio_asset_weight_non_financial': asset.get('portfolio_asset_weight_non_financial', ''),
                          'share_amount': asset.get('share_amount', '')})


def delete_assets_in_fund(event, fund_id, existing_asset_list, new_asset_list):
    for asset in new_asset_list not in existing_asset_list:
        id = event.select("SELECT id FROM `schema`.asset WHERE name = (:name)", {
                          'name': asset.get('name', '')}).val()

        event.change("DELETE FROM `schema`.investment_detail WHERE fund_id = (:fund_id) AND asset_id = (:asset_id)", {
                     'fund_id': fund_id})


def insert_assets_in_fund(event, id, asset_list):
    for asset in asset_list:
        asset_id = event.select("SELECT id FROM `schema`.asset WHERE name = (:asset_name)", {
                                'asset_name': asset['name']}).val()
        if asset_id is None:
            event.change("""INSERT INTO `schema`.asset (name, ticker, public_asset, market_cap, type_of_issuer, financial_industry, country, pri_signatory, asset_type, isin, isSynced)
                         VALUES ((:name), (:ticker), (:public_asset), (:market_cap), (:type_of_issuer), (:financial_industry), (:country), (:pri_signatory), (:asset_type), (:isin), (:isSynced))""",
                         {'name': asset.get('name', ''),
                          'ticker': asset.get('ticker', ''),
                          'public_asset': asset.get('public_asset', ''),
                          'market_cap': asset.get('market_cap', ''),
                          'type_of_issuer': asset.get('type_of_issuer', ''),
                          'financial_industry': asset.get('financial_industry', ''),
                          'country': asset.get('country', ''),
                          'pri_signatory': asset.get('pri_signatory', ''),
                          'asset_type': asset.get('asset_type', ''),
                          'isin': asset.get('isin', ''),
                          'isSynced': asset.get('isSynced', '')})

        event.change("""UPDATE `schema`.investment_detail SET fund_id = (:fund_id), asset_id = (:asset_id), investment_type = (:investment_type), currency = (:currency), 
                         nominal_amount = (:nominal_amount), portfolio_asset_weight_non_financial = (:portfolio_asset_weight_non_financial), share_amount = (:share_amount)""",
                     {'fund_id': id,
                      'asset_id': asset.get('id', ''),
                      'investment_type': asset.get('investment_type', ''),
                      'currency': asset.get('currency', ''),
                      'nominal_amount': asset.get('nominal_amount', ''),
                      'portfolio_asset_weight_non_financial': asset.get('portfolio_asset_weight_non_financial', ''),
                      'share_amount': asset.get('share_amount', '')})
