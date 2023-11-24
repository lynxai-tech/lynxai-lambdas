from main import *

# industry,
# assetType,
# country,
# isPrivate, (public_asset IN Public, Private)
# name,
# ticker,
# isin,
# amount,
# emissionsPerYearTCO2


@lynx()
def lambda_handler(event, context):
    d = event.body()

    res = event.change("""
    INSERT INTO `schema`.asset (name, ticker, public_asset, financial_industry, country, asset_type, isin, emissionsPerYearTCO2)
    VALUES (:name, :ticker, :public_asset, :financial_industry, :country, :asset_type, :isin, :emissionsPerYearTCO2)
    """, {
        'name': d.get('name'),
        'ticker': d.get('ticker'),
        'public_asset': 'Private' if d.get('isPrivate') == 'true' else 'Public' if d.get('isPrivate') == 'true' else None,
        'financial_industry': d.get('industry'),
        'country': d.get('country'),
        'asset_type': d.get('assetType'),
        'isin': d.get('isin'),
        'emissionsPerYearTCO2': d.get('emissionsPerYearTCO2')
    })

    return {
        'id': res['generatedFields'][0]['longValue']
    }
