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

    asset_id = d.get('id')
    print('asset_id', asset_id)

    res = event.change("""
    INSERT INTO `schema`.asset (name, ticker, public_asset, financial_industry, country, asset_type, isin, emissionsPerYearPerEuroTCO2, esgRiskScore, epcScore)
    VALUES (:name, :ticker, :public_asset, :financial_industry, :country, :asset_type, :isin, :emissionsPerYearPerEuroTCO2, :esgRiskScore, :epcScore)
    """, {
        'name': d.get('name'),
        'ticker': d.get('ticker'),
        'public_asset': 'Private' if d.get('isPrivate') == 'true' else 'Public' if d.get('isPrivate') == 'true' else None,
        'financial_industry': d.get('industry'),
        'country': d.get('country'),
        'asset_type': d.get('assetType'),
        'isin': d.get('isin'),
        'emissionsPerYearPerEuroTCO2': d.get('emissionsPerYearPerEuroTCO2'),
        'esgRiskScore': d.get('esgRiskScore'),
        'epcScore': d.get('epcScore'),
    })

    return {
        'id': res['generatedFields'][0]['longValue']
    }
