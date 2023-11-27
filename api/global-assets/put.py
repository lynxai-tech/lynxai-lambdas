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

    asset_id = event.param('id')
    print('asset_id', asset_id)

    res = event.change("""
    UPDATE `schema`.asset
       SET name = (:name),
           ticker = (:ticker),
           public_asset = (:public_asset),
           financial_industry = (:financial_industry),
           country = (:country),
           asset_type = (:asset_type),
           isin = (:isin),
           emissionsPerYearPerEuroTCO2 = (:emissionsPerYearPerEuroTCO2),
           esgRiskScore = (:esgRiskScore),
           epcScore = (:epcScore)
     WHERE id = (:id)
    """, {
        'id': asset_id,
        'name': d.get('name'),
        'ticker': d.get('ticker'),
        'public_asset': 'Private' if d.get('isPrivate') == 'true' else 'Public' if d.get('isPrivate') == 'true' else None,
        'financial_industry': d.get('industry'),
        'country': d.get('country'),
        'asset_type': d.get('assetType'),
        'isin': d.get('isin'),
        'emissionsPerYearPerEuroTCO2': d.get('emissionsPerYearPerEuroTCO2'),
        'esgRiskScore': d.get('esgRiskScore'),
        'epcScore': d.get('epcScore')
    })

    return True
