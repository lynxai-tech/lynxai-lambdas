from main import *
from datetime import datetime


@lynx()
def lambda_handler(event, context):
    simulation_id = event.param('id')
    asset_id = event.param('assetId')
    amount = event.param('amount')
    start_date = event.param('startDate')
    end_date = event.param('endDate')

    try:
        simulation_asset_id = event.param("simulationAssetId")
    except ParameterError:
        simulation_asset_id = None

    if not simulation_asset_id:
        res = event.select("""
        SELECT id
        FROM `schema`.investment_fund F
        WHERE F.main_fund_id = (:simulationId)
        """, {
            'simulationId': simulation_id,
        }).val()

        event.change("""
        INSERT INTO `schema`.investment_detail (asset_id, fund_id, nominal_amount, startDate, endDate)
        VALUES ((:assetId), (:simulationId), (:amount), (:startDate), (:endDate))
        """, {
            'assetId': asset_id,
            'simulationId': res,
            'amount': amount,
            'startDate': start_date,
            'endDate': end_date,
        })
    else:
        event.change("""
        UPDATE `schema`.investment_detail
           SET nominal_amount = (:amount),
               startDate = (:startDate),
               endDate = (:endDate)
         WHERE id = (:simulationAssetId)
        """, {
            'simulationAssetId': simulation_asset_id,
            'amount': amount,
            'startDate': start_date,
            'endDate': end_date,
        })

    return True