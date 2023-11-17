from main import *


@lynx()
def lambda_handler(event, context):
    simulation_id = event.param('id')

    # get the simulation assets
    res = event.select("""
    SELECT F.isDraft, F.fund_name AS name, F.simulatedMainFundId, ID.*
      FROM `schema`.main_fund F
      LEFT JOIN `schema`.investment_fund IVF
        ON IVF.main_fund_id = F.id
      LEFT JOIN `schema`.investment_detail ID
        ON ID.fund_id = IVF.id
     WHERE F.id = (:mainFundId)
    """, {
        'mainFundId': simulation_id
    }).list()

    simulated_fund_id = res[0].get('simulatedMainFundId')

    snapshot_id = event.select("""
    SELECT MAX(IVF.id)
      FROM `schema`.main_fund F
      LEFT JOIN `schema`.investment_fund IVF
        ON IVF.main_fund_id = F.id
     WHERE F.id = (:mainFundId)
    """, {
        'mainFundId': simulated_fund_id
    }).val()

    # get the actual assets
    actuals = event.select("""
    SELECT ID.*
      FROM `schema`.investment_fund IVF
      LEFT JOIN `schema`.investment_detail ID
        ON ID.fund_id = IVF.id
     WHERE IVF.id = (:simulatedFundId)
    """, {
        'simulatedFundId': snapshot_id
    }).list()

    def cleanup(x):
        del x['simulatedMainFundId']
        del x['isDraft']
        return x

    return {
        'name': res[0].get('name'),
        'fundId': simulated_fund_id,
        'isDraft': res[0].get('isDraft') == 1,
        'simulationAssets': [cleanup(x) for x in res],
        'actualAssets': actuals,
    }
