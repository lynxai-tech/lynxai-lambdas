from main import *
from pprint import pprint
from datetime import datetime, timedelta
import csv

THRESHOLDS = {
    'Produção de Eletricidade': 200000,
    'Fabricação de Cimento': 200000,
    'Commercial Real Estate - Residencial': 200000,
    'Commercial Real Estate - Serviços': 200000
}


def lookup_metrics(metrics_values, asset_name, ):
    emissions = {
        x.get('year'): x.get('value') for x in metrics_values
        if x.get('assetName') == asset_name if x.get('year')}

    return emissions


def compute_emissions(year, res, key='simulation'):
    """
    This outputs the total emissions from the simulation (res) for year (year)
    """

    assets = 0
    for asset in res:
        if asset.get('startDate').year < year:
            if asset.get('endDate').year < year:
                assets += 0
            elif asset.get('endDate').year > year:
                assets += asset.get('emissionsPerYearTCO2')
            else:
                # if the start date is before year and the end date is year
                # compute day in year of endDate, divide by 365, multiply by emissionsPerYearTCO2
                fraction = (asset.get('endDate') -
                            datetime(year, 1, 1)).days / 365.25
                value = asset.get('emissionsPerYearTCO2') * fraction
                # if value < 0:
                #  print('BEFORE START, YEAR OF END', fraction, value, asset)
                assets += value
        elif asset.get('startDate').year == year:
            # if the start date is THIS year
            if asset.get('endDate').year == year:
                n_days = (asset.get('endDate') - datetime(year, 1, 1)).days - \
                    (asset.get('startDate') - datetime(year, 1, 1)).days
                fraction = n_days / 365.25
                value = asset.get('emissionsPerYearTCO2') * fraction
                # if value < 0:
                #  print('START YEAR, END YEAR', fraction, value, asset)
                assets += value
            else:
                # end date is later than this year
                n_days = (datetime(year + 1, 1, 1) -
                          asset.get('startDate')).days
                fraction = n_days / 365.25
                value = asset.get('emissionsPerYearTCO2') * fraction
                # if value < 0:
                #  print('START YEAR, FUTURE END', fraction, value, asset)
                assets += value

    return {
        'year': year,
        key: assets
    }


# def update_assets(db):
#     fund_id = {
#         'Commercial Real Estate - Residencial': 448,
#         'Commercial Real Estate - Serviços': 447,
#         'Produção de Eletricidade': 446,
#         'Fabricação de Cimento': 445
#     }

#     db.change("""
#     DELETE FROM `schema`.investment_detail
#      WHERE fund_id IN (448, 447, 446, 445)
#     """)

#     with open('/Users/lewis/klug/lynxai/api/api/simulations/cgd.csv', mode='r', encoding='utf-8') as file:
#       reader = csv.reader(file)
#       header = next(reader)
#       for i, row in enumerate(reader):
#           row = dict(zip(header, row))
#           #res = db.change("""
#           #UPDATE `schema`.asset SET emissionsPerYearPerEuroTCO2 = (:emissionsPerYearPerEuroTCO2)
#           # WHERE name = (:name)
#           #""", {
#           #    'name': f"""{row.get('name')} {row.get('suffix')}""",
#           #    'emissionsPerYearPerEuroTCO2': row.get('emissionsPerYearPerEuroTCO2') if row.get('emissionsPerYearPerEuroTCO2') else 0,
#           #})


#           print(i+1, 67)
#           asset_id = db.select("""
#           SELECT id
#             FROM `schema`.asset
#            WHERE name = (:name)
#           """, {
#               'name': f"""{row.get('name')} {row.get('suffix')}""",
#           }).val()

#           res = db.change("""
#           INSERT INTO `schema`.investment_detail (asset_id, fund_id, nominal_amount, startDate, endDate)
#           VALUES (:asset_id, :fund_id, :nominal_amount, :startDate, :endDate)
#           """, {
#               'asset_id': asset_id,
#               'fund_id': fund_id.get(row.get('financial_industry')),
#               'nominal_amount': row.get('exposure'),
#               'startDate': row.get('startDate'),
#               'endDate': row.get('endDate')
#           })

@lynx()
def lambda_handler(event, context):
    simulation_id = event.param('id')

    # don't run this again
    # update_assets(event.rds)

    # get the simulation assets
    res = event.select("""
    SELECT F.isDraft, F.fund_name AS name, F.simulatedMainFundId, ID.*, A.name AS assetName, A.emissionsPerYearPerEuroTCO2, financial_industry, isClone
      FROM `schema`.main_fund F
      LEFT JOIN `schema`.investment_fund IVF
        ON IVF.main_fund_id = F.id
      LEFT JOIN `schema`.investment_detail ID
        ON ID.fund_id = IVF.id
      LEFT JOIN `schema`.asset A
        ON A.id = ID.asset_id
     WHERE F.id = (:mainFundId)
    """, {
        'mainFundId': simulation_id
    }).list()

    industry = list(set([x.get('financial_industry') for x in res]))
    print(industry)

    if len(industry) == 1:
        threshold = THRESHOLDS[industry[0]]
    else:
        threshold = 0

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
    SELECT ID.*, A.name AS assetName,
           A.emissionsPerYearPerEuroTCO2
      FROM `schema`.investment_fund IVF
      LEFT JOIN `schema`.investment_detail ID
        ON ID.fund_id = IVF.id
      LEFT JOIN `schema`.asset A
        ON A.id = ID.asset_id
     WHERE IVF.id = (:simulatedFundId)
    """, {
        'simulatedFundId': snapshot_id
    }).list()

    def cleanup(x):
        del x['simulatedMainFundId']
        del x['isDraft']
        return x

    simulation = [
        {
            'assetName': x.get('assetName'),
            'startDate': datetime.fromisoformat(x.get('startDate')) if x.get('startDate') else datetime(2022, 1, 1),
            'endDate': datetime.fromisoformat(x.get('endDate')) if x.get('endDate') else datetime(2100, 12, 31),
            'emissionsPerYearPerEuroTCO2': float(x.get('emissionsPerYearPerEuroTCO2')) if x.get('emissionsPerYearPerEuroTCO2') else 0,
            'emissionsPerYearTCO2': (float(x.get('emissionsPerYearPerEuroTCO2')) if x.get('emissionsPerYearPerEuroTCO2') else 0) * float(x.get('nominal_amount'))
        } for x in res
    ]

    fund = [
        {
            'assetName': x.get('assetName'),
            'startDate': datetime.fromisoformat(x.get('startDate')) if x.get('startDate') else datetime(2022, 1, 1),
            'endDate': datetime.fromisoformat(x.get('endDate')) if x.get('endDate') else datetime(2100, 12, 31),
            'emissionsPerYearPerEuroTCO2': float(x.get('emissionsPerYearPerEuroTCO2')) if x.get('emissionsPerYearPerEuroTCO2') else 0,
            'emissionsPerYearTCO2': (float(x.get('emissionsPerYearPerEuroTCO2')) if x.get('emissionsPerYearPerEuroTCO2') else 0) * float(x.get('nominal_amount'))
        } for x in actuals
    ]

    chart_data_emissions_simulations = [
        compute_emissions(year, simulation) for year in range(2022, 2051)
    ]

    chart_data_emissions_fund = [
        compute_emissions(year, fund, key='fund') for year in range(2022, 2051)
    ]

    chart_data_emissions = [
        x | y for x, y in zip(chart_data_emissions_simulations, chart_data_emissions_fund)
    ]

    simulation_emissions_original_simulation = chart_data_emissions[0].get(
        'simulation')
    simulation_emissions_original_fund = chart_data_emissions[0].get(
        'fund')

    chart_data_percentage = [
        {**x,
         'simulation': 100 - 100 * x.get('simulation') / simulation_emissions_original_simulation,
         'fund': 100 - 100 * x.get('fund') / simulation_emissions_original_fund,
         } for x in chart_data_emissions
    ]

    chart_data_emissions = [
        {**x, 'threshold': threshold if x.get('year') < 2050 else 0} if x.get('year') >= 2030 and x.get('year') <= 2050 else x for x in chart_data_emissions
    ]

    return {
        'name': res[0].get('name'),
        'fundId': simulated_fund_id,
        'isDraft': res[0].get('isDraft') == 1,
        'simulationAssets': [cleanup(x) for x in res],
        'actualAssets': actuals,
        'chartData': {
            'percentage': chart_data_percentage,
            'emissions': chart_data_emissions
        },
        'achievement': {
            '2030': {
                'simulationValue': [x for x in chart_data_emissions if x.get('year') == 2030][0].get('simulation'),
                'fundValue': [x for x in chart_data_emissions if x.get('year') == 2030][0].get('fund'),
                'simulationPercentage': [x for x in chart_data_percentage if x.get('year') == 2030][0].get('simulation'),
                'fundValuePercentage': [x for x in chart_data_percentage if x.get('year') == 2030][0].get('fund'),
                'threshold': threshold
            },
            '2050': {
                'simulationValue': [x for x in chart_data_emissions if x.get('year') == 2050][0].get('simulation'),
                'fundValue': [x for x in chart_data_emissions if x.get('year') == 2050][0].get('fund'),
                'simulationPercentage': [x for x in chart_data_percentage if x.get('year') == 2050][0].get('simulation'),
                'fundValuePercentage': [x for x in chart_data_percentage if x.get('year') == 2050][0].get('fund'),
                'threshold': 0
            }
        }
    }
