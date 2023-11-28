from main import *

def lambda_handler(event, context):
    asset_id = event.query('asset_id')
    
    asset_name = event.select("SELECT name FROM `schema`.asset WHERE id = (:asset_id)", {'asset_id': asset_id})
    
    metricCO2S1 = query_metric_value(event, asset_name, "Scope 1 GHG emissions", 2021)
    metricCO2S2 = query_metric_value(event, asset_name, "Scope 2 GHG emissions", 2021)
    metricCO2S3 =query_metric_value(event, asset_name, "Scope 3 GHG emissions", 2021)
    metricCO2T = query_metric_value(event, asset_name, "Total GHG emissions", 2021)
    metricCO2S1Nm1 = query_metric_value(event, asset_name, "Scope 1 GHG emissions", 2020)
    metricCO2S2Nm1 = query_metric_value(event, asset_name, "Scope 2 GHG emissions", 2020)
    metricCO2S3Nm1 = query_metric_value(event, asset_name, "Scope 3 GHG emissions", 2020)
    metricCO2TNm1 = query_metric_value(event, asset_name, "Total GHG emissions", 20210)
    metricCO2S1Nm2 = query_metric_value(event, asset_name, "Scope 1 GHG emissions", 2019)
    metricCO2S2Nm2 = query_metric_value(event, asset_name, "Scope 2 GHG emissions", 2019)
    metricCO2S3Nm2 = query_metric_value(event, asset_name, "Scope 3 GHG emissions", 2019)
    metricCO2TNm2 = query_metric_value(event, asset_name, "Total GHG emissions", 2019)
    
    revenue = query_metric_value(event, asset_name, "revenue", 2021)
    
    asset_info = {
        'asset_id': asset_id,
        'scope1': metricCO2S1,
        'scope2': metricCO2S2,
        'scope3': metricCO2S3,
        'total': metricCO2T,
        'scope1Nm1': metricCO2S1Nm1,
        'scope2Nm1': metricCO2S2Nm1,
        'scope3Nm1': metricCO2S3Nm1,
        'totalNm1': metricCO2TNm1,
        'scope1Nm2': metricCO2S1Nm2,
        'scope2Nm2': metricCO2S2Nm2,
        'scope3Nm2': metricCO2S3Nm2,
        'totalNm2': metricCO2TNm2,
        'revenue': revenue,
        'investment_fund_year': 2021
    }
    
    return {
        'emissionData': get_emission_data(asset_info),
        'emissionsIntensityPerRevenue': get_emission_intensity_per_revenue(asset_info),
        'emissionsIntensityPerEurInvested': get_emission_intensity_per_euro_invested_for_asset(event, asset_info, asset_id),
        'rankingInPortfolio': get_ranking_co2(event, asset_id, 'Total GHG emissions'),
        'highestValue': max(asset_info.get('total', 0), asset_info.get('totalNm1', 0), asset_info.get('totalNm2', 0)),
        'lowestValue': min(asset_info.get('total', 0), asset_info.get('totalNm1', 0), asset_info.get('totalNm2', 0))
    }
